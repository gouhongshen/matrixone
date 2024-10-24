// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var (
	varcharType = types.T_varchar.ToType()
)

func (builder *QueryBuilder) applyIndicesForFiltersUsingMasterIndex(nodeID int32, scanNode *plan.Node,
	cnt map[[2]int32]int, colMap map[[2]int32]*plan.Expr, indexDef *plan.IndexDef) int32 {

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ

	var prevIndexPkCol *Expr
	var prevLastNodeId int32
	var lastNodeId int32
	for i, filterExp := range scanNode.FilterList {
		idxObjRef, idxTableDef := builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, indexDef.IndexTableName)

		// 1. SELECT pk from idx WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value"))
		currIdxProjTag, currScanId := makeIndexTblScan(builder, builder.ctxByNode[nodeID], filterExp, idxTableDef, idxObjRef, scanNode)

		// 2. (SELECT pk from idx1 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value1")) )
		//    	INNER JOIN
		//    (SELECT pk from idx2 WHERE prefix_eq(`__mo_index_idx_col` =  serial_full("b","value2")) )
		//    	ON idx1.pk = idx2.pk
		//    ...
		lastNodeId = currScanId
		currIndexPkCol := &Expr{
			Typ: *DeepCopyType(pkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: currIdxProjTag,
					ColPos: 0, // __mo_index_pk_col
				},
			},
		}
		if i != 0 {
			wherePrevPkEqCurrPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
				currIndexPkCol,
				prevIndexPkCol,
			})
			lastNodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_INNER,
				Children: []int32{currScanId, prevLastNodeId},
				OnList:   []*Expr{wherePrevPkEqCurrPk},
			}, builder.ctxByNode[nodeID])
		}

		prevIndexPkCol = DeepCopyExpr(currIndexPkCol)
		prevLastNodeId = lastNodeId
	}
	scanNode.Limit, scanNode.Offset = nil, nil

	// 3. SELECT * from tbl INNER JOIN (
	//    	(SELECT pk from idx1 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value1")) )
	//    		INNER JOIN
	//    	(SELECT pk from idx2 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("b","value2")) )
	//    		ON idx1.pk = idx2.pk
	//    ) ON tbl.pk = idx1.pk
	wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: *DeepCopyType(pkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: pkPos, // tbl.pk
				},
			},
		},
		{
			Typ: *DeepCopyType(pkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: prevIndexPkCol.GetCol().RelPos, // last idxTbl (may be join) relPos
					ColPos: 0,                              // idxTbl.pk
				},
			},
		},
	})
	lastNodeId = builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INDEX,
		Children: []int32{scanNode.NodeId, lastNodeId},
		OnList:   []*Expr{wherePkEqPk},
	}, builder.ctxByNode[nodeID])

	return lastNodeId
}

func makeIndexTblScan(builder *QueryBuilder, bindCtx *BindContext, filterExp *plan.Expr,
	idxTableDef *TableDef, idxObjRef *ObjectRef, scanNode *plan.Node) (int32, int32) {

	// a. Scan * WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value"))
	idxScanTag := builder.genNewTag()
	args := filterExp.GetF().Args

	var filterList *plan.Expr
	indexKeyCol := &plan.Expr{
		Typ: *makePlan2Type(&varcharType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxScanTag, //__mo_index_idx_col
				ColPos: 0,
			},
		},
	}

	switch filterExp.GetF().Func.ObjName {
	case "=":
		serialExpr1, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full",
			[]*plan.Expr{
				makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
				args[1], // value
			})

		filterList, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_eq", []*Expr{
			indexKeyCol, // __mo_index_idx_col
			serialExpr1, // serial_full("a","value")
		})
	case "between":
		serialExpr1, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
			makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
			args[1], // value1
		})
		serialExpr2, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
			makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
			args[2], // value2
		})
		filterList, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_between", []*Expr{
			indexKeyCol, // __mo_index_idx_col
			serialExpr1, // serial_full("a","value1")
			serialExpr2, // serial_full("a","value2")
		})

	case "in":
		// Since this master index specifically for varchar, we assume the `IN` to contain only varchar values.
		inVecType := types.T_varchar.ToType()

		// a. varchar vector ("value1", "value2", "value3")
		arg1AsColValuesVec := vector.NewVec(inVecType)
		_ = arg1AsColValuesVec.UnmarshalBinary(args[1].GetVec().GetData())
		inExprListLen := arg1AsColValuesVec.Length()

		// b. const vector "a"
		mp := mpool.MustNewZero()
		arg0AsColNameVec, _ := vector.NewConstBytes(inVecType, []byte(args[0].GetCol().Name), inExprListLen, mp)

		// c. (serial_full("a","value1"), serial_full("a","value2"), serial_full("a","value3"))
		ps := types.NewPackerArray(inExprListLen, mp)
		defer func() {
			for _, p := range ps {
				p.FreeMem()
			}
		}()
		function.SerialHelper(arg0AsColNameVec, nil, ps, true)
		function.SerialHelper(arg1AsColValuesVec, nil, ps, true)
		arg1ForPrefixInVec := vector.NewVec(inVecType)
		for i := 0; i < inExprListLen; i++ {
			_ = vector.AppendBytes(arg1ForPrefixInVec, ps[i].Bytes(), false, mp)
		}

		// d. convert result vector to LiteralVec
		arg1ForPrefixInBytes, _ := arg1ForPrefixInVec.MarshalBinary()
		arg1ForPrefixInLitVec := &plan.Expr{
			Typ: *makePlan2Type(&varcharType),
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:  int32(len(arg1ForPrefixInBytes)),
					Data: arg1ForPrefixInBytes,
				},
			},
		}

		// e. free memory for arg0, arg1 vector. Packer's memory is freed in defer.
		arg1ForPrefixInVec.Free(mp)
		arg0AsColNameVec.Free(mp)

		filterList, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_in", []*Expr{
			indexKeyCol,           // __mo_index_idx_col
			arg1ForPrefixInLitVec, // (serial_full("a","value1"), serial_full("a","value2"), serial_full("a","value3"))
		})

	default:
		panic("unsupported filter expression")
	}

	scanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    idxTableDef,
		ObjRef:      idxObjRef,
		FilterList:  []*plan.Expr{filterList},
		BindingTags: []int32{idxScanTag},
		Limit:       DeepCopyExpr(scanNode.Limit),
		Offset:      DeepCopyExpr(scanNode.Offset),
	}, bindCtx)

	// b. Project __mo_index_pk_col
	projPkCol := &Expr{
		Typ: *makePlan2Type(&varcharType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxScanTag, //__mo_index_pk_col
				ColPos: 1,
			},
		},
	}
	idxProjectTag := builder.genNewTag()
	projectId := builder.appendNode(&Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{scanId},
		ProjectList: []*Expr{projPkCol},
		BindingTags: []int32{idxProjectTag},
	}, bindCtx)

	return idxProjectTag, projectId
}

func isKeyPresentInList(key string, list []string) bool {
	for _, item := range list {
		if key == item {
			return true
		}
	}
	return false
}
