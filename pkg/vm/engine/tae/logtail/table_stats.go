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

package logtail

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
	"slices"
)

// tblId -> [account_id, db_id, tbl_id] ==> [{stats_name, stats_val, update_ts}...]

const (
	TblStatsDataSize   = "data size"
	TblStatsDataRowCnt = "data row cnt"
	TblStatsDataBlkCnt = "data blk cnt"
	TblStatsDataObjCnt = "data objs cnt"

	TblStatsTombstoneSize   = "tombstone size"
	TblStatsTombstoneRowCnt = "tombstone row cnt"
	TblStatsTombstoneBlkCnt = "tombstone blk cnt"
	TblStatsTombstoneObjCnt = "tombstone obj cnt"
)

type tablePair struct {
	tableId    uint64
	databaseId uint64
	accountId  uint64
}

type statsPair struct {
	statsName string
	statsVal  any
	updateTS  types.TS
}

type tableStatsPair struct {
	tbl       tablePair
	statsList []statsPair
}

type TblStatsBox struct {
	CC *catalog.Catalog
	// table id --> stats list
	statsMap           map[uint64]tableStatsPair
	statsUpdateHandler map[string]func(uint64, uint64, uint64, any, types.TS)
}

func NewTableStatsBox() *TblStatsBox {
	tsb := &TblStatsBox{
		statsMap: make(map[uint64]tableStatsPair),
	}

	tsb.init()
	return tsb
}

func (tsb *TblStatsBox) String() string {
	// acc-db-tbl
	//		|---- stats1, val, ts
	// 		|---- stats2, val, ts
	var buf bytes.Buffer

	for tid := range tsb.statsMap {
		acc := tsb.statsMap[tid].tbl.accountId
		did := tsb.statsMap[tid].tbl.databaseId

		dbEntry, err := tsb.CC.GetDatabaseByID(did)
		if err != nil {
			logutil.Error("get database from catalog failed",
				zap.Uint64("db id", did),
				zap.Error(err))
		}

		tblEntry, err := dbEntry.GetTableEntryByID(tid)
		if err != nil {
			logutil.Error("get database from catalog failed",
				zap.Uint64("db id", did),
				zap.Uint64("tbl id", tid),
				zap.Error(err))
		}

		buf.WriteString(fmt.Sprintf("%d-%d(%s)-%d(%s)",
			acc, did, dbEntry.GetName(), tid, tblEntry.GetName()))

		for _, s := range tsb.statsMap[tid].statsList {
			buf.WriteString(fmt.Sprintf("\n\t%s %v %s", s.statsName, s.statsVal, s.updateTS.ToString()))
		}

		buf.WriteString("\n")
	}

	return buf.String()
}

func (tsb *TblStatsBox) Get(tbl uint64, name string) (val any, ts types.TS) {
	tsp, ok := tsb.statsMap[tbl]
	if !ok {
		return nil, types.TS{}
	}

	idx := slices.IndexFunc(tsp.statsList, func(sp statsPair) bool { return sp.statsName == name })
	if idx == -1 {
		return nil, types.TS{}
	}

	return tsp.statsList[idx].statsVal, tsp.statsList[idx].updateTS
}

func (tsb *TblStatsBox) Update(
	acc uint64, db uint64, tbl uint64,
	name string, val any, ts types.TS) {

	tsb.statsUpdateHandler[name](acc, db, tbl, val, ts)
}

func (tsb *TblStatsBox) updateByRows(
	acc uint64, ee api.Entry, ts types.TS, isTombstone bool) {

	var (
		val1 any
		val2 any

		deltaRowCnt   int64
		deltaDataSize int64

		oldRowCnt   int64
		oldDataSize int64
	)

	deltaRowCnt = int64(ee.Bat.Vecs[0].Len)

	if !isTombstone {
		val1, _ = tsb.Get(ee.TableId, TblStatsDataRowCnt)
		val2, _ = tsb.Get(ee.TableId, TblStatsDataSize)
	} else {
		val1, _ = tsb.Get(ee.TableId, TblStatsTombstoneRowCnt)
		val2, _ = tsb.Get(ee.TableId, TblStatsTombstoneSize)
	}

	if val1 == nil {
		oldRowCnt = int64(0)
	} else {
		oldRowCnt = val1.(int64)
	}

	if val2 == nil {
		oldDataSize = int64(0)
	} else {
		oldDataSize = val2.(int64)
	}

	deltaDataSize = int64(float64(deltaRowCnt) / float64(oldRowCnt) * float64(oldDataSize))

	if !isTombstone {
		tsb.Update(acc, ee.DatabaseId, ee.TableId, TblStatsDataRowCnt, oldRowCnt+deltaRowCnt, ts)
		tsb.Update(acc, ee.DatabaseId, ee.TableId, TblStatsDataSize, oldDataSize+int64(deltaDataSize), ts)
	} else {
		tsb.Update(acc, ee.DatabaseId, ee.TableId, TblStatsTombstoneRowCnt, oldRowCnt+deltaRowCnt, ts)
		tsb.Update(acc, ee.DatabaseId, ee.TableId, TblStatsTombstoneSize, oldDataSize+int64(deltaDataSize), ts)
	}
}

func (tsb *TblStatsBox) updateByLogtailObjectList(
	acc uint64, ee api.Entry, ts types.TS, isTombstone bool) {
	statsVec := MustVectorFromProto(ee.Bat.Vecs[2])
	defer statsVec.Free(common.LogtailAllocator)

	vec := MustVectorFromProto(ee.Bat.Vecs[6])
	defer vec.Free(common.LogtailAllocator)
	deleteTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	for i := 0; i < len(deleteTSCol); i++ {
		stats := objectio.ObjectStats(statsVec.GetBytesAt(i))
		if deleteTSCol[i].IsEmpty() {
			// new obj
			tsb.UpdateByObjectStats(acc, ee.DatabaseId, ee.TableId, stats, ts, isTombstone, false)
		} else {
			tsb.UpdateByObjectStats(acc, ee.DatabaseId, ee.TableId, stats, ts, isTombstone, true)
		}
	}
}

func (tsb *TblStatsBox) UpdateByLogtail(tails *[]logtail.TableLogtail) {
	for _, tail := range *tails {
		acc := uint64(tail.Table.AccId)
		ts := types.TimestampToTS(*tail.Ts)

		for _, ee := range tail.Commands {
			if ee.EntryType == api.Entry_Delete {
				tsb.updateByRows(acc, ee, ts, true)
			} else if ee.EntryType == api.Entry_Insert {
				if IsDataObjectList(ee.TableName) {
					tsb.updateByLogtailObjectList(acc, ee, ts, false)
				} else if IsTombstoneObjectList(ee.TableName) {
					tsb.updateByLogtailObjectList(acc, ee, ts, true)
				} else {
					tsb.updateByRows(acc, ee, ts, false)
				}
			} else {
				panic(ee.String())
			}
		}
	}
}

func (tsb *TblStatsBox) UpdateByObjectStats(
	acc uint64, db uint64, tbl uint64,
	objStats objectio.ObjectStats, ts types.TS,
	isTombstone bool, isDelete bool) {

	rowCnt := int64(objStats.Rows())
	blkCnt := int64(objStats.BlkCnt())
	cmpSize := int64(objStats.Size())

	factor := int64(1)
	if isDelete {
		factor = -1
	}

	if !isTombstone {
		tsb.Update(acc, db, tbl, TblStatsDataSize, factor*cmpSize, ts)
		tsb.Update(acc, db, tbl, TblStatsDataRowCnt, factor*rowCnt, ts)
		tsb.Update(acc, db, tbl, TblStatsDataBlkCnt, factor*blkCnt, ts)
		tsb.Update(acc, db, tbl, TblStatsDataObjCnt, factor*1, ts)
	} else {
		tsb.Update(acc, db, tbl, TblStatsTombstoneSize, factor*cmpSize, ts)
		tsb.Update(acc, db, tbl, TblStatsTombstoneRowCnt, factor*rowCnt, ts)
		tsb.Update(acc, db, tbl, TblStatsTombstoneBlkCnt, factor*blkCnt, ts)
		tsb.Update(acc, db, tbl, TblStatsTombstoneObjCnt, factor*1, ts)
	}
}

func (tsb *TblStatsBox) printTask() {
	//ticker := time.NewTicker(time.Second * 10)
	//for range ticker.C {
	//	fmt.Println(tsb.String())
	//	fmt.Println()
	//	fmt.Println()
	//}
}

func (tsb *TblStatsBox) init() {
	tsb.statsUpdateHandler = make(map[string]func(uint64, uint64, uint64, any, types.TS))

	int64StatsList := []string{
		TblStatsDataObjCnt, TblStatsTombstoneObjCnt,
		TblStatsDataSize, TblStatsTombstoneSize,
		TblStatsDataBlkCnt, TblStatsTombstoneBlkCnt,
		TblStatsDataRowCnt, TblStatsTombstoneRowCnt,
	}

	for _, name := range int64StatsList {
		tsb.statsUpdateHandler[name] = func(acc uint64, db uint64, tbl uint64, val any, ts types.TS) {
			sizeDelta := val.(int64)
			tsp := tsb.statsMap[tbl]

			tsp.tbl.tableId = tbl
			tsp.tbl.accountId = acc
			tsp.tbl.databaseId = db

			idx := slices.IndexFunc(tsp.statsList, func(sp statsPair) bool { return sp.statsName == name })
			if idx == -1 {
				tsp.statsList = append(tsp.statsList, statsPair{
					statsName: name,
					statsVal:  sizeDelta,
					updateTS:  ts,
				})
			} else {
				tsp.statsList[idx].updateTS = ts

				old := tsp.statsList[idx].statsVal.(int64)
				tsp.statsList[idx].statsVal = old + sizeDelta
			}

			tsb.statsMap[tbl] = tsp
		}
	}

	go tsb.printTask()
}
