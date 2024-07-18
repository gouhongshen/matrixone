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

package testutil

import (
	"context"
	"fmt"
	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"os/user"
	"path/filepath"
	"testing"
)

func GetDefaultTestPath(module string, t *testing.T) string {
	usr, _ := user.Current()
	dirName := fmt.Sprintf("%s-ut-workspace", usr.Username)
	return filepath.Join("/tmp", dirName, module, t.Name())
}

func MakeDefaultTestPath(module string, t *testing.T) string {
	path := GetDefaultTestPath(module, t)
	err := os.MkdirAll(path, os.FileMode(0755))
	assert.Nil(t, err)
	return path
}

func RemoveDefaultTestPath(module string, t *testing.T) {
	path := GetDefaultTestPath(module, t)
	os.RemoveAll(path)
}

func InitTestEnv(module string, t *testing.T) string {
	RemoveDefaultTestPath(module, t)
	return MakeDefaultTestPath(module, t)
}

func CreateEngines(ctx context.Context, opts TestOptions,
	t *testing.T) (disttaeEngine *TestDisttaeEngine, taeEngine *TestTxnStorage,
	rpcAgent *MockRPCAgent, mp *mpool.MPool) {

	if v := ctx.Value(defines.TenantIDKey{}); v == nil {
		panic("cannot find account id in ctx")
	}

	var err error

	mp, err = mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent = NewMockLogtailAgent()

	taeEngine, err = NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, opts.TaeEngineOptions)
	require.Nil(t, err)

	disttaeEngine, err = NewTestDisttaeEngine(ctx, mp, taeEngine.GetDB().Runtime.Fs.Service, rpcAgent, taeEngine)
	require.Nil(t, err)

	return
}

func GetDefaultTNShard() metadata.TNShard {
	return metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{
			ShardID:    0,
			LogShardID: 1,
		},
		ReplicaID: 0x2f,
		Address:   "echo to test",
	}
}

func EngineTableDefBySchema(schema *catalog.Schema) ([]engine.TableDef, error) {
	var defs = make([]engine.TableDef, 0)
	for idx := range schema.ColDefs {
		if schema.ColDefs[idx].Name == catalog2.Row_ID {
			continue
		}

		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{
				Type:          schema.ColDefs[idx].Type,
				IsRowId:       schema.ColDefs[idx].Name == catalog2.Row_ID,
				Name:          schema.ColDefs[idx].Name,
				ID:            uint64(schema.ColDefs[idx].Idx),
				Primary:       schema.ColDefs[idx].IsPrimary(),
				IsHidden:      schema.ColDefs[idx].IsHidden(),
				Seqnum:        schema.ColDefs[idx].SeqNum,
				ClusterBy:     schema.ColDefs[idx].ClusterBy,
				AutoIncrement: schema.ColDefs[idx].AutoIncrement,
			},
		})
	}

	if schema.Constraint != nil {
		var con engine.ConstraintDef
		err := con.UnmarshalBinary(schema.Constraint)
		if err != nil {
			return nil, err
		}

		defs = append(defs, &con)
	}

	return defs, nil
}

func PlanTableDefBySchema(schema *catalog.Schema, tableId uint64, databaseName string) plan.TableDef {
	tblDef := plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{},
	}

	tblDef.Name = schema.Name
	tblDef.TblId = tableId

	for idx := range schema.ColDefs {
		tblDef.Cols = append(tblDef.Cols, &plan.ColDef{
			ColId:     uint64(schema.ColDefs[idx].Idx),
			Name:      schema.ColDefs[idx].Name,
			Hidden:    schema.ColDefs[idx].Hidden,
			NotNull:   !schema.ColDefs[idx].Nullable(),
			TblName:   schema.Name,
			DbName:    databaseName,
			ClusterBy: schema.ColDefs[idx].ClusterBy,
			Primary:   schema.ColDefs[idx].IsPrimary(),
			Pkidx:     int32(schema.GetPrimaryKey().Idx),
			Typ: plan.Type{
				Id:          int32(schema.ColDefs[idx].Type.Oid),
				NotNullable: !schema.ColDefs[idx].Nullable(),
				Width:       schema.ColDefs[idx].Type.Oid.ToType().Width,
			},
			Seqnum: uint32(schema.ColDefs[idx].Idx),
		})
	}

	tblDef.Pkey.PkeyColName = schema.GetPrimaryKey().Name
	tblDef.Pkey.PkeyColId = uint64(schema.GetPrimaryKey().Idx)
	tblDef.Pkey.Names = append(tblDef.Pkey.Names, schema.GetPrimaryKey().Name)
	tblDef.Pkey.CompPkeyCol = nil
	tblDef.Pkey.Cols = append(tblDef.Pkey.Cols, uint64(schema.GetPrimaryKey().Idx))

	tblDef.Name2ColIndex = make(map[string]int32)
	for idx := range schema.ColDefs {
		tblDef.Name2ColIndex[schema.ColDefs[idx].Name] = int32(schema.ColDefs[idx].Idx)
	}

	return tblDef
}

func NewDefaultTableReader(
	ctx context.Context,
	rel engine.Relation,
	databaseName string,
	schema *catalog.Schema,
	expr *plan.Expr,
	proc *process.Process,
	fs fileservice.FileService,
	snapshotTS timestamp.Timestamp,
	packerPool *fileservice.Pool[*types.Packer],
) (*disttae.ReaderInProgress, error) {

	tblDef := PlanTableDefBySchema(schema, rel.GetTableID(ctx), databaseName)

	source, err := disttae.BuildLocalDataSource(ctx, rel, nil)
	if err != nil {
		return nil, err
	}

	r := disttae.NewReaderInProgress(
		ctx,
		proc,
		fs,
		packerPool,
		&tblDef,
		snapshotTS,
		expr,
		false,
		0,
		source,
	)

	return r, nil
}
