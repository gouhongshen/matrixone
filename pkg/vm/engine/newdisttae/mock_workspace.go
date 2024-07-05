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

package disttae

import (
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func cnCommitRequest(es []Entry, tn DNStore, snapshot timestamp.Timestamp) ([]*txn.TxnRequest, error) {
	var apiEntry []*api.Entry

	for idx := range es {
		pe, err := toPBEntry(es[idx])
		if err != nil {
			return nil, err
		}
		apiEntry = append(apiEntry, pe)
	}

	payload, err := types.Encode(&api.PrecommitWriteCmd{EntryList: apiEntry})
	if err != nil {
		return nil, err
	}

	txnMeta := txn.TxnMeta{}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	txnMeta.ID = id[:]
	txnMeta.SnapshotTS = snapshot

	return []*txn.TxnRequest{&txn.TxnRequest{
		CNRequest: &txn.CNOpRequest{
			OpCode:  uint32(api.OpCode_OpPreCommit),
			Payload: payload,
			Target: metadata.TNShard{
				TNShardRecord: metadata.TNShardRecord{
					ShardID: tn.Shards[0].ShardID,
				},
				ReplicaID: tn.Shards[0].ReplicaID,
				Address:   tn.TxnServiceAddress,
			},
		},
		Txn: txnMeta,
		Options: &txn.TxnRequestOptions{
			RetryCodes: []int32{
				// tn shard not found
				int32(moerr.ErrTNShardNotFound),
			},
			RetryInterval: int64(time.Second),
		},
	}}, nil
}

var mockRowIdAllocatorByTID = make(map[uint64][]uint32)

func MockIncrBlockId(tableId uint64) {
	mockRowIdAllocator := mockRowIdAllocatorByTID[tableId]

	idx := len(mockRowIdAllocator) - 2
	for mockRowIdAllocator[idx] == math.MaxUint32 {
		idx--
	}
	mockRowIdAllocator[idx]++
	for idx < len(mockRowIdAllocator)-1 {
		idx++
		mockRowIdAllocator[idx] = 0
	}
}

func MockGenRowId(tableId uint64) types.Rowid {
	mockRowIdAllocator := mockRowIdAllocatorByTID[tableId]
	mockRowIdAllocator[5]++
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(mockRowIdAllocator[:]))
}

func appendRowIdVec(tableId uint64, src *batch.Batch, m *mpool.MPool) (*batch.Batch, error) {
	vec := vector.NewVec(types.T_Rowid.ToType())
	MockIncrBlockId(tableId)
	rowId := MockGenRowId(tableId)
	if err := vector.AppendFixed(vec, rowId, false, m); err != nil {
		vec.Free(m)
		return nil, err
	}

	src.Vecs = append([]*vector.Vector{vec}, src.Vecs...)
	src.Attrs = append([]string{catalog.Row_ID}, src.Attrs...)

	return src, nil
}

func mockGenCreateDatabaseTuple(sql string, accountId, userId, roleId uint32,
	name string, databaseId uint64, typ string, m *mpool.MPool) (*batch.Batch, error) {
	bat, err := genCreateDatabaseTuple(sql, accountId, userId, roleId, name, databaseId, typ, m)
	if err != nil {
		return nil, err
	}

	return appendRowIdVec(catalog.MO_DATABASE_ID, bat, m)
}

func mockGenCreateTableTuple(
	tbl *txnTable, sql string, accountId, userId, roleId uint32, name string,
	tableId uint64, databaseId uint64, databaseName string, m *mpool.MPool) (*batch.Batch, error) {
	bat, err := genCreateTableTuple(tbl, sql, accountId, userId, roleId, name,
		tableId, databaseId, databaseName, types.Rowid{}, false, m)
	if err != nil {
		return nil, err
	}

	return appendRowIdVec(catalog.MO_TABLES_ID, bat, m)
}

func MockInsertRowsCommitRequest(
	accountId uint32, databaseId uint64, databaseName string, tableId uint64,
	tableName string, bat *batch.Batch, m *mpool.MPool, snapshot timestamp.Timestamp) ([]*txn.TxnRequest, error) {

	if bat.Attrs[0] != catalog.Row_ID {
		rowIdVec := vector.NewVec(types.T_Rowid.ToType())
		MockIncrBlockId(tableId)
		for idx := 0; idx < bat.RowCount(); idx++ {
			rowId := MockGenRowId(tableId)
			if err := vector.AppendFixed[types.Rowid](rowIdVec, rowId, false, m); err != nil {
				return nil, err
			}
		}

		bat.Vecs = append([]*vector.Vector{rowIdVec}, bat.Vecs...)
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
	}

	tnStore := func() DNStore {
		return metadata.TNService{
			ServiceID:         uuid.NewString(),
			TxnServiceAddress: "1",
			Shards: []metadata.TNShard{
				{
					TNShardRecord: metadata.TNShardRecord{ShardID: 2},
					ReplicaID:     rand.Uint64() % 0x11235,
				},
			},
		}
	}

	e := Entry{
		typ:          INSERT,
		accountId:    accountId,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		tnStore:      tnStore(),
		truncate:     false,
	}

	return cnCommitRequest([]Entry{e}, e.tnStore, snapshot)
}

func MockInsertDataObjectsCommitRequest() {

}

func MockInsertTombstoneObjectsCommitRequest() {

}

func MockDeleteRowsCommitRequest() {

}

func MockGenCreateDatabaseCommitRequest(
	datType, sql string, accountId, userId, roleId uint32,
	databaseId uint64, databaseName string, m *mpool.MPool, snapshot timestamp.Timestamp) ([]*txn.TxnRequest, error) {

	bat, err := mockGenCreateDatabaseTuple(sql, accountId, userId, roleId, databaseName, databaseId, datType, m)
	if err != nil {
		return nil, err
	}

	tnStore := func() DNStore {
		return metadata.TNService{
			ServiceID:         uuid.NewString(),
			TxnServiceAddress: "1",
			Shards: []metadata.TNShard{
				{
					TNShardRecord: metadata.TNShardRecord{ShardID: 2},
					ReplicaID:     rand.Uint64() % 0x11235,
				},
			},
		}
	}

	e := Entry{
		typ:          INSERT,
		accountId:    accountId,
		bat:          bat,
		tableId:      catalog.MO_DATABASE_ID,
		databaseId:   catalog.MO_CATALOG_ID,
		tableName:    catalog.MO_DATABASE,
		databaseName: catalog.MO_CATALOG,
		tnStore:      tnStore(),
		truncate:     false,
	}

	return cnCommitRequest([]Entry{e}, e.tnStore, snapshot)
}

func MockGenCreateTableCommitRequest(
	sql string, schema *catalog2.Schema, tableId uint64, databaseId uint64, databaseName string,
	m *mpool.MPool, snapshot timestamp.Timestamp) ([]*txn.TxnRequest, error) {
	txnTbl := new(txnTable)
	bat, err := mockGenCreateTableTuple(
		new(txnTable), sql, schema.AcInfo.TenantID, schema.AcInfo.UserID, schema.AcInfo.UserID,
		schema.Name, tableId, databaseId, databaseName, m)
	if err != nil {
		return nil, err
	}

	tnStore := func() DNStore {
		return metadata.TNService{
			ServiceID:         uuid.NewString(),
			TxnServiceAddress: "1",
			Shards: []metadata.TNShard{
				{
					TNShardRecord: metadata.TNShardRecord{ShardID: 2},
					ReplicaID:     rand.Uint64() % 0x11235,
				},
			},
		}
	}

	e := Entry{
		typ:          INSERT,
		accountId:    accountId,
		bat:          bat,
		tableId:      catalog.MO_TABLES_ID,
		databaseId:   catalog.MO_CATALOG_ID,
		tableName:    catalog.MO_TABLES,
		databaseName: catalog.MO_CATALOG,
		tnStore:      tnStore(),
		truncate:     false,
	}

	return cnCommitRequest([]Entry{e}, e.tnStore, snapshot)
}
