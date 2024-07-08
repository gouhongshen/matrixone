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

package test

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	testutil "github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_X(t *testing.T) {
	var (
		accountId = catalog.System_Account
		tableId   = 9999
		//databaseId   = 9999
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent := testutil.NewMockLogtailAgent()
	defer rpcAgent.Close()

	taeHandler, err := testutil.NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, nil)
	require.Nil(t, err)

	disttaeEngine, err := testutil.NewTestDisttaeEngine(ctx, mp, taeHandler.GetDB().Runtime.Fs.Service, rpcAgent)
	require.Nil(t, err)
	defer disttaeEngine.Close(ctx)

	txnOp, err := disttaeEngine.NewTxnOperator(ctx, timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()})
	require.Nil(t, err)

	resp, _ := rpcAgent.CreateDatabase(ctx, databaseName, disttaeEngine.Engine, txnOp)
	require.Nil(t, resp.TxnError)

	//resp = rpcAgent.CreateTable(ctx, "", accountId, 0, 0, tableName, uint64(tableId), uint64(databaseId), databaseName, mp)

	require.Nil(t, resp.TxnError)
	time.Sleep(time.Second)

	dbName, tblName, _, err := disttaeEngine.Engine.GetRelationById(ctx, txnOp, uint64(tableId))
	require.Nil(t, err)
	require.Equal(t, dbName, databaseName)
	require.Equal(t, tblName, tableName)

	time.Sleep(time.Second)
}

func Test_Y(t *testing.T) {
	var (
		accountId    = catalog.System_Account
		tableId      uint64
		databaseId   uint64
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctx = context.WithValue(ctx, defines.SqlKey{}, "create table test")

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent := testutil.NewMockLogtailAgent()
	defer rpcAgent.Close()

	taeHandler, err := testutil.NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, nil)
	require.Nil(t, err)

	disttaeEngine, err := testutil.NewTestDisttaeEngine(ctx, mp, taeHandler.GetDB().Runtime.Fs.Service, rpcAgent)
	require.Nil(t, err)
	defer disttaeEngine.Close(ctx)

	txnOp, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())

	resp, dbId := rpcAgent.CreateDatabase(ctx, databaseName, disttaeEngine.Engine, txnOp)
	require.Nil(t, resp.TxnError)

	databaseId = dbId

	schema := catalog2.MockSchemaAll(3, 0)
	bat := catalog2.MockBatch(schema, 10)
	schema.Name = tableName

	txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txnOp)
	require.Nil(t, err)
	require.NotNil(t, db)

	fmt.Println("RRRRR")
	resp, tableId = rpcAgent.CreateTable(ctx, db, schema, txnOp.SnapshotTS())
	require.Nil(t, resp.TxnError)

	fmt.Println("YYYY")

	time.Sleep(time.Second)

	entry, err := taeHandler.GetDB().Catalog.GetDatabaseByID(databaseId)
	require.Nil(t, err)

	tt, err := entry.GetTableEntryByID(tableId)
	require.Nil(t, err)

	fmt.Println(tt.GetFullName(), tt.GetLastestSchemaLocked(false).Attrs())

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	dbName, tblName, rel, err := disttaeEngine.Engine.GetRelationById(ctx, txnOp, uint64(tableId))
	require.Nil(t, err)
	require.Equal(t, dbName, databaseName)
	require.Equal(t, tblName, tableName)

	fmt.Println(rel.GetTableName(), rel.GetTableID(ctx))
	rpcAgent.Insert(ctx, accountId, rel, databaseName, bat, mp, txnOp.SnapshotTS())
	require.Nil(t, resp.TxnError)

	fmt.Println("XXXX")

	time.Sleep(time.Second * 2)

	err = disttaeEngine.Engine.PClient.TryToSubscribeTable(ctx, dbId, tableId)
	require.Nil(t, err)

	rows, err := disttaeEngine.CountStar(ctx, uint64(databaseId), uint64(tableId))
	require.Nil(t, err)
	fmt.Println(rows)

	time.Sleep(time.Second)
}
