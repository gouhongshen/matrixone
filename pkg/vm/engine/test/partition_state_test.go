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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	testutil "github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_X(t *testing.T) {

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	ctx := context.Background()
	taeHandler, err := testutil.NewTestTAEEngine(ctx, "partition_state", t, nil)
	require.Nil(t, err)

	disttaeEngine := testutil.NewTestDisttaeEngine(ctx, taeHandler, mp)
	defer disttaeEngine.Close(ctx)

	var (
		tableId      = 9999
		databaseId   = 9999
		tableName    = "test1"
		databaseName = "db1"
	)

	_, err = disttaeEngine.CreateDatabase(ctx, "", "", 0, 0, 0, uint64(databaseId), databaseName, mp)
	require.Nil(t, err)

	_, err = disttaeEngine.CreateTable(ctx, "", 0, 0, 0, tableName, uint64(tableId), uint64(databaseId), databaseName, mp)

	require.Nil(t, err)
	time.Sleep(time.Second)

	err = disttaeEngine.Subscribe(ctx, uint64(databaseId), uint64(tableId))
	require.Nil(t, err)

	time.Sleep(time.Second)
}
