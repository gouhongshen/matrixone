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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type TxnOperation interface {
	CreateDatabase(ctx context.Context, datType, sql string, accountId, userId,
		roleId uint32, databaseId uint64, databaseName string, m *mpool.MPool) (response *txn.TxnResponse)

	CreateTable(ctx context.Context, sql string, schema *catalog.Schema,
		tableId uint64, databaseId uint64, databaseName string, m *mpool.MPool) (response *txn.TxnResponse)

	//AlterTable()
	//DropTable()
	//DropDatabase()

	//Select()
	//Insert()
	//Delete()
	//Update()
}
