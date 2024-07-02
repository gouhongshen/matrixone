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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
	"testing"
)

type TestTxnStorage struct {
	t        *testing.T
	schema   *catalog.Schema
	tenantID uint32

	taeHandler    *rpc.Handle
	logtailServer *TestLogtailServer
}

func (ts *TestTxnStorage) Start() error { return nil }

func (ts *TestTxnStorage) StartRecovery(context.Context, chan txn.TxnMeta) {}
func (ts *TestTxnStorage) Close(context.Context) error                     { return nil }
func (ts *TestTxnStorage) Destroy(context.Context) error                   { return nil }
func (ts *TestTxnStorage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (storage.ReadResult, error) {
	return nil, nil
}
func (ts *TestTxnStorage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
	switch op {
	case uint32(apipb.OpCode_OpPreCommit):
		return taestorage.HandleWrite(ctx, txnMeta, payload, ts.taeHandler.HandlePreCommitWrite)
	default:
		return nil, moerr.NewNotSupported(ctx, "unknown write op: %v", op)
	}
}
func (ts *TestTxnStorage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	return timestamp.Timestamp{}, nil
}
func (ts *TestTxnStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error { return nil }
func (ts *TestTxnStorage) Commit(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	return ts.taeHandler.HandleCommit(ctx, txnMeta)
}
func (ts *TestTxnStorage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error { return nil }
func (ts *TestTxnStorage) Debug(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
	return nil, nil
}

func NewTestTAEEngine(ctx context.Context, moduleName string, t *testing.T, opts *options.Options) (*TestTxnStorage, error) {
	blockio.Start()
	taeHandler := InitTestDB(ctx, moduleName, t, opts)
	logtailserver, err := NewMockLogtailServer(ctx, taeHandler.GetDB(), defaultLogtailConfig(), runtime.DefaultRuntime())
	if err != nil {
		return nil, err
	}

	err = logtailserver.Start()
	if err != nil {
		return nil, err
	}

	return &TestTxnStorage{
		taeHandler:    taeHandler,
		logtailServer: logtailserver,
	}, nil
}

func InitTestDB(ctx context.Context, moduleName string, t *testing.T, opts *options.Options) *rpc.Handle {
	dir := InitTestEnv(moduleName, t)
	handle := rpc.NewTAEHandle(ctx, dir, opts)
	handle.GetDB().DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			minTS := handle.GetDB().TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			end := ckp.GetEnd()
			return !end.GreaterEq(&minTS)
		})

	return handle
}
