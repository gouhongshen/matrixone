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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	newdisttae "github.com/matrixorigin/matrixone/pkg/vm/engine/newdisttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"sync"
	"time"
)

type TestDisttaeEngine struct {
	txnStorage *TestTxnStorage
	engine     *newdisttae.Engine
	//logtailReceiveQueue sm.Queue
	logtailReceiver chan morpc.Message
	broken          chan struct{}
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewTestDisttaeEngine(ctx context.Context, txnStorage *TestTxnStorage, mp *mpool.MPool) *TestDisttaeEngine {
	de := new(TestDisttaeEngine)

	de.logtailReceiver = make(chan morpc.Message)
	de.broken = make(chan struct{})

	de.txnStorage = txnStorage
	de.ctx, de.cancel = context.WithCancel(ctx)

	de.engine = newdisttae.New(
		ctx, mp, txnStorage.taeHandler.GetDB().Runtime.Fs.Service, client.NewTxnClient(nil),
		nil, nil, 0)

	go de.engine.PClient.Connector.Run(ctx)
	go de.logtailReceiveWorker()

	return de
}

func (de *TestDisttaeEngine) logtailReceiveWorker() {
	de.wg.Add(1)
	go func() {
		defer de.wg.Done()
		for {
			select {
			case <-de.ctx.Done():
				break
			default:
				resp, err := service.LogtailResponseReceive(de.ctx, de.logtailReceiver, de.broken)
				if r := resp.GetUpdateResponse(); r != nil {

				} else if r := resp.GetSubscribeResponse(); r != nil {

				} else if r := resp.GetUnsubscribeResponse(); r != nil {

				}
			}
		}
	}()
}

func (de *TestDisttaeEngine) Now() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
}

func (de *TestDisttaeEngine) Close(ctx context.Context) {
	de.txnStorage.Close(ctx)
	close(de.logtailReceiver)
	de.cancel()
	de.wg.Wait()
}

/////////////////////////////////////////// test Interface

func (de *TestDisttaeEngine) CreateDatabase(
	ctx context.Context, datType, sql string, accountId, userId, roleId uint32, databaseId uint64,
	databaseName string, m *mpool.MPool) (response *txn.TxnResponse, err error) {

	commitReq, err := newdisttae.MockGenCreateDatabaseCommitRequest(
		datType, sql, accountId, userId, roleId, databaseId, databaseName, m, de.Now())
	if err != nil {
		return nil, err
	}

	return writeAndCommitRequest(ctx, de.txnStorage, commitReq)
}

func (de *TestDisttaeEngine) CreateTable(
	ctx context.Context, sql string, accountId, userId, roleId uint32,
	tableName string, tableId uint64, databaseId uint64,
	databaseName string, m *mpool.MPool) (response *txn.TxnResponse, err error) {

	commitReq, err := newdisttae.MockGenCreateTableCommitRequest(
		sql, accountId, userId, roleId, tableName, tableId, databaseId, databaseName, m, de.Now())

	if err != nil {
		return nil, err
	}

	return writeAndCommitRequest(ctx, de.txnStorage, commitReq)
}

func writeAndCommitRequest(
	ctx context.Context, txnHandler *TestTxnStorage, commitReq []*txn.TxnRequest) (
	response *txn.TxnResponse, err error) {

	reqs := txn.TxnRequest{
		Method: txn.TxnMethod_Commit,
		Flag:   txn.SkipResponseFlag,
		CommitRequest: &txn.TxnCommitRequest{
			Payload:       commitReq,
			Disable1PCOpt: false,
		}}

	response = new(txn.TxnResponse)
	response.CNOpResponse = &txn.CNOpResponse{}
	req := reqs.CommitRequest.Payload[0]
	_, err = txnHandler.Write(ctx, req.Txn, req.CNRequest.OpCode, req.CNRequest.Payload)
	if err != nil {
		util.LogTxnWriteFailed(txn.TxnMeta{}, err)
		response.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
		return nil, err
	}

	_, err = txnHandler.Commit(ctx, commitReq[0].Txn)
	return response, err
}

func (de *TestDisttaeEngine) Subscribe(ctx context.Context, dbId, tblId uint64) error {
	table := api.TableID{DbId: dbId, TbId: tblId}
	request := &service.LogtailRequest{}
	request.Request = &logtail.LogtailRequest_SubscribeTable{
		SubscribeTable: &logtail.SubscribeRequest{
			Table: &table,
		},
	}

	rpcMsg := morpc.RPCMessage{Message: request}

	return de.txnStorage.logtailServer.OnMessage(ctx, rpcMsg, 0, newTestClientSession(de.logtailReceiver))
}
