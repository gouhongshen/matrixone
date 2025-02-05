package disttae

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"runtime/debug"
	"sync"
	"time"
)

var _ client.BatchCommitter = new(batchCommiter)

type batchCommiter struct {
	ctx   context.Context
	queue sm.Queue

	eng    *Engine
	sender rpc.TxnSender
	client client.TxnClient

	idCache sync.Map
}

func NewBatchCommiter(
	ctx context.Context,
	eng *Engine,
	client client.TxnClient,
	sender rpc.TxnSender,
) client.BatchCommitter {

	bc := &batchCommiter{}
	bc.queue = sm.NewSafeQueue(100, 1, bc.onBatchCommit)

	bc.ctx = ctx
	bc.eng = eng
	bc.client = client
	bc.sender = sender

	bc.queue.Start()

	return bc
}

type bcReqWithNotifier struct {
	client.BatchCommitReq
	notifier chan client.BatchCommitResp
	stack    string
}

func (bc *batchCommiter) BatchCommit(
	req client.BatchCommitReq,
) client.BatchCommitResp {

	notifier := make(chan client.BatchCommitResp)
	withNotifier := bcReqWithNotifier{
		BatchCommitReq: req,
		notifier:       notifier,
		stack:          string(debug.Stack()),
	}

	bc.queue.Enqueue(withNotifier)

	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			fmt.Printf("waited 10s, readOnly: %v, %p, queued: %v\n",
				req.WS.Readonly(), notifier, bc.queue.Len())
		case resp := <-notifier:
			return resp
		}
	}
}

func (bc *batchCommiter) mergeWorkspace(
	ws ...client.Workspace,
) (*Transaction, error) {

	finally := &Transaction{}

	hasWrite := false

	for i := range ws {
		if ws[i] == nil {
			continue
		}
		item := ws[i].(*Transaction)

		if finally.engine == nil {
			finally = item.CloneSnapshotWS().(*Transaction)
		}

		hasWrite = hasWrite || !item.ReadOnly()

		if item.Readonly() {
			continue
		}

		if item.tableCache != nil {
			item.tableCache.Range(func(key, value any) bool {
				finally.tableCache.Store(key, value)
				return true
			})
		}

		finally.writes = append(finally.writes, item.writes...)
	}

	finally.readOnly.Store(!hasWrite)

	ts := timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
	op, err := bc.client.New(bc.ctx,
		timestamp.Timestamp{},
		client.WithSkipPushClientReady(),
		client.WithSnapshotTS(ts))
	if err != nil {
		return nil, err
	}

	op.AddWorkspace(finally)
	finally.BindTxnOp(op)

	return finally, nil
}

/*

1. why cannot read from temporary table??
2. why temporary table creation is ok only when restart with no data ??

*/

func (bc *batchCommiter) singleCommit(item bcReqWithNotifier) {
	requests := item.Extra

	if item.WS != nil && !item.WS.Readonly() && item.Commit {
		//err := item.WS.TransferTombstoneByCommit(item.Ctx)
		//if err != nil {
		//	item.notifier <- client.BatchCommitResp{
		//		Err: err,
		//	}
		//}

		writes, err := genWriteReqs(item.Ctx, item.WS.(*Transaction))
		if err != nil {
			item.notifier <- client.BatchCommitResp{
				Err: err,
			}
		}

		var txnReqs []*txn.TxnRequest
		for i := range writes {
			//writes[i].Txn = item.Meta
			txnReqs = append(txnReqs, &writes[i])

			exist := false
			for _, tn := range item.Meta.TNShards {
				if tn.ShardID == writes[i].GetTargetTN().ShardID {
					exist = true
					break
				}
			}

			if !exist {
				item.Meta.TNShards = append(item.Meta.TNShards, writes[i].GetTargetTN())
			}
		}

		requests = append(requests, txn.TxnRequest{
			Txn:    item.Meta,
			Method: txn.TxnMethod_Commit,
			Flag:   txn.SkipResponseFlag,
			CommitRequest: &txn.TxnCommitRequest{
				Payload: txnReqs,
			}})
	}

	for i := range requests {
		requests[i].Txn = item.Meta
	}

	rpcRet, err := bc.sender.Send(item.Ctx, requests)

	item.notifier <- client.BatchCommitResp{
		Err:    err,
		RPCRet: rpcRet,
	}

	for i := range requests {
		fmt.Println(requests[i].Method.String(), item.Meta.ID,
			requests[i].CNRequest == nil,
			requests[i].CommitRequest == nil,
			requests[i].GetTargetTN().Address)
	}
}

func (bc *batchCommiter) onBatchCommit(items ...any) {

	//sent := false
	ws := make([]client.Workspace, 0)
	withNotifiers := make([]bcReqWithNotifier, 0)

	defer func() {
		//fmt.Printf("%p, %v\n\n", &items[0], "Exit")

		//if v := withNotifiers[0].Ctx.Value(defines.TemporaryTN{}); v != nil {
		//	fmt.Println("found temporary tn", sent)
		//}
	}()

	notifyAll := func(rpcRet *rpc.SendResult, err error) {
		for i := range withNotifiers {
			if withNotifiers[i].WS != nil {
				withNotifiers[i].WS.(*Transaction).delTransaction()
			}
			withNotifiers[i].notifier <- client.BatchCommitResp{
				Err:    err,
				RPCRet: rpcRet,
			}
		}
	}

	for i := range items {
		item := items[i].(bcReqWithNotifier)

		idStr := string(item.Meta.ID)
		if _, ok := bc.idCache.Load(idStr); ok || !item.Commit {
			bc.idCache.Store(idStr, struct{}{})
			bc.singleCommit(item)
			if item.Commit {
				bc.idCache.Delete(idStr)
			}
			continue
		}

		ws = append(ws, item.WS)
		withNotifiers = append(withNotifiers, item)
	}

	var (
		err   error
		newWS *Transaction
	)

	if newWS, err = bc.mergeWorkspace(ws...); err != nil {
		notifyAll(nil, err)
		return
	}

	if !newWS.ReadOnly() {
		//if err = newWS.transferTombstonesByCommit(bc.ctx); err != nil {
		//	notifyAll(nil, err)
		//	return
		//}
	}

	writes, err := genWriteReqs(bc.ctx, newWS)
	if err != nil {
		notifyAll(nil, err)
		return
	}

	var commitReqs = make([]txn.TxnRequest, 1)

	commitReqs[0] = txn.TxnRequest{
		Txn:           txn.TxnMeta{},
		Method:        txn.TxnMethod_Commit,
		Flag:          txn.SkipResponseFlag,
		CommitRequest: &txn.TxnCommitRequest{},
	}

	commitReqs[0].Txn.SnapshotTS = newWS.op.SnapshotTS()
	id := uuid.New()
	commitReqs[0].Txn.ID = id[:]

	tryAppendTNShard := func(tn metadata.TNShard) {
		for _, shard := range commitReqs[0].Txn.TNShards {
			if shard.ShardID == tn.ShardID {
				return
			}
		}
		commitReqs[0].Txn.TNShards = append(commitReqs[0].Txn.TNShards, tn)
	}

	for i := range withNotifiers {
		commitReqs = append(commitReqs, withNotifiers[i].Extra...)
		commitReqs[0].Txn.LockTables = append(commitReqs[0].Txn.LockTables, withNotifiers[i].Meta.LockTables...)

		for _, tn := range withNotifiers[i].Meta.TNShards {
			tryAppendTNShard(tn)
		}

		for _, extra := range withNotifiers[i].Extra {
			tryAppendTNShard(extra.CNRequest.Target)
		}

		if withNotifiers[i].Meta.LockService != "" {
			commitReqs[0].Txn.LockService = withNotifiers[i].Meta.LockService
		}
	}

	for i := range writes {
		commitReqs[0].CommitRequest.Payload = append(
			commitReqs[0].CommitRequest.Payload, &writes[i])
		tryAppendTNShard(writes[i].CNRequest.Target)
	}

	if len(commitReqs[0].Txn.TNShards) == 0 { // commit no write handled txn
		notifyAll(nil, nil)
		return
	}

	if commitReqs[0].Txn.TNShards[0].Address == defines.TEMPORARY_TABLE_TN_ADDR {
		fmt.Println("batch commit",
			commitReqs[0].Method.String(),
			commitReqs[0].Txn.ID)
	}

	//sent = true
	rpcRet, err := bc.sender.Send(withNotifiers[0].Ctx, commitReqs)

	notifyAll(rpcRet, err)

	return
}
