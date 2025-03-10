// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"
	"fmt"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"

	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	skippedTbl = map[uint64]bool{
		pkgcatalog.MO_DATABASE_ID: true,
		pkgcatalog.MO_TABLES_ID:   true,
		pkgcatalog.MO_COLUMNS_ID:  true,
	}
)

type WalReplayer struct {
	db            *DB
	maxTs         types.TS
	once          sync.Once
	fromTS        types.TS
	wg            sync.WaitGroup
	applyDuration time.Duration
	txnCmdChan    chan *txnbase.TxnCmd
	readCount     int
	applyCount    int
	maxLSN        uint64

	lsn            uint64
	enableLSNCheck bool
}

func newWalReplayer(
	db *DB,
	fromTS types.TS,
	lsn uint64,
	enableLSNCheck bool,
) *WalReplayer {
	replayer := &WalReplayer{
		db:     db,
		fromTS: fromTS,
		lsn:    lsn,
		// for ckp version less than 7, lsn is always 0 and lsnCheck is disable
		enableLSNCheck: enableLSNCheck,
		txnCmdChan:     make(chan *txnbase.TxnCmd, 100),
	}
	replayer.OnTimeStamp(fromTS)
	return replayer
}

func (replayer *WalReplayer) PreReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) (err error) {
		if entry.GetTable().IsVirtual() {
			return moerr.GetOkStopCurrRecur()
		}
		dropCommit, obj := entry.TreeMaxDropCommitEntry()
		if dropCommit != nil && dropCommit.DeleteBeforeLocked(replayer.fromTS) {
			return moerr.GetOkStopCurrRecur()
		}
		if obj != nil && obj.DeleteBefore(replayer.fromTS) {
			return moerr.GetOkStopCurrRecur()
		}
		entry.InitData(replayer.db.Catalog.DataFactory)
		return
	}
	if err := replayer.db.Catalog.RecurLoop(processor); err != nil {
		if !moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			panic(err)
		}
	}
}

func (replayer *WalReplayer) postReplayWal() error {
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) (err error) {
		if skippedTbl[entry.GetTable().ID] {
			return nil
		}
		if entry.IsAppendable() && entry.HasDropCommitted() {
			err = entry.GetObjectData().TryUpgrade()
		}
		return
	}
	return replayer.db.Catalog.RecurLoop(processor)
}
func (replayer *WalReplayer) Replay(ctx context.Context) (err error) {
	replayer.wg.Add(1)
	go replayer.applyTxnCmds()
	if err = replayer.db.Wal.Replay(
		ctx,
		replayer.OnReplayEntry,
		func() driver.ReplayMode { return driver.ReplayMode_ReplayForWrite },
		nil,
	); err != nil {
		return
	}
	replayer.txnCmdChan <- txnbase.NewLastTxnCmd()
	close(replayer.txnCmdChan)
	replayer.wg.Wait()
	if err = replayer.postReplayWal(); err != nil {
		return
	}
	logutil.Info(
		"Wal-Replay-Trace-End",
		zap.Duration("apply-cost", replayer.applyDuration),
		zap.Int("read-count", replayer.readCount),
		zap.Int("apply-count", replayer.applyCount),
		zap.Uint64("max-lsn", replayer.maxLSN),
	)
	return
}

func (replayer *WalReplayer) OnReplayEntry(group uint32, lsn uint64, payload []byte, typ uint16, info any) driver.ReplayEntryState {
	replayer.once.Do(replayer.PreReplayWal)
	if group != wal.GroupPrepare && group != wal.GroupC {
		return driver.RE_Internal
	}
	if !replayer.checkLSN(lsn) {
		return driver.RE_Truncate
	}
	if lsn > replayer.maxLSN {
		replayer.maxLSN = lsn
	}
	head := objectio.DecodeIOEntryHeader(payload)
	if head.Version < txnbase.IOET_WALTxnEntry_V4 {
		return driver.RE_Nomal
	}
	codec := objectio.GetIOEntryCodec(*head)
	entry, err := codec.Decode(payload[4:])
	txnCmd := entry.(*txnbase.TxnCmd)
	txnCmd.Lsn = lsn
	if err != nil {
		panic(err)
	}
	replayer.txnCmdChan <- txnCmd
	return driver.RE_Nomal
}
func (replayer *WalReplayer) applyTxnCmds() {
	defer replayer.wg.Done()
	for {
		txnCmd := <-replayer.txnCmdChan
		if txnCmd.IsLastCmd() {
			break
		}
		t0 := time.Now()
		replayer.OnReplayTxn(txnCmd, txnCmd.Lsn)
		txnCmd.Close()
		replayer.applyDuration += time.Since(t0)

	}
}
func (replayer *WalReplayer) GetMaxTS() types.TS {
	return replayer.maxTs
}

func (replayer *WalReplayer) OnTimeStamp(ts types.TS) {
	if ts.GT(&replayer.maxTs) {
		replayer.maxTs = ts
	}
}
func (replayer *WalReplayer) checkLSN(lsn uint64) (needReplay bool) {
	if !replayer.enableLSNCheck {
		return true
	}
	if lsn <= replayer.lsn {
		return false
	}
	if lsn == replayer.lsn+1 {
		replayer.lsn++
		return true
	}
	panic(fmt.Sprintf("invalid lsn %d, current lsn %d", lsn, replayer.lsn))
}
func (replayer *WalReplayer) OnReplayTxn(cmd txnif.TxnCmd, lsn uint64) {
	var err error
	replayer.readCount++
	txnCmd := cmd.(*txnbase.TxnCmd)
	// If WAL entry splits, they share same prepareTS
	if txnCmd.PrepareTS.LT(&replayer.maxTs) {
		return
	}
	replayer.applyCount++
	txn := txnimpl.MakeReplayTxn(
		replayer.db.Runtime.Options.Ctx,
		replayer.db.TxnMgr,
		txnCmd.TxnCtx,
		lsn,
		txnCmd,
		replayer,
		replayer.db.Catalog,
		replayer.db.Wal,
	)
	if err = replayer.db.TxnMgr.OnReplayTxn(txn); err != nil {
		panic(err)
	}
	if txn.Is2PC() {
		if _, err = txn.Prepare(replayer.db.Opts.Ctx); err != nil {
			panic(err)
		}
	} else {
		if err = txn.Commit(replayer.db.Opts.Ctx); err != nil {
			panic(err)
		}
	}
}
