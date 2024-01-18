// Copyright 2021 - 2023 Matrix Origin
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

package common

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	None int = iota
	Done
	Range
	CNCommit
	Operator
)

type phase struct {
	txn        *transaction
	typ        int
	name       string
	start, end int64
	hit, total int64
}

type transaction struct {
	phaseDuration map[string][]phase
	ranges        []struct {
		hit, total, start, end int64
	}
	cnCommit []struct {
		start, end int64
	}
	txnId uuid.UUID
}

func newTxn() *transaction {
	s := new(transaction)
	s.phaseDuration = make(map[string][]phase)
	return s
}

type Sketchpad struct {
	sync.Mutex
	pool sync.Pool
	//TxnId     uuid.UUID
	collectCh chan phase
	//Stmts     *transaction
	ctx context.Context

	pad  sync.Map
	ants *ants.Pool
}

var InsertLogger *Sketchpad

func InitInsertLogger(ctx context.Context) *Sketchpad {
	t := new(Sketchpad)

	t.pool = sync.Pool{
		New: func() any {
			return newTxn()
		},
	}

	t.ctx = ctx
	//t.TxnId = uuid.MustParse("dd1dccb4-4d3c-41f8-b482-5251dc7a41bf")
	t.collectCh = make(chan phase, 1000*100)

	//t.Stmts = t.pool.Get().(*transaction)

	t.ants, _ = ants.NewPool(10)
	go t.collectTask()
	return t
}

func (t *Sketchpad) SetTxnId(txnId uuid.UUID) {
	_, ok := t.pad.Load(txnId)
	if ok {
		panic("re set txn id")
	}

	txn := t.pool.Get().(*transaction)
	txn.txnId = txnId
	t.pad.Store(txnId, txn)
}

func (t *Sketchpad) RecordCNCommit(txnId uuid.UUID, start, end int64) {
	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- phase{
		txn:   val.(*transaction),
		start: start,
		end:   end,
		typ:   CNCommit,
	}
}

func (t *Sketchpad) RecordRanges(txnId uuid.UUID, hit, total, start, end int64) {
	if total == 0 {
		return
	}

	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- phase{
		txn:   val.(*transaction),
		hit:   hit,
		total: total,
		start: start,
		end:   end,
		typ:   Range,
	}
}

func (t *Sketchpad) RecordPhase(name string, txnId uuid.UUID, start, end int64) {
	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- phase{
		txn:   val.(*transaction),
		name:  name,
		start: start,
		end:   end,
		typ:   Operator,
	}
}

func (t *Sketchpad) TryLog(txnId uuid.UUID, start, end int64) {
	val, ok := t.pad.Load(txnId)
	if ok {
		t.collectCh <- phase{
			txn:   val.(*transaction),
			start: start,
			end:   end,
			typ:   Done,
		}

		t.pad.Delete(txnId)
	}
}

func (txn *transaction) clear() {
	for x := range txn.phaseDuration {
		delete(txn.phaseDuration, x)
	}
	txn.ranges = txn.ranges[:0]
	txn.cnCommit = txn.cnCommit[:0]
}

func maxInt64(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func (txn *transaction) operatorMerge() {
	for name := range txn.phaseDuration {
		phs := txn.phaseDuration[name]
		sort.Slice(phs, func(i, j int) bool {
			return phs[i].start < phs[j].start
		})

		shadow := []phase{phs[0]}
		sIdx := 0
		for idx := 1; idx < len(phs); idx++ {
			if shadow[sIdx].end >= phs[idx].start {
				// has overlap
				shadow[sIdx].end = maxInt64(shadow[sIdx].end, phs[idx].end)
			} else {
				shadow = append(shadow, phs[idx])
				sIdx++
			}
		}
		txn.phaseDuration[name] = shadow
	}
}

func (txn *transaction) doLog(t *Sketchpad, txnId uuid.UUID, start, end int64) {
	defer func() {
		txn.clear()
		t.pool.Put(txn)
	}()

	if _, ok := txn.phaseDuration[" MergeS3BlocksMetaLoc "]; ok {
		return
	}

	txn.operatorMerge()

	phaseStr := ""
	for k := range txn.phaseDuration {
		phs := txn.phaseDuration[k]
		phaseStr += fmt.Sprintf("%s:", k)
		for x := range phs {
			phaseStr += fmt.Sprintf("(%d,%d)", phs[x].start, phs[x].end)
		}
		phaseStr += ";"
	}

	rangesStr := ""
	for idx := range txn.ranges {
		rangesStr += fmt.Sprintf("(%d,%d);(%d,%d)",
			txn.ranges[idx].hit, txn.ranges[idx].total,
			txn.ranges[idx].start, txn.ranges[idx].end)
	}

	cnCommitStr := ""
	for idx := range txn.cnCommit {
		cnCommitStr += fmt.Sprintf("(%d,%d)", txn.cnCommit[idx].start, txn.cnCommit[idx].end)
	}

	logutil.Info("Slow Insert",
		zap.String("txnId", txnId.String()),
		zap.String("txnLifeSpan", fmt.Sprintf("%dms(%d,%d)",
			time.Unix(0, end).Sub(time.Unix(0, start)).Milliseconds(), start, end)),
		zap.String("phase duration", phaseStr),
		zap.String("ranges", rangesStr),
		zap.String("cnCommit", cnCommitStr))
}

func (t *Sketchpad) collectTask() {
	for {
		select {
		case <-t.ctx.Done():
			return

		case ph := <-t.collectCh:

			switch ph.typ {
			case Done:
				txn := ph.txn
				t.ants.Submit(func() {
					txn.doLog(t, txn.txnId, ph.start, ph.end)
				})

			case Operator:
				if len(ph.name) > 30 {
					idx := strings.Index(ph.name, "(")
					if idx == -1 {
						idx = 30
					}
					ph.name = ph.name[:idx]
				}

				old := ph.txn.phaseDuration[ph.name]
				old = append(old, ph)
				ph.txn.phaseDuration[ph.name] = old

			case Range:
				ph.txn.ranges = append(ph.txn.ranges, struct {
					hit, total, start, end int64
				}{hit: ph.hit, total: ph.total, start: ph.start, end: ph.end})

			case CNCommit:
				ph.txn.cnCommit = append(ph.txn.cnCommit,
					struct{ start, end int64 }{start: ph.start, end: ph.end})
			}
		}
	}
}
