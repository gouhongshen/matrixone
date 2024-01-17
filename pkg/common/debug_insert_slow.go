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
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
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
	typ        int
	name       string
	start, end int64
	hit, total int
	dur        time.Duration
}

type statement struct {
	phaseDuration map[string][]phase
	ranges        []struct {
		hit, total int
		dur        time.Duration
	}
	cnCommit []struct {
		start, end int64
	}
}

func newStmt() *statement {
	s := new(statement)
	s.phaseDuration = make(map[string][]phase)
	return s
}

type Transaction struct {
	sync.Mutex
	pool      sync.Pool
	TxnId     uuid.UUID
	collectCh chan phase
	Stmts     *statement
	ctx       context.Context

	ants *ants.Pool
}

var InsertLogger *Transaction

func InitInsertLogger(ctx context.Context) *Transaction {
	t := new(Transaction)

	t.pool = sync.Pool{
		New: func() any {
			return newStmt()
		},
	}

	t.ctx = ctx
	t.TxnId = uuid.New()
	t.collectCh = make(chan phase, 1000*100)

	t.Stmts = t.pool.Get().(*statement)

	t.ants, _ = ants.NewPool(10)
	go t.collectTask()
	return t
}

func (t *Transaction) SetTxnId(txnId uuid.UUID) {
	t.TxnId = txnId
}

func (t *Transaction) RecordCNCommit(txnId uuid.UUID, start, end int64) {
	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}

	t.collectCh <- phase{
		start: start,
		end:   end,
		typ:   CNCommit,
	}
}

func (t *Transaction) RecordRanges(txnId uuid.UUID, hit, total int, dur time.Duration) {
	if total == 0 {
		return
	}

	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}

	t.collectCh <- phase{
		hit:   hit,
		total: total,
		dur:   dur,
		typ:   Range,
	}
}

func (t *Transaction) RecordPhase(name string, txnId uuid.UUID, start, end int64) {
	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}

	t.collectCh <- phase{name: name, start: start, end: end, typ: Operator}
}

func (t *Transaction) TryLog(txnId uuid.UUID, start, end int64) {
	if bytes.Equal(txnId[:], t.TxnId[:]) {
		t.collectCh <- phase{start: start, end: end, typ: Done}
	}
}

func (stmt *statement) clear() {
	for x := range stmt.phaseDuration {
		delete(stmt.phaseDuration, x)
	}
	stmt.ranges = stmt.ranges[:0]
	stmt.cnCommit = stmt.cnCommit[:0]
}

func (stmt *statement) doLog(t *Transaction, txnId uuid.UUID, start, end int64) {
	defer func() {
		stmt.clear()
		t.pool.Put(stmt)
	}()

	if _, ok := stmt.phaseDuration["MergeS3BlocksMetaLoc"]; ok {
		return
	}

	phaseStr := ""

	for k := range stmt.phaseDuration {
		phs := stmt.phaseDuration[k]
		if len(k) > 30 {
			idx := strings.Index(k, "(")
			if idx == -1 {
				idx = 30
			}
			k = k[:idx]
		}
		phaseStr += fmt.Sprintf("%s:", k)
		for x := range phs {
			phaseStr += fmt.Sprintf("(%d,%d)", phs[x].start, phs[x].end)
		}
		phaseStr += ";"
	}

	rangesStr := ""
	for idx := range stmt.ranges {
		rangesStr += fmt.Sprintf("[%d/%d=%.3f, %.3fms]; ",
			stmt.ranges[idx].hit, stmt.ranges[idx].total,
			float64(stmt.ranges[idx].hit)/float64(stmt.ranges[idx].total),
			float64(stmt.ranges[idx].dur.Nanoseconds())/(1000*1000))
	}

	cnCommitStr := ""
	for idx := range stmt.cnCommit {
		cnCommitStr += fmt.Sprintf("(%d,%d)", stmt.cnCommit[idx].start, stmt.cnCommit[idx].end)
	}

	logutil.Info("Slow Insert",
		zap.String("txnId", txnId.String()),
		zap.String("txnLifeSpan", fmt.Sprintf("(%d,%d)", start, end)),
		zap.String("phase duration", phaseStr),
		zap.String("ranges", rangesStr),
		zap.String("cnCommit", cnCommitStr))
}

func (t *Transaction) collectTask() {
	for {
		select {
		case <-t.ctx.Done():
			return

		case ph := <-t.collectCh:

			switch ph.typ {
			case Done:
				stmt := t.Stmts
				t.Stmts = t.pool.Get().(*statement)
				t.ants.Submit(func() {
					stmt.doLog(t, t.TxnId, ph.start, ph.end)
				})

			case Operator:
				old := t.Stmts.phaseDuration[ph.name]
				old = append(old, ph)
				t.Stmts.phaseDuration[ph.name] = old

			case Range:
				t.Stmts.ranges = append(t.Stmts.ranges, struct {
					hit, total int
					dur        time.Duration
				}{hit: ph.hit, total: ph.total, dur: ph.dur})

			case CNCommit:
				t.Stmts.cnCommit = append(t.Stmts.cnCommit,
					struct{ start, end int64 }{start: ph.start, end: ph.end})
			}
		}
	}
}
