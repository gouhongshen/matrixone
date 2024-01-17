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
	typ        int
	name       string
	dur        time.Duration
	cnt        int64
	maxDur     time.Duration
	hit, total int
}

type statement struct {
	phaseDuration map[string]phase
	ranges        []struct {
		hit, total int
		dur        time.Duration
	}
	cnCommit []struct {
		dur time.Duration
	}
}

func newStmt() *statement {
	s := new(statement)
	s.phaseDuration = make(map[string]phase)
	return s
}

type Transaction struct {
	sync.Mutex
	pool      sync.Pool
	TxnId     uuid.UUID
	collectCh chan phase
	Stmts     *statement
	ctx       context.Context
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

	go t.collectTask()
	return t
}

func (t *Transaction) SetTxnId(txnId uuid.UUID) {
	t.TxnId = txnId
}

func (t *Transaction) RecordCNCommit(txnId uuid.UUID, dur time.Duration) {
	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}

	t.collectCh <- phase{
		dur: dur,
		typ: CNCommit,
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

func (t *Transaction) RecordPhase(name string, txnId uuid.UUID, sql string, dur time.Duration) {
	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}

	t.collectCh <- phase{name: name, dur: dur, typ: Operator}
}

func (t *Transaction) TryLog(txnId uuid.UUID, lifeSpan time.Duration) {
	if bytes.Equal(txnId[:], t.TxnId[:]) {
		t.collectCh <- phase{dur: lifeSpan, typ: Done}
	}
}

func (stmt *statement) clear() {
	for x := range stmt.phaseDuration {
		delete(stmt.phaseDuration, x)
	}
	stmt.ranges = stmt.ranges[:0]
	stmt.cnCommit = stmt.cnCommit[:0]
}

func (stmt *statement) doLog(t *Transaction, txnId uuid.UUID, lifeSpan time.Duration) {

	names := make([]string, len(stmt.phaseDuration))

	idx := 0
	for k := range stmt.phaseDuration {
		if strings.Contains(k, "projection") {
			k = "projection"
		}
		names[idx] = k
		idx++
	}

	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	phaseStr := ""

	for idx := range names {
		k := names[idx]
		p := stmt.phaseDuration[k]
		phaseStr += fmt.Sprintf("[%s(%5d), total-%.3fms, max-%.3fms]; ",
			k, p.cnt, float64(p.dur.Nanoseconds())/(1000*1000), float64(p.maxDur.Nanoseconds())/(1000*1000))
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
		cnCommitStr += fmt.Sprintf("[%.3fms]",
			float64(stmt.cnCommit[idx].dur)/(1000*1000))
	}

	logutil.Info("Slow Insert",
		zap.String("txnId", txnId.String()),
		zap.String("txnLifeSpan", fmt.Sprintf("%.3fms", float64(lifeSpan.Nanoseconds())/(1000*1000))),
		zap.String("phase duration", phaseStr),
		zap.String("ranges", rangesStr),
		zap.String("cnCommit", cnCommitStr))

	stmt.clear()
	t.pool.Put(stmt)

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
				go stmt.doLog(t, t.TxnId, ph.dur)

			case Operator:
				old := t.Stmts.phaseDuration[ph.name]
				old.dur += ph.dur
				old.cnt++

				if ph.dur > old.maxDur {
					old.maxDur = ph.dur
				}

				t.Stmts.phaseDuration[ph.name] = old

			case Range:
				t.Stmts.ranges = append(t.Stmts.ranges, struct {
					hit, total int
					dur        time.Duration
				}{hit: ph.hit, total: ph.total, dur: ph.dur})

			case CNCommit:
				t.Stmts.cnCommit = append(t.Stmts.cnCommit,
					struct{ dur time.Duration }{dur: ph.dur})
			}
		}
	}
}
