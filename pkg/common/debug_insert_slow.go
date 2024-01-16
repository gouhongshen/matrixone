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

type phase struct {
	name   string
	dur    time.Duration
	cnt    int64
	maxDur time.Duration
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
	TxnId     uuid.UUID
	collectCh chan phase
	Stmts     statement
	ctx       context.Context
}

var InsertLogger *Transaction

func InitInsertLogger(ctx context.Context) *Transaction {
	t := new(Transaction)
	t.Stmts = *newStmt()
	t.ctx = ctx
	t.TxnId = uuid.New()
	t.collectCh = make(chan phase, 1000*100)
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

	t.Stmts.cnCommit = append(t.Stmts.cnCommit,
		struct{ dur time.Duration }{dur: dur})
}

func (t *Transaction) RecordRanges(txnId uuid.UUID, hit, total int, dur time.Duration) {
	if total == 0 {
		return
	}

	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}
	t.Stmts.ranges = append(t.Stmts.ranges, struct {
		hit, total int
		dur        time.Duration
	}{hit: hit, total: total, dur: dur})
}

func (t *Transaction) RecordPhase(name string, txnId uuid.UUID, sql string, dur time.Duration) {
	if !bytes.Equal(txnId[:], t.TxnId[:]) {
		return
	}

	t.collectCh <- phase{name: name, dur: dur}
}

func (t *Transaction) TryLog(txnId uuid.UUID, lifeSpan time.Duration) {
	if bytes.Equal(txnId[:], t.TxnId[:]) {
		t.collectCh <- phase{cnt: 0x3fff, dur: lifeSpan}
	}
}

func (t *Transaction) doLog(txnId uuid.UUID, lifeSpan time.Duration) {
	stmt := t.Stmts

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
	for idx := range t.Stmts.ranges {
		rangesStr += fmt.Sprintf("[%d/%d=%.3f, %.3fms]; ",
			t.Stmts.ranges[idx].hit, t.Stmts.ranges[idx].total,
			float64(t.Stmts.ranges[idx].hit)/float64(t.Stmts.ranges[idx].total),
			float64(t.Stmts.ranges[idx].dur.Nanoseconds())/(1000*1000))
	}

	cnCommitStr := ""
	for idx := range t.Stmts.cnCommit {
		cnCommitStr += fmt.Sprintf("[%.3fms]",
			float64(t.Stmts.cnCommit[idx].dur)/(1000*1000))
	}

	logutil.Info("Slow Insert",
		zap.String("txnId", txnId.String()),
		zap.String("txnLifeSpan", fmt.Sprintf("%.3fms", float64(lifeSpan.Nanoseconds())/(1000*1000))),
		zap.String("phase duration", phaseStr),
		zap.String("ranges", rangesStr),
		zap.String("cnCommit", cnCommitStr))

	for x := range t.Stmts.phaseDuration {
		delete(t.Stmts.phaseDuration, x)
	}
}

func (t *Transaction) collectTask() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case ph := <-t.collectCh:

			if ph.cnt == 0x3fff {
				t.doLog(t.TxnId, ph.dur)
				continue
			}

			old := t.Stmts.phaseDuration[ph.name]
			old.dur += ph.dur
			old.cnt++

			if ph.dur > old.maxDur {
				old.maxDur = ph.dur
			}

			t.Stmts.phaseDuration[ph.name] = old

		}
	}
}
