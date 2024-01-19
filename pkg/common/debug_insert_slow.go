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
	Reader
)

type records struct {
	txn        *transaction
	typ        int
	name       string
	bytes      int
	start, end int64
	hit, total int64
}

type transaction struct {
	phaseDuration map[string][]records
	ranges        []records
	cnCommit      []records
	reader        map[string][]records

	txnId uuid.UUID
}

func newTxn() *transaction {
	s := new(transaction)
	s.phaseDuration = make(map[string][]records)
	s.reader = make(map[string][]records)
	return s
}

func formatPrecision(duration time.Time) int64 {
	return duration.UnixNano() / 1000000
}

// return the ms between start and end
func formattedDistance(start, end int64) int64 {
	return end - start
}

type Sketchpad struct {
	sync.Mutex
	pool sync.Pool
	//TxnId     uuid.UUID
	collectCh chan records
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
	t.collectCh = make(chan records, 1000*100)

	//t.Stmts = t.pool.Get().(*transaction)

	t.ants, _ = ants.NewPool(10)
	go t.collectTask()
	return t
}

func (t *Sketchpad) SetTxnId(txnId uuid.UUID) {
	_, ok := t.pad.Load(txnId)
	if ok {
		return
	}

	txn := t.pool.Get().(*transaction)
	txn.txnId = txnId
	t.pad.Store(txnId, txn)
}

func (t *Sketchpad) RecordReader(txnId uuid.UUID, name string, bytes int, start, end time.Time) {
	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- records{
		txn:   val.(*transaction),
		start: formatPrecision(start),
		end:   formatPrecision(end),
		bytes: bytes,
		name:  name,
		typ:   Reader,
	}
}

func (t *Sketchpad) RecordCNCommit(txnId uuid.UUID, start, end time.Time) {
	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- records{
		txn:   val.(*transaction),
		start: formatPrecision(start),
		end:   formatPrecision(end),
		typ:   CNCommit,
	}
}

func (t *Sketchpad) RecordRanges(txnId uuid.UUID, hit, total int64, start, end time.Time) {
	if total == 0 {
		return
	}

	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- records{
		txn:   val.(*transaction),
		hit:   hit,
		total: total,
		start: formatPrecision(start),
		end:   formatPrecision(end),
		typ:   Range,
	}
}

func (t *Sketchpad) RecordPhase(name string, txnId uuid.UUID, start, end time.Time) {
	val, ok := t.pad.Load(txnId)
	if !ok {
		return
	}

	t.collectCh <- records{
		txn:   val.(*transaction),
		name:  name,
		start: formatPrecision(start),
		end:   formatPrecision(end),
		typ:   Operator,
	}
}

func (t *Sketchpad) TryLog(txnId uuid.UUID, start, end time.Time) {
	val, ok := t.pad.Load(txnId)
	if ok {
		// the short txn no need to record
		if end.Sub(start).Milliseconds() < 1000 {
			t.pad.Delete(txnId)
			val.(*transaction).clear()
			t.pool.Put(val)
			return
		}

		t.collectCh <- records{
			txn:   val.(*transaction),
			start: formatPrecision(start),
			end:   formatPrecision(end),
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

func (txn *transaction) mergeHelper(phs []records) []records {
	if len(phs) == 0 {
		return phs
	}

	sort.Slice(phs, func(i, j int) bool {
		return phs[i].start < phs[j].start
	})

	shadow := []records{phs[0]}
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

	return shadow
}

func (txn *transaction) operatorMerge() {
	for name := range txn.phaseDuration {
		phs := txn.phaseDuration[name]

		txn.phaseDuration[name] = txn.mergeHelper(phs)
	}
}

func (txn *transaction) readerMerge() {
	for name := range txn.reader {
		phs := txn.reader[name]

		txn.reader[name] = txn.mergeHelper(phs)
	}
}

func (txn *transaction) doLog(t *Sketchpad, txnId uuid.UUID, start, end int64) {
	defer func() {
		txn.clear()
		t.pool.Put(txn)
	}()

	if _, ok := txn.phaseDuration[" MergeS3BlocksMetaLoc "]; ok {
		// skip merge...
		return
	}

	txn.operatorMerge()

	phaseStr := ""
	for k := range txn.phaseDuration {
		phs := txn.phaseDuration[k]
		phaseStr += fmt.Sprintf("%s:", k)

		// to this format to shorten the log length
		// origin: (a1,b1)(a2,b2)(a3,b3)...
		// final: (a1,b1-a1)(a2-a1,b2-a2)(a3-a1,b3-a3)
		offset := int64(0)
		for x := 0; x < len(phs); x++ {
			if x != 0 && formattedDistance(phs[x].start, phs[x].end) <= 0 {
				continue
			}

			if x != 0 {
				offset = phs[0].start
			}

			phaseStr += fmt.Sprintf("(%d,%d)", phs[x].start-offset, phs[x].end-phs[x].start)
		}
		phaseStr += ";"
	}

	rangesStr := ""
	for idx := range txn.ranges {
		rangesStr += fmt.Sprintf("(%d,%d);(%d,%d)",
			txn.ranges[idx].hit, txn.ranges[idx].total,
			txn.ranges[idx].start, txn.ranges[idx].end-txn.ranges[idx].start)
	}

	cnCommitStr := ""
	for idx := range txn.cnCommit {
		cnCommitStr += fmt.Sprintf("(%d,%d)",
			txn.cnCommit[idx].start, txn.cnCommit[idx].end-txn.cnCommit[idx].start)
	}

	txn.readerMerge()
	readerStr := ""
	for k := range txn.reader {
		phs := txn.reader[k]

		totalBytes := 0
		tmpStr := ""
		offset := int64(0)
		for x := 0; x < len(phs); x++ {
			if x != 0 && formattedDistance(phs[x].start, phs[x].end) <= 0 {
				continue
			}

			if x != 0 {
				offset = phs[0].start
			}
			totalBytes += phs[x].bytes
			tmpStr += fmt.Sprintf("(%d,%d)", phs[x].start-offset, phs[x].end-phs[x].start)
		}
		readerStr += fmt.Sprintf("%s(%.3fmb):", k, float64(totalBytes)/(1024*1024)) + tmpStr + ";"
	}

	logutil.Info("Slow Insert",
		zap.String("txnId", txnId.String()),
		zap.String("txnLifeSpan", fmt.Sprintf("%dms(%d,%d)",
			formattedDistance(start, end), start, end-start)),
		zap.String("operator duration", phaseStr),
		zap.String("ranges", rangesStr),
		zap.String("cnCommit", cnCommitStr),
		zap.String("reader", readerStr))
}

func (t *Sketchpad) operatorFilter(name string) bool {
	// values scan is very fast, no need to collect them
	if strings.Contains(name, "value_scan") {
		return true
	}

	return false
}

func (t *Sketchpad) adjustOpName(name string) string {
	if idx := strings.Index(name, "("); idx != -1 {
		name = name[:idx]
	}

	if len(name) > 30 {
		name = name[:30]
	}

	return name
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
				if t.operatorFilter(ph.name) {
					continue
				}

				ph.name = t.adjustOpName(ph.name)

				old := ph.txn.phaseDuration[ph.name]
				old = append(old, ph)
				ph.txn.phaseDuration[ph.name] = old

			case Range:
				ph.txn.ranges = append(ph.txn.ranges, ph)

			case CNCommit:
				ph.txn.cnCommit = append(ph.txn.cnCommit, ph)

			case Reader:
				old := ph.txn.reader[ph.name]
				old = append(old, ph)
				ph.txn.reader[ph.name] = old
			}
		}
	}
}
