// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type ObjectInfo struct {
	objectio.ObjectStats
	CreateTime types.TS
	DeleteTime types.TS
}

func (o ObjectInfo) String() string {
	return fmt.Sprintf(
		"%s; appendable: %v; sorted: %v; createTS: %s; deleteTS: %s",
		o.ObjectStats.String(),
		o.ObjectStats.GetAppendable(),
		o.ObjectStats.GetSorted(),
		o.CreateTime.ToString(),
		o.DeleteTime.ToString())
}

func (o ObjectInfo) Location() objectio.Location {
	return o.ObjectLocation()
}

type ObjectEntry struct {
	ObjectInfo
}

func (o ObjectEntry) ObjectNameIndexLess(than ObjectEntry) bool {
	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
}

// ObjectDTSIndexLess has the order:
// 1. if the delete time is empty, let it be the max ts
// 2. ascending object with delete ts.
// 3. ascending object with name when same dts.
//
// sort by DELETE time and name
func (o ObjectEntry) ObjectDTSIndexLess(than ObjectEntry) bool {
	// (c, d), (c, d), (c, d), (c, inf), (c, inf) ...
	x, y := o.DeleteTime, than.DeleteTime
	if x.IsEmpty() {
		x = types.MaxTs()
	}
	if y.IsEmpty() {
		y = types.MaxTs()
	}

	if !x.Equal(&y) {
		return x.LT(&y)
	}

	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
}

func (o ObjectEntry) IsEmpty() bool {
	return o.Size() == 0
}

func (o ObjectEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LE(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.LT(&o.DeleteTime))
}

func (o ObjectEntry) Location() objectio.Location {
	return o.ObjectLocation()
}

func (o ObjectInfo) StatsValid() bool {
	return o.ObjectStats.Rows() != 0
}

// sharedStates is shared among all PartitionStates
type sharedStates struct {
	sync.Mutex
	// last block flush timestamp for table
	lastFlushTimestamp types.TS
}

type BaseEntry struct {
	EntryID int64
	RowID   objectio.Rowid
	Time    types.TS
	Deleted bool
	Offset  int64 // row offset in raw data of this entry
	RawData *batch.Batch
}

func (be BaseEntry) Less(other BaseEntry) bool {
	// asc
	if ret := be.RowID.Compare(&other.RowID); ret != 0 {
		return ret < 0
	}

	// desc
	return be.Time.Compare(&other.Time) > 0
}

func (pie PrimaryIndexEntry) RowEntryLess(than PrimaryIndexEntry) bool {
	return pie.BaseEntry.Less(than.BaseEntry)
}

type PrimaryIndexEntry struct {
	BaseEntry
	PrimaryKeyBytes []byte
}

func (pie PrimaryIndexEntry) String() string {
	pk, _ := types.Unpack(pie.PrimaryKeyBytes)
	return fmt.Sprintf("RID(%s)-EID(%d)-TS(%s)-DEL(%v)-PK(%s)\n",
		pie.RowID.String(),
		pie.EntryID,
		pie.Time.ToString(),
		pie.Deleted,
		pk.SQLStrings(nil),
	)
}

func (pie PrimaryIndexEntry) Less(than PrimaryIndexEntry) bool {
	if res := bytes.Compare(pie.PrimaryKeyBytes, than.PrimaryKeyBytes); res < 0 {
		return true
	} else if res > 0 {
		return false
	}
	return pie.EntryID < than.EntryID
}

type ObjectIndexByTSEntry struct {
	Time         types.TS // insert or delete time
	ShortObjName objectio.ObjectNameShort

	IsDelete     bool
	IsAppendable bool
}

func (b ObjectIndexByTSEntry) Less(than ObjectIndexByTSEntry) bool {
	// asc
	if b.Time.LT(&than.Time) {
		return true
	}
	if than.Time.LT(&b.Time) {
		return false
	}

	cmp := bytes.Compare(b.ShortObjName[:], than.ShortObjName[:])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}

	return false
}

var nextRowEntryID = int64(1)
