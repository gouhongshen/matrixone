// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/tidwall/btree"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
)

type PartitionState struct {
	// also modify the Copy method if adding fields

	// data
	//rows *btree.BTreeG[RowEntry] // use value type to avoid locking on elements
	//table data objects

	dataObjects     *btree.BTreeG[ObjectEntry]
	tombstoneObjets *btree.BTreeG[ObjectEntry]

	//TODO:: It's transient, should be removed in future PR.
	//blockDeltas *btree.BTreeG[BlockDeltaEntry]
	checkpoints []string
	start       types.TS
	end         types.TS

	inmemDeletes *btree.BTreeG[PrimaryIndexEntry]
	inmemInserts *btree.BTreeG[PrimaryIndexEntry]

	// index
	//primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
	//for non-appendable block's memory deletes, used to getting dirty
	// non-appendable blocks quickly.
	//TODO::remove it
	//dirtyBlocks *btree.BTreeG[types.Blockid]
	//index for objects by timestamp.
	objectIndexByTS *btree.BTreeG[ObjectIndexByTSEntry]

	// noData indicates whether to retain data batch
	// for primary key dedup, reading data is not required
	noData bool

	// some data need to be shared between all states
	// should have been in the Partition structure, but doing that requires much more codes changes
	// so just put it here.
	shared *sharedStates

	// blocks deleted before minTS is hard deleted.
	// partition state can't serve txn with snapshotTS less than minTS
	minTS types.TS
}

// sharedStates is shared among all PartitionStates
type sharedStates struct {
	sync.Mutex
	// last block flush timestamp for table
	lastFlushTimestamp types.TS
}

// RowEntry represents a version of a row
type RowEntry struct {
	BlockID types.Blockid // we need to iter by block id, so put it first to allow faster iteration
	RowID   types.Rowid
	Time    types.TS

	ID                int64 // a unique version id, for primary index building and validating
	Deleted           bool
	Batch             *batch.Batch
	Offset            int64
	PrimaryIndexBytes []byte
}

func (r RowEntry) Less(than RowEntry) bool {
	// asc
	cmp := r.BlockID.Compare(than.BlockID)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	// asc
	if r.RowID.Less(than.RowID) {
		return true
	}
	if than.RowID.Less(r.RowID) {
		return false
	}
	// desc
	if than.Time.Less(&r.Time) {
		return true
	}
	if r.Time.Less(&than.Time) {
		return false
	}
	return false
}

type BlockEntry struct {
	objectio.BlockInfo

	CreateTime types.TS
	DeleteTime types.TS
}

func (b BlockEntry) Less(than BlockEntry) bool {
	return b.BlockID.Compare(than.BlockID) < 0
}

type BlockDeltaEntry struct {
	BlockID types.Blockid

	CommitTs types.TS
	DeltaLoc objectio.ObjectLocation
}

func (b BlockDeltaEntry) Less(than BlockDeltaEntry) bool {
	return b.BlockID.Compare(than.BlockID) < 0
}

func (b BlockDeltaEntry) DeltaLocation() objectio.Location {
	return b.DeltaLoc[:]
}

type ObjectInfo struct {
	objectio.ObjectStats

	// TODO(ghs) Remove
	HasDeltaLoc bool
	EntryState  bool
	Sorted      bool
	CommitTS    types.TS
	CreateTime  types.TS
	DeleteTime  types.TS
}

func (o ObjectInfo) String() string {
	return fmt.Sprintf(
		"%s; entryState: %v; sorted: %v; commitTS: %s; createTS: %s; deleteTS: %s",
		o.ObjectStats.String(), o.EntryState, o.Sorted, o.CommitTS.ToString(),
		o.CreateTime.ToString(), o.DeleteTime.ToString())
}

func (o ObjectInfo) Location() objectio.Location {
	return o.ObjectLocation()
}

type ObjectEntry struct {
	ObjectInfo
}

func (o ObjectEntry) Less(than ObjectEntry) bool {
	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
}

func (o ObjectEntry) IsEmpty() bool {
	return o.Size() == 0
}

func (o *ObjectEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(&o.DeleteTime))
}

func (o ObjectEntry) Location() objectio.Location {
	return o.ObjectLocation()
}

func (o ObjectInfo) StatsValid() bool {
	return o.ObjectStats.Rows() != 0
}

type ObjectIndexByCreateTSEntry struct {
	ObjectInfo
}

func (o ObjectIndexByCreateTSEntry) Less(than ObjectIndexByCreateTSEntry) bool {
	//asc
	if o.CreateTime.Less(&than.CreateTime) {

		return true
	}
	if than.CreateTime.Less(&o.CreateTime) {
		return false
	}

	cmp := bytes.Compare(o.ObjectShortName()[:], than.ObjectShortName()[:])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	return false
}

func (o *ObjectIndexByCreateTSEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(&o.DeleteTime))
}

type PrimaryIndexEntry struct {
	Bytes      []byte
	RowEntryID int64

	// fields for validating
	BlockID types.Blockid
	RowID   types.Rowid
	Offset  int64
	Time    types.TS
	Batch   *batch.Batch
}

func (p PrimaryIndexEntry) Less(than PrimaryIndexEntry) bool {
	if ret := bytes.Compare(p.Bytes, than.Bytes); ret != 0 {
		return ret < 0
	}

	if ret := p.BlockID.Compare(than.BlockID); ret != 0 {
		return ret < 0
	}

	return p.RowEntryID < than.RowEntryID
}

type ObjectIndexByTSEntry struct {
	Time         types.TS // insert or delete time
	ShortObjName objectio.ObjectNameShort

	IsDelete     bool
	IsAppendable bool
}

func (b ObjectIndexByTSEntry) Less(than ObjectIndexByTSEntry) bool {
	// asc
	if b.Time.Less(&than.Time) {
		return true
	}
	if than.Time.Less(&b.Time) {
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

func NewPartitionState(noData bool) *PartitionState {
	opts := btree.Options{
		Degree: 64,
	}
	return &PartitionState{
		noData: noData,
		//rows:        btree.NewBTreeGOptions((RowEntry).Less, opts),
		dataObjects: btree.NewBTreeGOptions((ObjectEntry).Less, opts),
		//blockDeltas:     btree.NewBTreeGOptions((BlockDeltaEntry).Less, opts),
		//primaryIndex: btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		inmemDeletes: btree.NewBTreeGOptions(PrimaryIndexEntry.Less, opts),
		inmemInserts: btree.NewBTreeGOptions(PrimaryIndexEntry.Less, opts),
		//dirtyBlocks:     btree.NewBTreeGOptions((types.Blockid).Less, opts),
		objectIndexByTS: btree.NewBTreeGOptions((ObjectIndexByTSEntry).Less, opts),
		shared:          new(sharedStates),
	}
}

func (p *PartitionState) Copy() *PartitionState {
	state := PartitionState{
		//rows:        p.rows.Copy(),
		dataObjects: p.dataObjects.Copy(),
		//blockDeltas:     p.blockDeltas.Copy(),
		//primaryIndex: p.primaryIndex.Copy(),
		inmemDeletes: p.inmemDeletes.Copy(),
		inmemInserts: p.inmemInserts.Copy(),
		noData:       p.noData,
		//dirtyBlocks:     p.dirtyBlocks.Copy(),
		objectIndexByTS: p.objectIndexByTS.Copy(),
		shared:          p.shared,
		start:           p.start,
		end:             p.end,
	}
	if len(p.checkpoints) > 0 {
		state.checkpoints = make([]string, len(p.checkpoints))
		copy(state.checkpoints, p.checkpoints)
	}
	return &state
}

func (p *PartitionState) Checkpoints() []string {
	return p.checkpoints
}

func (p *PartitionState) RowExists(rowID types.Rowid, ts types.TS) bool {
	insIter := p.inmemInserts.Copy().Iter()
	delIter := p.inmemDeletes.Copy().Iter()

	defer func() {
		insIter.Release()
		delIter.Release()
	}()

	for insIter.Next() {
		insItem := insIter.Item()
		if !insItem.RowID.Equal(rowID) {
			continue
		}

		if insItem.Time.GreaterEq(&ts) {
			continue
		}

		alreadyDeleted := false

		pivot := PrimaryIndexEntry{Bytes: insItem.Bytes}
		for delIter.Seek(pivot); delIter.Next(); {
			delItem := insIter.Item()
			if !bytes.Equal(delItem.Bytes, pivot.Bytes) {
				break
			}

			if delItem.Time.Less(&insItem.Time) {
				continue
			}

			if delItem.Time.GreaterEq(&ts) {
				continue
			}

			alreadyDeleted = true
		}

		return !alreadyDeleted
	}

	return false

	//iter := p.rows.Iter()
	//defer iter.Release()
	//
	//blockID := rowID.CloneBlockID()
	//for ok := iter.Seek(RowEntry{
	//	BlockID: blockID,
	//	RowID:   rowID,
	//	Time:    ts,
	//}); ok; ok = iter.Next() {
	//	entry := iter.Item()
	//	if entry.BlockID != blockID {
	//		break
	//	}
	//	if entry.RowID != rowID {
	//		break
	//	}
	//	if entry.Time.Greater(&ts) {
	//		// not visible
	//		continue
	//	}
	//	if entry.Deleted {
	//		// deleted
	//		return false
	//	}
	//	return true
	//}

	//return false
}

func (p *PartitionState) HandleLogtailEntry(
	ctx context.Context,
	fs fileservice.FileService,
	entry *api.Entry,
	primarySeqnum int,
	packer *types.Packer,
) {
	txnTrace.GetService().ApplyLogtail(entry, 1)
	switch entry.EntryType {
	case api.Entry_Insert:
		if IsDataObjectList(entry.TableName) {
			p.HandleDataObjectList(ctx, entry, fs)
		} else if IsTombstoneObjectList(entry.TableName) {
			p.HandleTombstoneObjectList(ctx, entry, fs)
		} else {
			p.HandleRowsInsert(ctx, entry.Bat, primarySeqnum, packer)
		}

	case api.Entry_Delete:
		p.HandleRowsDeletes(ctx, entry.Bat, packer)

	default:
		logutil.Panicf("unsupported logtail entry type: %s", entry.String())
	}
}

func (p *PartitionState) HandleDataObjectList(
	ctx context.Context, ee *api.Entry, fs fileservice.FileService) {
	var numDeleted int64
	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	stateCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[3]))
	sortedCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[4]))
	createTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[7]))
	deleteTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[8]))
	//startTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[9]))
	commitTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[10]))

	for idx := 0; idx < statsVec.Length(); idx++ {
		p.shared.Lock()
		if t := commitTSCol[idx]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()
		var objEntry ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		if objEntry.Size() == 0 {
			//logutil.Infof("handle dataObjectList all pushed objects should have stats: %s", objEntry.String())
			continue
		}

		objEntry.EntryState = stateCol[idx]
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		objEntry.CommitTS = commitTSCol[idx]
		objEntry.Sorted = sortedCol[idx]

		old, exist := p.dataObjects.Get(objEntry)
		if exist {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine,
			if !old.DeleteTime.IsEmpty() {
				continue
			}
		} else {
			e := ObjectIndexByTSEntry{
				Time:         createTSCol[idx],
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     false,
				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Set(e)
		}

		//prefetch the object meta
		if err := blockio.PrefetchMeta(fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.dataObjects.Set(objEntry)

		//Need to insert an ee in objectIndexByTS, when soft delete appendable object.
		if !deleteTSCol[idx].IsEmpty() {
			e := ObjectIndexByTSEntry{
				Time:         deleteTSCol[idx],
				IsDelete:     true,
				ShortObjName: *objEntry.ObjectShortName(),
				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Set(e)
		}

		if objEntry.EntryState && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}

		// for appendable object, gc rows when delete object
		p.gcInmemInserts()
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionState) HandleTombstoneObjectList(
	ctx context.Context, ee *api.Entry, fs fileservice.FileService) {
	var numDeleted int64
	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	stateCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[3]))
	sortedCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[4]))
	createTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[7]))
	deleteTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[8]))
	//startTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[9]))
	commitTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[10]))

	for idx := 0; idx < statsVec.Length(); idx++ {
		p.shared.Lock()
		if t := commitTSCol[idx]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()
		var objEntry ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		if objEntry.Size() == 0 {
			//fmt.Printf("handle tombstoneObjectList all pushed objects should have stats: %s, deleteTS: %v\n",
			//	objEntry.String())
			//logutil.Infof("handle tombstoneObjectList all pushed objects should have stats: %s", objEntry.String())
			continue
		}

		objEntry.EntryState = stateCol[idx]
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		objEntry.CommitTS = commitTSCol[idx]
		objEntry.Sorted = sortedCol[idx]

		old, exist := p.tombstoneObjets.Get(objEntry)
		if exist {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine,
			if !old.DeleteTime.IsEmpty() {
				continue
			}
		}

		//prefetch the object meta
		if err := blockio.PrefetchMeta(fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.tombstoneObjets.Set(objEntry)

		if objEntry.EntryState && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}

		// for appendable object, gc rows when delete object
		p.gcInmemDeletes()
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

var nextRowEntryID = int64(1)

func (p *PartitionState) HandleRowsInsert(
	ctx context.Context,
	inputBat *api.Batch,
	primarySeqNum int,
	packer *types.Packer) {

	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsInsert")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(inputBat.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(inputBat.Vecs[1]))

	bat, err := batch.ProtoBatchToBatch(inputBat)
	if err != nil {
		panic(err)
	}

	var (
		ok          bool
		numInserted int64
		entry       PrimaryIndexEntry
		primaryKeys [][]byte
	)

	primaryKeys = EncodePrimaryKeyVector(bat.Vecs[2+primarySeqNum], packer)

	for i, rowID := range rowIDVector {

		blockID := rowID.CloneBlockID()
		pivot := PrimaryIndexEntry{
			Bytes:   primaryKeys[i],
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}

		if entry, ok = p.inmemInserts.Get(pivot); !ok {
			entry = pivot
			entry.RowEntryID = atomic.AddInt64(&nextRowEntryID, 1)
			numInserted++
		}

		if !p.noData {
			entry.Batch = bat
			entry.Offset = int64(i)
		}

		p.inmemInserts.Set(entry)
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.InsertEntries.Add(1)
		c.DistTAE.Logtail.InsertRows.Add(numInserted)
		c.DistTAE.Logtail.ActiveRows.Add(numInserted)
	})

	return
}

func (p *PartitionState) HandleRowsDeletes(
	ctx context.Context,
	inputBat *api.Batch,
	packer *types.Packer) {

	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsDelete")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(inputBat.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(inputBat.Vecs[1]))

	bat, err := batch.ProtoBatchToBatch(inputBat)
	if err != nil {
		panic(err)
	}

	var (
		ok          bool
		numDeleted  int64
		primaryKeys [][]byte
		entry       PrimaryIndexEntry
	)

	primaryKeys = EncodePrimaryKeyVector(bat.Vecs[2], packer)

	for i, rowID := range rowIDVector {

		blockID := rowID.CloneBlockID()
		pivot := PrimaryIndexEntry{
			Bytes:   primaryKeys[i],
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}

		if entry, ok = p.inmemDeletes.Get(pivot); !ok {
			entry = pivot
			entry.RowEntryID = atomic.AddInt64(&nextRowEntryID, 1)
			numDeleted++
		}

		if !p.noData {
			entry.Batch = bat
			entry.Offset = int64(i)
		}

		p.inmemDeletes.Set(entry)
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
		c.DistTAE.Logtail.DeleteRows.Add(numDeleted)
	})
}

func (p *PartitionState) gcInmemInserts() {
	var (
		ok     bool
		object ObjectEntry
		entry  PrimaryIndexEntry
		iter   btree.IterG[PrimaryIndexEntry]
		stats  objectio.ObjectStats
	)

	iter = p.inmemInserts.Copy().Iter()
	for iter.Next() {

		entry = iter.Item()
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(entry.BlockID.Segment(), 0))

		object, ok = p.dataObjects.Get(ObjectEntry{ObjectInfo{ObjectStats: stats}})
		if ok && !object.DeleteTime.IsEmpty() {
			p.inmemInserts.Delete(entry)
		}
	}

	iter.Release()
}

func (p *PartitionState) gcInmemDeletes() {
	var (
		ok     bool
		object ObjectEntry
		entry  PrimaryIndexEntry
		iter   btree.IterG[PrimaryIndexEntry]
		stats  objectio.ObjectStats
	)

	iter = p.inmemDeletes.Copy().Iter()
	for iter.Next() {

		entry = iter.Item()
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(entry.BlockID.Segment(), 0))

		object, ok = p.tombstoneObjets.Get(ObjectEntry{ObjectInfo{ObjectStats: stats}})
		if ok && !object.DeleteTime.IsEmpty() {
			p.inmemDeletes.Delete(entry)
		}
	}

	iter.Release()
}

func (p *PartitionState) CacheCkpDuration(
	start types.TS,
	end types.TS,
	partition *Partition) {
	if partition.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.start = start
	p.end = end
}

func (p *PartitionState) AppendCheckpoint(
	checkpoint string,
	partiton *Partition) {
	if partiton.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.checkpoints = append(p.checkpoints, checkpoint)
}

func (p *PartitionState) consumeCheckpoints(
	fn func(checkpoint string, state *PartitionState) error,
) error {
	for _, checkpoint := range p.checkpoints {
		if err := fn(checkpoint, p); err != nil {
			return err
		}
	}
	p.checkpoints = p.checkpoints[:0]
	return nil
}

func (p *PartitionState) truncate(ids [2]uint64, ts types.TS) {
	if p.minTS.Greater(&ts) {
		logutil.Errorf("logic error: current minTS %v, incoming ts %v", p.minTS.ToString(), ts.ToString())
		return
	}
	p.minTS = ts
	gced := false
	pivot := ObjectIndexByTSEntry{
		Time:         ts.Next(),
		ShortObjName: objectio.ObjectNameShort{},
		IsDelete:     true,
	}
	iter := p.objectIndexByTS.Copy().Iter()
	ok := iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	objIDsToDelete := make(map[objectio.ObjectNameShort]struct{}, 0)
	objectsToDelete := ""
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.Greater(&ts) {
			continue
		}
		if entry.IsDelete {
			objIDsToDelete[entry.ShortObjName] = struct{}{}
			if gced {
				objectsToDelete = fmt.Sprintf("%s, %v", objectsToDelete, entry.ShortObjName)
			} else {
				objectsToDelete = fmt.Sprintf("%s%v", objectsToDelete, entry.ShortObjName)
			}
			gced = true
		}
	}
	iter = p.objectIndexByTS.Copy().Iter()
	ok = iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.Greater(&ts) {
			continue
		}
		if _, ok := objIDsToDelete[entry.ShortObjName]; ok {
			p.objectIndexByTS.Delete(entry)
		}
	}
	if gced {
		logutil.Infof("GC partition_state at %v for table %d:%s", ts.ToString(), ids[1], objectsToDelete)
	}

	objsToDelete := ""
	objIter := p.dataObjects.Copy().Iter()
	objGced := false
	firstCalled := false
	for {
		if !firstCalled {
			if !objIter.First() {
				break
			}
			firstCalled = true
		} else {
			if !objIter.Next() {
				break
			}
		}

		objEntry := objIter.Item()

		if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LessEq(&ts) {
			p.dataObjects.Delete(objEntry)
			//p.dataObjectsByCreateTS.Delete(ObjectIndexByCreateTSEntry{
			//	//CreateTime:   objEntry.CreateTime,
			//	//ShortObjName: objEntry.ShortObjName,
			//	ObjectInfo: objEntry.ObjectInfo,
			//})
			if objGced {
				objsToDelete = fmt.Sprintf("%s, %s", objsToDelete, objEntry.Location().Name().String())
			} else {
				objsToDelete = fmt.Sprintf("%s%s", objsToDelete, objEntry.Location().Name().String())
			}
			objGced = true
		}
	}
	if objGced {
		logutil.Infof("GC partition_state at %v for table %d:%s", ts.ToString(), ids[1], objsToDelete)
	}
}

func (p *PartitionState) LastFlushTimestamp() types.TS {
	p.shared.Lock()
	defer p.shared.Unlock()
	return p.shared.lastFlushTimestamp
}
