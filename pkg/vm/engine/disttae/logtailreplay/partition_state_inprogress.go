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
	"context"
	"fmt"
	"github.com/tidwall/btree"
	"regexp"
	"runtime/trace"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

var (
	dataObjectListPattern      = regexp.MustCompile(`_\d+_data_meta`)
	tombstoneObjectListPattern = regexp.MustCompile(`_\d+_tombstone_meta`)
)

func IsMetaEntry(tblName string) bool {
	return IsDataObjectList(tblName) || IsTombstoneObjectList(tblName)
}

func IsDataObjectList(tblName string) bool {
	return dataObjectListPattern.MatchString(tblName)
}

func IsTombstoneObjectList(tblName string) bool {
	return tombstoneObjectListPattern.MatchString(tblName)
}

type PartitionStateInProgress struct {
	// also modify the Copy method if adding fields

	// data
	rows *btree.BTreeG[RowEntry] // use value type to avoid locking on elements
	//table data objects

	dataObjects     *btree.BTreeG[ObjectEntry]
	tombstoneObjets *btree.BTreeG[ObjectEntry]

	//TODO:: It's transient, should be removed in future PR.
	//blockDeltas *btree.BTreeG[BlockDeltaEntry]
	checkpoints []string
	start       types.TS
	end         types.TS

	// index
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
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

func NewPartitionStateInProgress(noData bool) *PartitionStateInProgress {
	opts := btree.Options{
		Degree: 64,
	}
	return &PartitionStateInProgress{
		noData:          noData,
		rows:            btree.NewBTreeGOptions(RowEntry.Less, opts),
		dataObjects:     btree.NewBTreeGOptions(ObjectEntry.Less, opts),
		tombstoneObjets: btree.NewBTreeGOptions(ObjectEntry.Less, opts),
		//blockDeltas:     btree.NewBTreeGOptions((BlockDeltaEntry).Less, opts),
		primaryIndex: btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		//dirtyBlocks:     btree.NewBTreeGOptions((types.Blockid).Less, opts),
		objectIndexByTS: btree.NewBTreeGOptions(ObjectIndexByTSEntry.Less, opts),
		shared:          new(sharedStates),
	}
}

func (p *PartitionStateInProgress) Copy() *PartitionStateInProgress {
	state := PartitionStateInProgress{
		rows:            p.rows.Copy(),
		dataObjects:     p.dataObjects.Copy(),
		tombstoneObjets: p.tombstoneObjets.Copy(),
		primaryIndex:    p.primaryIndex.Copy(),
		noData:          p.noData,
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

func (p *PartitionStateInProgress) RowExists(rowID types.Rowid, ts types.TS) bool {
	iter := p.rows.Iter()
	defer iter.Release()

	blockID := rowID.CloneBlockID()
	for ok := iter.Seek(RowEntry{
		BlockID: blockID,
		RowID:   rowID,
		Time:    ts,
	}); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.BlockID != blockID {
			break
		}
		if entry.RowID != rowID {
			break
		}
		if entry.Time.Greater(&ts) {
			// not visible
			continue
		}
		if entry.Deleted {
			// deleted
			return false
		}
		return true
	}

	return false
}

func (p *PartitionStateInProgress) HandleLogtailEntry(
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

func (p *PartitionStateInProgress) HandleDataObjectList(
	ctx context.Context, ee *api.Entry, fs fileservice.FileService) {
	var numDeleted, scanCnt int64
	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	stateCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[3]))
	sortedCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[4]))
	createTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[7]))
	deleteTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[8]))
	startTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[9]))
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
		{
			//Need to insert an ee in objectIndexByTS, when soft delete appendable object.
			e := ObjectIndexByTSEntry{
				ShortObjName: *objEntry.ObjectShortName(),

				IsAppendable: objEntry.EntryState,
			}
			if !deleteTSCol[idx].IsEmpty() {
				e.Time = deleteTSCol[idx]
				e.IsDelete = true
				p.objectIndexByTS.Set(e)
			}
		}

		if objEntry.EntryState && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}
		// for appendable object, gc rows when delete object
		iter := p.rows.Copy().Iter()
		objID := objEntry.ObjectStats.ObjectName().ObjectId()
		truncatePoint := startTSCol[idx]
		blkCnt := objEntry.ObjectStats.BlkCnt()

		for i := uint32(0); i < blkCnt; i++ {
			blkID := objectio.NewBlockidWithObjectID(objID, uint16(i))
			pivot := RowEntry{
				// aobj has only one blk
				RowID: *types.NewRowid(blkID, 0),
			}
			for ok := iter.Seek(pivot); ok; ok = iter.Next() {
				rowItem := iter.Item()
				if rowItem.BlockID != *blkID {
					break
				}
				scanCnt++

				// if the inserting block is appendable, need to delete the rows for it;
				// if the inserting block is non-appendable and has delta location, need to delete
				// the deletes for it.
				if objEntry.EntryState {
					if rowItem.Time.LessEq(&truncatePoint) {
						// delete the row
						p.rows.Delete(rowItem)

						// delete the row's primary index
						if len(rowItem.PrimaryIndexBytes) > 0 {
							p.primaryIndex.Delete(&PrimaryIndexEntry{
								Bytes:      rowItem.PrimaryIndexBytes,
								RowEntryID: rowItem.ID,
							})
						}
						numDeleted++
					}
				}
			}
			iter.Release()
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionStateInProgress) HandleTombstoneObjectList(
	ctx context.Context, ee *api.Entry, fs fileservice.FileService) {
	var numDeleted, scanCnt int64
	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	stateCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[3]))
	sortedCol := vector.MustFixedCol[bool](mustVectorFromProto(ee.Bat.Vecs[4]))
	createTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[7]))
	deleteTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[8]))
	startTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(ee.Bat.Vecs[9]))
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
		iter := p.rows.Copy().Iter()
		objID := objEntry.ObjectStats.ObjectName().ObjectId()
		truncatePoint := startTSCol[idx]
		blkCnt := objEntry.ObjectStats.BlkCnt()

		for i := uint32(0); i < blkCnt; i++ {
			blkID := objectio.NewBlockidWithObjectID(objID, uint16(i))
			pivot := RowEntry{
				// aobj has only one blk
				RowID: *types.NewRowid(blkID, 0),
			}
			for ok := iter.Seek(pivot); ok; ok = iter.Next() {
				rowItem := iter.Item()
				if rowItem.BlockID != *blkID {
					break
				}
				scanCnt++

				// if the inserting block is appendable, need to delete the rows for it;
				// if the inserting block is non-appendable and has delta location, need to delete
				// the deletes for it.
				if objEntry.EntryState {
					if rowItem.Time.LessEq(&truncatePoint) {
						// delete the row
						p.rows.Delete(rowItem)
						numDeleted++
					}
				}
			}
			iter.Release()
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionStateInProgress) HandleRowsInsert(
	ctx context.Context,
	inputBat *api.Batch,
	primarySeqNum int,
	packer *types.Packer) {

	var primaryKeys [][]byte

	ctx, task := trace.NewTask(ctx, "PartitionStateInProgress.HandleRowsInsert")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(inputBat.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(inputBat.Vecs[1]))

	bat, err := batch.ProtoBatchToBatch(inputBat)
	if err != nil {
		panic(err)
	}

	primaryKeys = EncodePrimaryKeyVector(
		bat.Vecs[2+primarySeqNum],
		packer,
	)

	var numInserted int64
	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := p.rows.Get(pivot)
		if !ok {
			entry = pivot
			entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			numInserted++
		}

		if !p.noData {
			entry.Batch = bat
			entry.Offset = int64(i)
		}
		entry.PrimaryIndexBytes = primaryKeys[i]
		p.rows.Set(entry)

		{
			pIdxEntry := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
			}
			p.primaryIndex.Set(pIdxEntry)
		}
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.InsertEntries.Add(1)
		c.DistTAE.Logtail.InsertRows.Add(numInserted)
		c.DistTAE.Logtail.ActiveRows.Add(numInserted)
	})

	return
}

func (p *PartitionStateInProgress) HandleRowsDeletes(
	ctx context.Context,
	inputBat *api.Batch,
	packer *types.Packer) {

	ctx, task := trace.NewTask(ctx, "PartitionStateInProgress.HandleRowsDelete")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(inputBat.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(inputBat.Vecs[1]))

	bat, err := batch.ProtoBatchToBatch(inputBat)
	if err != nil {
		panic(err)
	}

	var primaryKeys [][]byte
	if len(inputBat.Vecs) > 2 {
		// has primary key
		primaryKeys = EncodePrimaryKeyVector(
			bat.Vecs[2],
			packer,
		)
	}

	numDeletes := int64(0)
	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := p.rows.Get(pivot)
		if !ok {
			entry = pivot
			entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			numDeletes++
		}

		entry.Deleted = true
		if i < len(primaryKeys) {
			entry.PrimaryIndexBytes = primaryKeys[i]
		}
		if !p.noData {
			entry.Batch = bat
			entry.Offset = int64(i)
		}
		p.rows.Set(entry)

		// primary key
		if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
			pIdxEntry := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
			}
			p.primaryIndex.Set(pIdxEntry)
		}
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
		c.DistTAE.Logtail.DeleteRows.Add(numDeletes)
	})
}

func (p *PartitionStateInProgress) CacheCkpDuration(
	start types.TS,
	end types.TS,
	partition *Partition) {
	if partition.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.start = start
	p.end = end
}

func (p *PartitionStateInProgress) AppendCheckpoint(
	checkpoint string,
	partiton *Partition) {
	if partiton.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.checkpoints = append(p.checkpoints, checkpoint)
}

func (p *PartitionStateInProgress) consumeCheckpoints(
	fn func(checkpoint string, state *PartitionStateInProgress) error,
) error {
	for _, checkpoint := range p.checkpoints {
		if err := fn(checkpoint, p); err != nil {
			return err
		}
	}
	p.checkpoints = p.checkpoints[:0]
	return nil
}

func (p *PartitionStateInProgress) truncate(ids [2]uint64, ts types.TS) {
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

func (p *PartitionStateInProgress) LastFlushTimestamp() types.TS {
	p.shared.Lock()
	defer p.shared.Unlock()
	return p.shared.lastFlushTimestamp
}

func (p *PartitionStateInProgress) Checkpoints() []string {
	return p.checkpoints
}
