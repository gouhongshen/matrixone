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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/tidwall/btree"
)

type ObjectsIter interface {
	Next() bool
	Close() error
	Entry() ObjectEntry
}

type objectsIter struct {
	ts                   types.TS
	iterByMin, iterByMax btree.IterG[ObjectEntry]
	iter                 *btree.IterG[ObjectEntry]
	firstCalled          bool
	canFast              bool
	TableName            string
}

// not accurate!  only used by stats
func (p *PartitionState) ApproxObjectsNum() int {
	return p.dataObjects.Len()
}

func (p *PartitionState) NewObjectsIter(ts types.TS, useSortKeyIndex bool) (*objectsIter, error) {
	if ts.Less(&p.minTS) {
		return nil, moerr.NewTxnStaleNoCtx()
	}
	ret := &objectsIter{
		ts: ts,
	}

	if p.dataObjectsSortKeyIndexByMin != nil && useSortKeyIndex {
		ret.iterByMin = p.dataObjectsSortKeyIndexByMin.Copy().Iter()
		ret.iterByMax = p.dataObjectsSortKeyIndexByMax.Copy().Iter()
		ret.canFast = true
		ret.iter = &ret.iterByMax

		//if strings.Contains(p.TableName, "bmsql") {
		//	p.dataObjectsSortKeyIndexByMin.Scan(func(item ObjectEntry) bool {
		//		fmt.Println(p.TableName, "min", item.SortKeyZoneMap().String())
		//		return true
		//	})
		//
		//	p.dataObjectsSortKeyIndexByMax.Scan(func(item ObjectEntry) bool {
		//		fmt.Println(p.TableName, "max", item.SortKeyZoneMap().String())
		//		return true
		//	})
		//	fmt.Println()
		//}

	} else {
		tmpI := p.dataObjects.Copy().Iter()
		ret.iter = &tmpI
		ret.canFast = false
	}
	ret.TableName = p.TableName

	return ret, nil
}

var _ ObjectsIter = new(objectsIter)

func (b *objectsIter) Seek(op func(t types.T) (pivot objectio.ZoneMap, stop func(stats objectio.ObjectStats) bool)) (
	stop func(stats objectio.ObjectStats) bool, ok bool) {

	var item ObjectEntry
	if b.Next() {
		item = b.Entry()
	} else {
		return nil, false
	}

	if op == nil || !b.canFast {
		return nil, true
	}

	zm, _ := op(item.SortKeyZoneMap().GetType())

	if !zm.Valid() {
		return nil, true
	}

	piovt := ObjectEntry{}
	objectio.SetObjectStatsSortKeyZoneMap(&piovt.ObjectStats, zm)

	if ok = b.iterByMax.Seek(piovt); !ok {
		return nil, false
	}
	r := b.iterByMax.Item()
	//fmt.Println("j", r.SortKeyZoneMap())

	b.iterByMin.Seek(piovt)
	minest := b.iterByMin.Item()

	//fmt.Println("x", minest.SortKeyZoneMap().String())

	if bytes.Equal(minest.ObjectName(), r.ObjectName()) {
		return nil, true
	}

	for r1, r2 := minest.SortKeyZoneMap().Intersect(zm); r1 && r2; {
		b.iterByMin.Next()
		minest = b.iterByMin.Item()
	}

	//fmt.Println("y", minest.SortKeyZoneMap().String())

	stop = func(stats objectio.ObjectStats) bool {
		return bytes.Equal(stats.ObjectName(), minest.ObjectName())
	}

	//x1 := b.iterByMin.Item()
	//x2 := b.iterByMax.Item()

	return stop, true
}

func (b *objectsIter) Next() bool {
	for b.iter.Next() {
		item := b.iter.Item()
		if !item.Visible(b.ts) {
			// not visible
			continue
		}
		return true
	}
	return false
}

func (b *objectsIter) Prev() bool {
	for b.iter.Prev() {
		item := b.iter.Item()
		if !item.Visible(b.ts) {
			continue
		}
		return true
	}
	return false
}

func (b *objectsIter) Entry() ObjectEntry {
	return ObjectEntry{
		ObjectInfo: b.iter.Item().ObjectInfo,
	}
}

func (b *objectsIter) Close() error {
	if b.canFast {
		b.iterByMin.Release()
		b.iterByMax.Release()
	} else {
		b.iter.Release()
	}
	return nil
}

type BlocksIter interface {
	Next() bool
	Close() error
	Entry() types.Blockid
}

type dirtyBlocksIter struct {
	iter        btree.IterG[types.Blockid]
	firstCalled bool
}

func (p *PartitionState) NewDirtyBlocksIter() *dirtyBlocksIter {
	iter := p.dirtyBlocks.Copy().Iter()
	ret := &dirtyBlocksIter{
		iter: iter,
	}
	return ret
}

var _ BlocksIter = new(dirtyBlocksIter)

func (b *dirtyBlocksIter) Next() bool {
	if !b.firstCalled {
		if !b.iter.First() {
			return false
		}
		b.firstCalled = true
		return true
	}
	return b.iter.Next()
}

func (b *dirtyBlocksIter) Entry() types.Blockid {
	return b.iter.Item()
}

func (b *dirtyBlocksIter) Close() error {
	b.iter.Release()
	return nil
}

// GetChangedObjsBetween get changed objects between [begin, end],
// notice that if an object is created after begin and deleted before end, it will be ignored.
func (p *PartitionState) GetChangedObjsBetween(
	begin types.TS,
	end types.TS,
) (
	deleted map[objectio.ObjectNameShort]struct{},
	inserted map[objectio.ObjectNameShort]struct{},
) {
	inserted = make(map[objectio.ObjectNameShort]struct{})
	deleted = make(map[objectio.ObjectNameShort]struct{})

	iter := p.objectIndexByTS.Copy().Iter()
	defer iter.Release()

	for ok := iter.Seek(ObjectIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.Greater(&end) {
			break
		}

		if entry.IsDelete {
			// if the object is inserted and deleted between [begin, end], it will be ignored.
			if _, ok := inserted[entry.ShortObjName]; !ok {
				deleted[entry.ShortObjName] = struct{}{}
			} else {
				delete(inserted, entry.ShortObjName)
			}
		} else {
			inserted[entry.ShortObjName] = struct{}{}
		}

	}
	return
}

func (p *PartitionState) GetBockDeltaLoc(bid types.Blockid) (objectio.ObjectLocation, types.TS, bool) {
	iter := p.blockDeltas.Copy().Iter()
	defer iter.Release()

	pivot := BlockDeltaEntry{
		BlockID: bid,
	}
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if e.BlockID.Compare(bid) == 0 {
			return e.DeltaLoc, e.CommitTs, true
		}
	}
	return objectio.ObjectLocation{}, types.TS{}, false
}

func (p *PartitionState) BlockPersisted(blockID types.Blockid) bool {
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(&blockID))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(&blockID)[:]) {
			return true
		}
	}
	return false
}

func (p *PartitionState) GetObject(name objectio.ObjectNameShort) (ObjectInfo, bool) {
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, &name)
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], name[:]) {
			return iter.Item().ObjectInfo, true
		}
	}
	return ObjectInfo{}, false
}
