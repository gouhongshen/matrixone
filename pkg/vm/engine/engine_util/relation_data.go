// Copyright 2021-2024 Matrix Origin
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

package engine_util

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.RelData = new(BlockListRelData)

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		relData := new(BlockListRelData)
		if err := relData.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return relData, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

// emptyCnt is the number of empty blocks preserved
func NewBlockListRelationData(emptyCnt int) *BlockListRelData {
	return &BlockListRelData{
		blklist: objectio.MakeBlockInfoSlice(emptyCnt),
	}
}

func NewBlockListRelationDataOfObject(
	obj *objectio.ObjectStats, withInMemory bool,
) *BlockListRelData {
	slice := objectio.ObjectStatsToBlockInfoSlice(
		obj, withInMemory,
	)
	return &BlockListRelData{
		blklist: slice,
	}
}

type BlockListRelData struct {
	// blkList[0] is a empty block info
	blklist objectio.BlockInfoSlice

	// tombstones
	tombstones engine.Tombstoner
}

func (relData *BlockListRelData) String() string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("RelData[%d]<\n", relData.GetType()))
	if relData.blklist != nil {
		w.WriteString(fmt.Sprintf("\tBlockList: %s\n", relData.blklist.String()))
	} else {
		w.WriteString("\tBlockList: nil\n")
	}
	if relData.tombstones != nil {
		w.WriteString(relData.tombstones.StringWithPrefix("\t"))
	} else {
		w.WriteString("\tTombstones: nil\n")
	}
	return w.String()
}

func (relData *BlockListRelData) GetShardIDList() []uint64 {
	panic("not supported")
}
func (relData *BlockListRelData) GetShardID(i int) uint64 {
	panic("not supported")
}
func (relData *BlockListRelData) SetShardID(i int, id uint64) {
	panic("not supported")
}
func (relData *BlockListRelData) AppendShardID(id uint64) {
	panic("not supported")
}

func (relData *BlockListRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return relData.blklist.GetAllBytes()
}

func (relData *BlockListRelData) BuildEmptyRelData(i int) engine.RelData {
	l := make([]byte, 0, objectio.BlockInfoSize*i)
	return &BlockListRelData{
		blklist: l,
	}
}

func (relData *BlockListRelData) GetBlockInfo(i int) objectio.BlockInfo {
	return *relData.blklist.Get(i)
}

func (relData *BlockListRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	relData.blklist.Set(i, blk)
}

func (relData *BlockListRelData) SetBlockList(slice objectio.BlockInfoSlice) {
	relData.blklist = slice
}

func (relData *BlockListRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	relData.blklist.AppendBlockInfo(blk)
}

func (relData *BlockListRelData) UnmarshalBinary(data []byte) (err error) {
	typ := engine.RelDataType(types.DecodeUint8(data))
	if typ != engine.RelDataBlockList {
		return moerr.NewInternalErrorNoCtxf("UnmarshalBinary RelDataBlockList with %v", typ)
	}
	data = data[1:]

	sizeofblks := types.DecodeUint32(data)
	data = data[4:]

	relData.blklist = data[:sizeofblks]
	data = data[sizeofblks:]

	tombstoneLen := types.DecodeUint32(data)
	data = data[4:]

	if tombstoneLen == 0 {
		return
	}

	relData.tombstones, err = UnmarshalTombstoneData(data[:tombstoneLen])
	return
}

func (relData *BlockListRelData) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
	typ := uint8(relData.GetType())
	if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	sizeofblks := uint32(relData.blklist.Size())
	if _, err = w.Write(types.EncodeUint32(&sizeofblks)); err != nil {
		return
	}

	// marshal blk list
	if _, err = w.Write(relData.blklist); err != nil {
		return
	}

	// marshal tombstones
	offset := w.Len()
	tombstoneLen := uint32(0)
	if _, err = w.Write(types.EncodeUint32(&tombstoneLen)); err != nil {
		return
	}
	if relData.tombstones != nil {
		if err = relData.tombstones.MarshalBinaryWithBuffer(w); err != nil {
			return
		}
		tombstoneLen = uint32(w.Len() - offset - 4)
		buf := w.Bytes()
		copy(buf[offset:], types.EncodeUint32(&tombstoneLen))
	}
	return
}

func (relData *BlockListRelData) GetType() engine.RelDataType {
	return engine.RelDataBlockList
}

func (relData *BlockListRelData) MarshalBinary() ([]byte, error) {
	var w bytes.Buffer
	if err := relData.MarshalBinaryWithBuffer(&w); err != nil {
		return nil, err
	}
	buf := w.Bytes()
	return buf, nil
}

func (relData *BlockListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	relData.tombstones = tombstones
	return nil
}

func (relData *BlockListRelData) GetTombstones() engine.Tombstoner {
	return relData.tombstones
}

func (relData *BlockListRelData) DataSlice(i, j int) engine.RelData {
	blist := objectio.BlockInfoSlice(relData.blklist.Slice(i, j))
	return &BlockListRelData{
		blklist:    blist,
		tombstones: relData.tombstones,
	}
}

func (relData *BlockListRelData) GroupByPartitionNum() map[int16]engine.RelData {
	ret := make(map[int16]engine.RelData)

	blks := relData.GetBlockInfoSlice()
	blksLen := blks.Len()
	for idx := range blksLen {
		blkInfo := blks.Get(idx)
		if blkInfo.IsMemBlk() {
			continue
		}
		partitionNum := blkInfo.PartitionNum
		if _, ok := ret[partitionNum]; !ok {
			ret[partitionNum] = &BlockListRelData{
				tombstones: relData.tombstones,
			}
			ret[partitionNum].AppendBlockInfo(&objectio.EmptyBlockInfo)
		}
		ret[partitionNum].AppendBlockInfo(blkInfo)
	}

	return ret
}

func (relData *BlockListRelData) DataCnt() int {
	return relData.blklist.Len()
}

///////////////////// ObjectListRelData ///////////////////

var _ engine.RelData = new(ObjectListRelData)

type ObjectListRelData struct {
	// In most cases, objectListRelData is a Sparse Matrix,
	// in order to save space, the matrix need to be compressed.
	objects objectio.ObjectStatsSlice

	// objIdxes[x] indicates that which object stats the x-th blk belongs to.
	objIdxes []uint32
	// blkIdxes[x] indicates the offset of the x-th blk in the stats.
	blkIdxes []uint32
}

func (o ObjectListRelData) GetType() engine.RelDataType {
	return engine.RelDataObjectList
}

func (o ObjectListRelData) String() string {
	var buf bytes.Buffer
	o.Scan(func(blk objectio.BlockInfo) bool {
		buf.WriteString(blk.String())
		buf.WriteString("; ")
		return true
	})
	return buf.String()
}

// Scan traverses blks util the `onItem` returns false
func (o ObjectListRelData) Scan(onItem func(blk objectio.BlockInfo) bool) {
	var (
		blkInfo objectio.BlockInfo
	)

	for i := range o.objIdxes {
		obj := o.objects.Get(int(o.objIdxes[i]))
		blkIdx := o.blkIdxes[i]

		obj.ConstructBlockInfoTo(uint16(blkIdx), &blkInfo)

		if !onItem(blkInfo) {
			break
		}
	}
}

func (o ObjectListRelData) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	size := uint32(len(o.objects))
	buf.Write(types.EncodeUint32(&size))
	buf.Write(o.objects)

	for _, idx := range o.objIdxes {
		buf.Write(types.EncodeUint32(&idx))
	}

	for _, idx := range o.blkIdxes {
		buf.Write(types.EncodeUint32(&idx))
	}

	return buf.Bytes(), nil
}

func (o ObjectListRelData) UnmarshalBinary(buf []byte) error {
	objSliceLen := types.DecodeUint32(buf[0:4])
	buf = buf[4:]

	o.objects.Append(buf[0:objSliceLen])
	buf = buf[objSliceLen:]

	split := len(buf) / 2
	if split*2 != len(buf) {
		panic(fmt.Sprintf("decode blk matrix err, buf has length: %d", len(buf)))
	}

	o.objIdxes = make([]uint32, 0, split/4)
	o.blkIdxes = make([]uint32, 0, split/4)
	for i := 0; i < split; i += 4 {
		o.objIdxes = append(o.objIdxes, types.DecodeUint32(buf[i:i+4]))
	}

	for i := split; i < len(buf); i += 4 {
		o.blkIdxes = append(o.blkIdxes, types.DecodeUint32(buf[i:i+4]))
	}

	return nil
}

func (o ObjectListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) GetTombstones() engine.Tombstoner {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) DataSlice(begin, end int) engine.RelData {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) GroupByPartitionNum() map[int16]engine.RelData {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) BuildEmptyRelData(preAllocSize int) engine.RelData {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) DataCnt() int {
	return len(o.blkIdxes)
}

func (o ObjectListRelData) GetShardIDList() []uint64 {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) GetShardID(i int) uint64 {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) SetShardID(i int, id uint64) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) AppendShardID(id uint64) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	//TODO implement me
	panic("implement me")
}

func (o ObjectListRelData) GetBlockInfo(i int) objectio.BlockInfo {
	var blkInfo objectio.BlockInfo

	obj := o.objects.Get(int(o.objIdxes[i]))
	obj.ConstructBlockInfoTo(uint16(i), &blkInfo)

	return blkInfo
}

func (o ObjectListRelData) indexBlkInObjectList(
	blk objectio.BlockInfo,
) (exist bool, idx int) {
	objName := blk.MetaLocation().Name().Short()

	for idx = 0; idx < o.objects.Len(); idx++ {
		if o.objects.Get(idx).ObjectName().Short().Equal(objName[:]) {
			break
		}
	}

	return idx < o.objects.Len(), idx
}

func (o ObjectListRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	exist, idx := o.indexBlkInObjectList(*blk)
	// want to add a new blkInfo? use the AppendBlockInfo instead.
	if !exist {
		panic(fmt.Sprintf("ObjectListRelData.SetBlockInfo out of range: %s, %d, %s",
			o.String(), i, blk.String()))
	}

	o.objIdxes[i] = uint32(idx)
	o.blkIdxes[i] = uint32(blk.BlockID.Sequence())
}

func (o ObjectListRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	//TODO implement me
	panic("implement me")
}
