// Copyright 2021 Matrix Origin
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

package objectio

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const Magic = 0xFFFFFFFF
const Version = 1
const FSName = "local"

type Object struct {
	// name is the object file's name
	name string
	// fs is an instance of fileservice
	fs fileservice.FileService
}

func NewObject(name string, fs fileservice.FileService) *Object {
	object := &Object{
		name: name,
		fs:   fs,
	}
	return object
}

func (o *Object) GetFs() fileservice.FileService {
	return o.fs
}

type ObjectDescriber interface {
	DescribeObject() (*ObjectStats, error)
}

type ObjectStats struct {
	// 0: Data
	// 1: Tombstone
	zoneMaps   [2]ZoneMap
	blkCnt     uint32
	extent     Extent
	name       ObjectName
	sortKeyIdx uint16
}

func newObjectStats() *ObjectStats {
	description := new(ObjectStats)
	return description
}

func (des *ObjectStats) Marshal() []byte {
	var buf bytes.Buffer

	// marshal zone map
	for idx := 0; idx < len(des.zoneMaps); idx++ {
		if len(des.zoneMaps[idx]) == 0 {
			buf.Write(index.ZeroZM)
		} else {
			if data, err := des.zoneMaps[idx].Marshal(); err != nil {
				logutil.Info("[object stats]: marshal zone map failed")
				buf.Write(index.ZeroZM)
			} else {
				buf.Write(data)
			}
		}
	}

	buf.Write(types.EncodeUint32(&des.blkCnt))
	buf.Write(des.extent)
	buf.Write(des.name)
	buf.Write(types.EncodeUint16(&des.sortKeyIdx))

	return buf.Bytes()
}

func (des *ObjectStats) UnMarshal(data []byte) {
	offset := 0
	for idx := 0; idx < len(des.zoneMaps); idx++ {
		if des.zoneMaps[idx] == nil {
			des.zoneMaps[idx] = index.ZeroZM
		}
		des.zoneMaps[idx].Unmarshal(data[offset : offset+ZoneMapSize])
		offset += ZoneMapSize
	}

	des.blkCnt = types.DecodeUint32(data[offset : offset+4])
	offset += 4

	des.extent = data[offset : offset+ExtentLen]
	offset += ExtentLen

	des.name = data[offset : offset+ObjectNameLen]
	offset += ObjectNameLen

	des.sortKeyIdx = types.DecodeUint16(data[offset : offset+2])
}

// Clone deep copies the stats and returns its pointer
func (des *ObjectStats) Clone() *ObjectStats {
	copied := newObjectStats()

	copy(copied.name, des.name)
	copy(copied.extent, des.extent)

	copied.blkCnt = des.blkCnt
	copied.sortKeyIdx = des.sortKeyIdx

	copied.zoneMaps[SchemaData] = des.zoneMaps[SchemaData].Clone()
	copied.zoneMaps[SchemaTombstone] = des.zoneMaps[SchemaTombstone].Clone()

	return copied
}

func (des *ObjectStats) GetSortKeyIdx() uint16 {
	return des.sortKeyIdx
}

func (des *ObjectStats) GetOriginSize() uint32 {
	return des.extent.OriginSize()
}

func (des *ObjectStats) GetCompSize() uint32 {
	return des.extent.Length()
}

func (des *ObjectStats) GetObjLoc() Location {
	return BuildLocation(des.name, des.extent, 0, 0)
}

func (des *ObjectStats) GetBlkCnt() uint32 {
	return des.blkCnt
}

func (des *ObjectStats) GetDataSortKeyZoneMap() ZoneMap {
	return des.zoneMaps[SchemaData]
}

func (des *ObjectStats) GetTombstoneSortKeyZoneMap() ZoneMap {
	return des.zoneMaps[SchemaTombstone]
}

func (des *ObjectStats) String() string {
	return fmt.Sprintf("[object stats]: objName: %s; extent: %v; blkCnt: %d; zoneMaps: %v",
		des.name.String(), des.extent.String(), des.blkCnt, des.zoneMaps)
}
