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

package inspectcn

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

var UpdateLogTail func(
	ctx context.Context, tbl engine.Relation) (p *logtailreplay.PartitionState, err error)

func getPartitionState(
	ctx context.Context,
	tbl engine.Relation) (*logtailreplay.PartitionState, error) {

	p, err := UpdateLogTail(ctx, tbl)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func DebugPStateTable(ctx context.Context, db engine.Database, tbl engine.Relation, ts types.TS) (string, error) {
	state, err := getPartitionState(ctx, tbl)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer

	if ts.IsEmpty() {
		ts = types.MaxTs()
	}

	{
		buf.WriteString("\nobject info:\n")
		objectIter, err := state.NewObjectsIter(ts)
		if err != nil {
			return "", err
		}

		for objectIter.Next() {
			objectEntry := objectIter.Entry()
			buf.WriteString(fmt.Sprintf("\t%s\n", objectEntry.String()))
		}
		objectIter.Close()
	}

	buf.WriteString("\n")

	{
		buf.WriteString("\n dirty block info:\n")
		dirtyIter := state.NewDirtyBlocksIter()
		if err != nil {
			return "", nil
		}

		for dirtyIter.Next() {
			dirty := dirtyIter.Entry()
			buf.WriteString(fmt.Sprintf("\t%s\n", dirty.String()))
		}

		dirtyIter.Close()

	}

	return buf.String(), nil
}
