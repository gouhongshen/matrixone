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

package objectio

import (
	"fmt"
	"go.uber.org/zap/zapcore"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockReadStats_Export(t *testing.T) {
	var ratesExpected []string
	h1, t1 := rand.Int(), rand.Int()+1
	ratesExpected = append(ratesExpected,
		fmt.Sprintf("%.4f", float32(h1)/float32(t1)))
	BlkReadStats.BlksByReaderStats.Record(h1, t1)

	h2, t2 := rand.Int(), rand.Int()+1
	ratesExpected = append(ratesExpected,
		fmt.Sprintf("%.4f", float32(h2)/float32(t2)))
	BlkReadStats.EntryCacheHitStats.Record(h2, t2)

	h3, t3 := rand.Int(), rand.Int()+1
	ratesExpected = append(ratesExpected,
		fmt.Sprintf("%.4f", float32(h3)/float32(t3)))
	BlkReadStats.BlkCacheHitStats.Record(h3, t3)

	fields := BlkReadStats.Export()
	require.Equal(t, 9, len(fields))

	var ratesActually []string
	for _, field := range fields {
		if field.Type == zapcore.Float32Type {
			ratesActually = append(ratesActually,
				fmt.Sprintf("%.4f", math.Float32frombits(uint32(field.Integer))))
		}
	}

	sort.Slice(ratesExpected, func(i, j int) bool { return ratesExpected[i] < ratesExpected[j] })
	sort.Slice(ratesActually, func(i, j int) bool { return ratesActually[i] < ratesActually[j] })

	for id, _ := range ratesExpected {
		require.Equal(t, ratesExpected[id], ratesActually[id])
	}
}
