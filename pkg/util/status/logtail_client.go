// Copyright 2021 -2024 Matrix Origin
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

package status

import (
	"fmt"
	newdisttae "github.com/matrixorigin/matrixone/pkg/vm/engine/newdisttae"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type SubTableID struct {
	DatabaseID uint64 `json:"database_id"`
	TableID    uint64 `json:"table_id"`
}

type SubTableStatus struct {
	SubState   int32     `json:"sub_state"`
	LatestTime time.Time `json:"latest_time"`
}

type LogtailStatus struct {
	LatestTS         timestamp.Timestamp       `json:"latest_ts"`
	SubscribedTables map[string]SubTableStatus `json:"subscribed_tables"`
}

func (s *LogtailStatus) fill(c *newdisttae.PushClient) {
	if c == nil {
		return
	}
	st := c.GetState()
	s.SubscribedTables = make(map[string]SubTableStatus, len(st.SubTables))
	for id, status := range st.SubTables {
		tid := fmt.Sprintf("%d-%d", id.DatabaseID, id.TableID)
		s.SubscribedTables[tid] = SubTableStatus{
			SubState:   int32(status.SubState),
			LatestTime: status.LatestTime,
		}
	}
	s.LatestTS = st.LatestTS
}
