// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSplitRewriteKeepsRowsExpiringAfterFixedSwitch(t *testing.T) {
	fixed := time.Unix(2000, 0)
	later := fixed.Add(time.Hour)
	plan := &datapb.CompactionPlan{StartTime: later.Unix(), SplitRewrite: &datapb.SplitShardRewritePlan{TtlCutoffTimetick: tsoutil.ComposeTSByTime(fixed)}}
	mix := NewMixCompactionTask(t.Context(), nil, nil, plan, compaction.GenParams(), nil)
	hash := NewSplitShardRewriteCompactionTask(t.Context(), nil, plan, compaction.GenParams())
	sort := NewSortCompactionTask(t.Context(), nil, plan, compaction.GenParams(), nil)
	bump := NewBumpSchemaVersionCompactionTask(t.Context(), nil, plan, compaction.GenParams())
	for name, cutoff := range map[string]time.Time{"mix": mix.currentTime, "hash": hash.currentTime, "sort": sort.currentTime, "schema": bump.currentTime} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, fixed.UnixMilli(), cutoff.UnixMilli())
			row := tsoutil.ComposeTSByTime(fixed.Add(-time.Minute))
			// Both collection TTL and row TTL expire after T_switch, before execution.
			require.False(t, compaction.NewEntityFilter(nil, int64(2*time.Minute), cutoff, 0).Filtered(int64(1), row, -1))
			expiry := fixed.Add(time.Minute).UnixMicro()
			require.False(t, compaction.NewEntityFilter(nil, 0, cutoff, 0).Filtered(int64(1), row, expiry))
			require.True(t, compaction.NewEntityFilter(nil, int64(2*time.Minute), later, 0).Filtered(int64(1), row, -1), "execution wall time would lose this fixed snapshot row")
			require.True(t, compaction.NewEntityFilter(nil, 0, later, 0).Filtered(int64(1), row, expiry))
			expired := tsoutil.ComposeTSByTime(fixed.Add(-3 * time.Minute))
			require.True(t, compaction.NewEntityFilter(nil, int64(2*time.Minute), cutoff, 0).Filtered(int64(1), expired, -1), "already expired history need not be retained")
		})
	}
}

func TestQueuedSourceCompactionKeepsItsCreationTTLCutoff(t *testing.T) {
	fixed := time.Unix(2000, 0)
	later := fixed.Add(time.Hour)
	for _, start := range []int64{fixed.Unix(), 0} {
		plan := &datapb.CompactionPlan{StartTime: start}
		got := compactionTTLTime(plan, later)
		require.Equal(t, time.Unix(start, 0), got)
		require.False(t, compaction.NewEntityFilter(nil, int64(2*time.Minute), got, 0).Filtered(int64(1), tsoutil.ComposeTSByTime(fixed.Add(-time.Minute)), -1))
	}
}
