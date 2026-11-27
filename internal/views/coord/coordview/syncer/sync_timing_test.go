// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func syncStageCount(t *testing.T, operation, stage, result string) uint64 {
	t.Helper()
	histogram := metrics.QueryStageDuration.WithLabelValues("coord", operation, stage, result).(prometheus.Metric)
	var sample dto.Metric
	require.NoError(t, histogram.Write(&sample))
	return sample.GetHistogram().GetSampleCount()
}

func TestSyncTimingSurvivesResendsAndEndsOnce(t *testing.T) {
	p := newPendingSyncQueryViews()
	t.Cleanup(p.closeTiming)
	inflight := metrics.QueryStageInflight.WithLabelValues("coord", "sync_preparing", "roundtrip")
	baseline := testutil.ToFloat64(inflight)
	success := syncStageCount(t, "sync_preparing", "roundtrip", "success")
	queue := syncStageCount(t, "sync", "send_queue", "success")
	view := newTestSyncViewWithState(1, 1, qviews.QueryViewStatePreparing, func(qviews.QueryViewAtWorkNode) bool { return false }, nil)
	p.Upsert(view)
	first := p.entries[view.View.QueryViewKey()].timing
	p.Upsert(view)
	require.Same(t, first, p.entries[view.View.QueryViewKey()].timing)
	require.Len(t, p.CollectProtos(), 1)
	require.Len(t, p.DrainUnsent(), 1)
	p.Upsert(view)
	require.Len(t, p.DrainUnsent(), 1)
	require.Equal(t, queue+1, syncStageCount(t, "sync", "send_queue", "success"))
	require.Equal(t, baseline+1, testutil.ToFloat64(inflight))
	ready := newTestSyncViewWithState(1, 1, qviews.QueryViewStateReady, nil, nil)
	p.MatchResponse(ready.View.IntoProto())
	p.MatchResponse(ready.View.IntoProto())
	p.closeTiming()
	require.Equal(t, success+1, syncStageCount(t, "sync_preparing", "roundtrip", "success"))
	require.Equal(t, baseline, testutil.ToFloat64(inflight))
}

func TestSyncTimingTerminalOutcomes(t *testing.T) {
	for _, result := range []string{"error", "superseded", "canceled"} {
		t.Run(result, func(t *testing.T) {
			p := newPendingSyncQueryViews()
			t.Cleanup(p.closeTiming)
			inflight := metrics.QueryStageInflight.WithLabelValues("coord", "sync_preparing", "roundtrip")
			baseline := testutil.ToFloat64(inflight)
			before := syncStageCount(t, "sync_preparing", "roundtrip", result)
			view := newTestSyncViewWithState(1, 1, qviews.QueryViewStatePreparing, func(qviews.QueryViewAtWorkNode) bool { return true }, nil)
			p.Upsert(view)
			switch result {
			case "error":
				response := newTestSyncViewWithState(1, 1, qviews.QueryViewStateUnrecoverable, nil, nil)
				p.MatchResponse(response.View.IntoProto())
			case "superseded":
				p.Upsert(newTestSyncViewWithState(1, 1, qviews.QueryViewStateDropped, nil, nil))
			case "canceled":
				p.closeTiming()
			}
			p.closeTiming()
			require.Equal(t, before+1, syncStageCount(t, "sync_preparing", "roundtrip", result))
			require.Equal(t, baseline, testutil.ToFloat64(inflight))
		})
	}
}
