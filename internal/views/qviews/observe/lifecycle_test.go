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

package observe

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestLifecyclePreservesFirstBoundaryAndCleansUp(t *testing.T) {
	now := time.Now()
	o := newMetricsObserverWithNow(func() time.Time { return now })
	ctx := context.Background()
	view := qviews.QueryViewKey{ShardID: qviews.ShardID{VChannel: "test", ReplicaID: 1}}
	o.Observe(ctx, CoordViewCreatedEvent{View: view, State: qviews.QueryViewStatePreparing})
	now = now.Add(time.Second)
	o.Observe(ctx, CoordPersistViewEvent{View: view, State: qviews.QueryViewStatePreparing})
	first := o.lifecycles[metricViewKey{component: componentCoord, view: view}].since
	now = now.Add(time.Second)
	o.Observe(ctx, CoordPersistViewEvent{View: view, State: qviews.QueryViewStatePreparing})
	require.Equal(t, first, o.lifecycles[metricViewKey{component: componentCoord, view: view}].since)
	o.Observe(ctx, CoordViewReportAppliedEvent{ViewStateTransition: ViewStateTransition{View: view, From: qviews.QueryViewStatePreparing, To: qviews.QueryViewStateReady}})
	o.Observe(ctx, CoordSyncViewAcceptedEvent{View: view, State: qviews.QueryViewStateUp})
	o.Observe(ctx, CoordViewReportAppliedEvent{ViewStateTransition: ViewStateTransition{View: view, From: qviews.QueryViewStateReady, To: qviews.QueryViewStateUp}})
	require.Empty(t, o.lifecycles)
}

func TestLifecycleMarksMissingEnqueueInsteadOfInventingTime(t *testing.T) {
	o := NewMetricsObserver()
	ctx := context.Background()
	view := qviews.QueryViewKey{}
	metric := metrics.QueryStageItems.WithLabelValues("coord", "view_load", "coverage", "incomplete_order")
	before := testutil.ToFloat64(metric)
	o.Observe(ctx, CoordViewCreatedEvent{View: view, State: qviews.QueryViewStatePreparing})
	o.Observe(ctx, CoordViewReportAppliedEvent{ViewStateTransition: ViewStateTransition{View: view, To: qviews.QueryViewStateUp}})
	require.Empty(t, o.lifecycles)
	require.Equal(t, before+1, testutil.ToFloat64(metric))
	o.Observe(ctx, CoordSyncViewAcceptedEvent{View: view, State: qviews.QueryViewStateUp})
	require.Empty(t, o.lifecycles)
}

func TestLifecycleAbortedPrepareDoesNotLeak(t *testing.T) {
	o := NewMetricsObserver()
	ctx := context.Background()
	view := qviews.QueryViewKey{}
	o.Observe(ctx, QueryNodeAcquireSegmentsEvent{View: view})
	require.Len(t, o.lifecycles, 1)
	o.Observe(ctx, QueryNodeReleaseSegmentsEvent{View: view})
	require.Empty(t, o.lifecycles)
}

func TestStreamingNodeUnrecoverablePrepareEndsAsError(t *testing.T) {
	o := NewMetricsObserver()
	ctx := context.Background()
	view := qviews.QueryViewKey{}
	inflight := metrics.QueryStageInflight.WithLabelValues("streamingNode", "view_load", "prepare")
	baseline := testutil.ToFloat64(inflight)
	histogram := metrics.QueryStageDuration.WithLabelValues("streamingNode", "view_load", "prepare", "error").(prometheus.Metric)
	var before, after dto.Metric
	require.NoError(t, histogram.Write(&before))
	o.Observe(ctx, StreamingNodeAcquireResourceEvent{View: view})
	o.Observe(ctx, StreamingNodeReportViewEvent{View: view, State: qviews.QueryViewStatePreparing})
	require.Len(t, o.lifecycles, 1)
	o.Observe(ctx, StreamingNodeReportViewEvent{View: view, State: qviews.QueryViewStateUnrecoverable})
	o.Observe(ctx, StreamingNodeReportViewEvent{View: view, State: qviews.QueryViewStateUnrecoverable})
	o.Observe(ctx, StreamingNodeReleaseResourceEvent{View: view})
	require.Empty(t, o.lifecycles)
	require.Equal(t, baseline, testutil.ToFloat64(inflight))
	require.NoError(t, histogram.Write(&after))
	require.Equal(t, before.GetHistogram().GetSampleCount()+1, after.GetHistogram().GetSampleCount())
}
