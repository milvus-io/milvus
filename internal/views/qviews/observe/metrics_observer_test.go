package observe

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestMetricsObserverTracksCoordViewStates(t *testing.T) {
	metrics.QVViewStates.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		View:  view,
		State: qviews.QueryViewStatePreparing,
	})

	assertGaugeValue(t, metrics.QVViewStates, 1, "coord", qviews.QueryViewStatePreparing.String())

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})

	assertGaugeValue(t, metrics.QVViewStates, 0, "coord", qviews.QueryViewStatePreparing.String())
	assertGaugeValue(t, metrics.QVViewStates, 1, "coord", qviews.QueryViewStateReady.String())
}

func TestMetricsObserverCountsCoordViewTransitions(t *testing.T) {
	metrics.QVViewStates.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})

	assertCounterValue(
		t,
		metrics.QVViewTransitionTotal,
		1,
		"coord",
		qviews.QueryViewStatePreparing.String(),
		qviews.QueryViewStateReady.String(),
		"reportReady",
	)
}

func TestMetricsObserverSeparatesStateTotalByComponent(t *testing.T) {
	metrics.QVViewStates.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		View:  view,
		State: qviews.QueryViewStatePreparing,
	})
	observer.Observe(context.Background(), QueryNodeSegmentsReadyEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReadySegmentCount: 10,
	})

	assertGaugeValue(t, metrics.QVViewStates, 1, "coord", qviews.QueryViewStatePreparing.String())
	assertGaugeValue(t, metrics.QVViewStates, 1, "queryNode", qviews.QueryViewStateReady.String())
}

func TestMetricsObserverTracksCoordViewReadyPercentBuckets(t *testing.T) {
	metrics.QVViewReadyPercentBucket.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 10,
		View:         view,
		State:        qviews.QueryViewStatePreparing,
	})
	err := testutil.CollectAndCompare(
		metrics.QVViewReadyPercentBucket,
		strings.NewReader(`
# HELP milvus_qv_view_ready_percent_bucket current number of QueryViews by resource readiness percent bucket
# TYPE milvus_qv_view_ready_percent_bucket gauge
milvus_qv_view_ready_percent_bucket{component="coord",le="+Inf",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="0",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="100",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="25",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="50",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="75",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="90",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="99",state="Preparing"} 1
`),
		"milvus_qv_view_ready_percent_bucket",
	)
	if err != nil {
		t.Fatalf("collect ready percent bucket before report: %v", err)
	}

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStatePreparing,
		},
		ReportedState:        qviews.QueryViewStatePreparing,
		ResourceReadyPercent: 42,
	})
	err = testutil.CollectAndCompare(
		metrics.QVViewReadyPercentBucket,
		strings.NewReader(`
# HELP milvus_qv_view_ready_percent_bucket current number of QueryViews by resource readiness percent bucket
# TYPE milvus_qv_view_ready_percent_bucket gauge
milvus_qv_view_ready_percent_bucket{component="coord",le="+Inf",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="0",state="Preparing"} 0
milvus_qv_view_ready_percent_bucket{component="coord",le="100",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="25",state="Preparing"} 0
milvus_qv_view_ready_percent_bucket{component="coord",le="50",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="75",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="90",state="Preparing"} 1
milvus_qv_view_ready_percent_bucket{component="coord",le="99",state="Preparing"} 1
`),
		"milvus_qv_view_ready_percent_bucket",
	)
	if err != nil {
		t.Fatalf("collect ready percent bucket after report: %v", err)
	}
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 0, "coord", qviews.QueryViewStatePreparing.String(), "0")
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 0, "coord", qviews.QueryViewStatePreparing.String(), "25")
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 1, "coord", qviews.QueryViewStatePreparing.String(), "50")

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 0, "coord", qviews.QueryViewStatePreparing.String(), "50")
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 0, "coord", qviews.QueryViewStateReady.String(), "99")
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 1, "coord", qviews.QueryViewStateReady.String(), "100")
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 1, "coord", qviews.QueryViewStateReady.String(), "+Inf")

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStateReady,
			To:           qviews.QueryViewStateUp,
		},
		ReportedState:        qviews.QueryViewStateUp,
		ResourceReadyPercent: 100,
	})
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 0, "coord", qviews.QueryViewStateReady.String(), "100")
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 0, "coord", qviews.QueryViewStateReady.String(), "+Inf")
}

func TestMetricsObserverTracksShardLoadState(t *testing.T) {
	metrics.QVShardLoadStates.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 10,
		View:         view,
		State:        qviews.QueryViewStatePreparing,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 1, shardLoadStateLoading)

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateUp,
		},
		ReportedState:        qviews.QueryViewStateUp,
		ResourceReadyPercent: 100,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateLoading)
	assertGaugeValue(t, metrics.QVShardLoadStates, 1, shardLoadStateLoaded)

	nextView := testQueryViewKey()
	nextView.QueryViewVersion.QueryVersion++
	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 10,
		View:         nextView,
		State:        qviews.QueryViewStatePreparing,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateLoading)
	assertGaugeValue(t, metrics.QVShardLoadStates, 1, shardLoadStateLoaded)

	observer.Observe(context.Background(), CoordViewQueryNodeLostAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStateUp,
			To:           qviews.QueryViewStateUnrecoverable,
		},
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateLoaded)
	assertGaugeValue(t, metrics.QVShardLoadStates, 1, shardLoadStateRecovering)

	observer.Observe(context.Background(), CoordViewReleaseRequestedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStateUnrecoverable,
			To:           qviews.QueryViewStateDropping,
		},
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 1, shardLoadStateRecovering)

	observer.Observe(context.Background(), CoordViewReleaseRequestedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         nextView,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateDropping,
		},
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateRecovering)

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStateDropping,
			To:           qviews.QueryViewStateDropped,
		},
		ReportedState:        qviews.QueryViewStateDropped,
		ResourceReadyPercent: 100,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateRecovering)

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         nextView,
			From:         qviews.QueryViewStateDropping,
			To:           qviews.QueryViewStateDropped,
		},
		ReportedState:        qviews.QueryViewStateDropped,
		ResourceReadyPercent: 100,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateRecovering)

	reloadedView := testQueryViewKey()
	reloadedView.QueryViewVersion.QueryVersion += 2
	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 10,
		View:         reloadedView,
		State:        qviews.QueryViewStatePreparing,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 1, shardLoadStateLoading)
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateRecovering)
}

func TestMetricsObserverIgnoresWorkerShardLoadState(t *testing.T) {
	metrics.QVShardLoadStates.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), QueryNodeSegmentsReadyEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 12,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateUp,
		},
		ReadySegmentCount: 10,
	})

	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateLoaded)
}

func TestMetricsObserverCollectsViewStateMaxAgeSeconds(t *testing.T) {
	now := time.Unix(100, 0)
	observer := newMetricsObserverWithNow(func() time.Time {
		return now
	})
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 10,
		View:         view,
		State:        qviews.QueryViewStatePreparing,
	})
	now = now.Add(10 * time.Second)
	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})
	now = now.Add(5 * time.Second)
	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStateReady,
			To:           qviews.QueryViewStateUp,
		},
		ReportedState:        qviews.QueryViewStateUp,
		ResourceReadyPercent: 100,
	})
	now = now.Add(20 * time.Second)

	err := testutil.CollectAndCompare(
		metrics.QVViewStateMaxAgeSeconds,
		strings.NewReader(`
# HELP milvus_qv_view_state_max_age_seconds top QueryViews by seconds since last state or resource progress
# TYPE milvus_qv_view_state_max_age_seconds gauge
`),
		"milvus_qv_view_state_max_age_seconds",
	)
	if err != nil {
		t.Fatalf("collect max age metric: %v", err)
	}

	anotherView := testQueryViewKey()
	anotherView.QueryViewVersion.QueryVersion++
	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 11,
		View:         anotherView,
		State:        qviews.QueryViewStatePreparing,
	})
	now = now.Add(30 * time.Second)

	err = testutil.CollectAndCompare(
		metrics.QVViewStateMaxAgeSeconds,
		strings.NewReader(`
# HELP milvus_qv_view_state_max_age_seconds top QueryViews by seconds since last state or resource progress
# TYPE milvus_qv_view_state_max_age_seconds gauge
milvus_qv_view_state_max_age_seconds{collection_id="11",component="coord",data_version="10/20",query_view_version="10/20/4",rank="1",replica_id="1",state="Preparing",vchannel="v1"} 30
`),
		"milvus_qv_view_state_max_age_seconds",
	)
	if err != nil {
		t.Fatalf("collect max age metric: %v", err)
	}
}

func TestMetricsObserverLimitsViewStateMaxAgeSecondsToTopNPerComponent(t *testing.T) {
	now := time.Unix(100, 0)
	observer := newMetricsObserverWithNow(func() time.Time {
		return now
	})

	for i := 0; i < defaultViewStateMaxAgeTopN+1; i++ {
		view := testQueryViewKey()
		view.ShardID.ReplicaID = int64(i + 1)
		view.QueryViewVersion.QueryVersion = int64(i + 1)
		observer.Observe(context.Background(), CoordViewCreatedEvent{
			CollectionID: int64(100 + i),
			View:         view,
			State:        qviews.QueryViewStatePreparing,
		})
		now = now.Add(time.Second)
	}
	for i := 0; i < defaultViewStateMaxAgeTopN+1; i++ {
		view := testQueryViewKey()
		view.ShardID.ReplicaID = int64(i + 11)
		view.QueryViewVersion.QueryVersion = int64(i + 11)
		observer.Observe(context.Background(), QueryNodeSegmentsReadyEvent{
			ViewStateTransition: ViewStateTransition{
				CollectionID: int64(200 + i),
				View:         view,
				From:         qviews.QueryViewStatePreparing,
				To:           qviews.QueryViewStateReady,
			},
			ReadySegmentCount: 10,
		})
		now = now.Add(time.Second)
	}
	now = now.Add(10 * time.Second)

	metrics := observer.collectViewStateMaxAge()
	want := defaultViewStateMaxAgeTopN * 2
	if len(metrics) != want {
		t.Fatalf("topN metric count = %d, want %d", len(metrics), want)
	}
	if metrics[0].Component != "coord" || metrics[0].ReplicaID != "1" || metrics[0].Rank != "1" {
		t.Fatalf("first coord topN metric = %#v, want oldest coord replica with rank 1", metrics[0])
	}
	if metrics[defaultViewStateMaxAgeTopN-1].Component != "coord" || metrics[defaultViewStateMaxAgeTopN-1].ReplicaID != "5" || metrics[defaultViewStateMaxAgeTopN-1].Rank != "5" {
		t.Fatalf("last coord topN metric = %#v, want fifth oldest coord replica with rank 5", metrics[defaultViewStateMaxAgeTopN-1])
	}
	if metrics[defaultViewStateMaxAgeTopN].Component != "queryNode" || metrics[defaultViewStateMaxAgeTopN].ReplicaID != "11" || metrics[defaultViewStateMaxAgeTopN].Rank != "1" {
		t.Fatalf("first queryNode topN metric = %#v, want oldest queryNode replica with rank 1", metrics[defaultViewStateMaxAgeTopN])
	}
	if metrics[len(metrics)-1].Component != "queryNode" || metrics[len(metrics)-1].ReplicaID != "15" || metrics[len(metrics)-1].Rank != "5" {
		t.Fatalf("last queryNode topN metric = %#v, want fifth oldest queryNode replica with rank 5", metrics[len(metrics)-1])
	}
}

func TestMetricsObserverTracksTimeSinceProgress(t *testing.T) {
	metrics.QVViewStates.Reset()
	metrics.QVShardLoadStates.Reset()
	metrics.QVViewReadyPercentBucket.Reset()
	start := time.Unix(100, 0)
	now := start
	observer := newMetricsObserverWithNow(func() time.Time { return now })
	view := testQueryViewKey()
	observer.Observe(context.Background(), CoordViewCreatedEvent{
		View:  view,
		State: qviews.QueryViewStatePreparing,
	})

	now = start.Add(50 * time.Second)
	anotherView := view
	anotherView.QueryViewVersion.QueryVersion++
	anotherView.ShardID.ReplicaID++
	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 20,
		View:         anotherView,
		State:        qviews.QueryViewStatePreparing,
	})

	now = start.Add(100 * time.Second)
	for i := 0; i < 3; i++ {
		now = start.Add(time.Duration(100+i*25) * time.Second)
		observer.Observe(context.Background(), CoordViewReportAppliedEvent{
			ViewStateTransition: ViewStateTransition{
				CollectionID: 10,
				View:         view,
				From:         qviews.QueryViewStatePreparing,
				To:           qviews.QueryViewStatePreparing,
			},
			ReportedState:        qviews.QueryViewStatePreparing,
			ResourceReadyPercent: 42,
			ResourceProgress:     i == 0,
		})
	}
	now = start.Add(200 * time.Second)
	// Repeated scrapes must retain the oldest view and never duplicate candidates.
	for i := 0; i < 2; i++ {
		ages := observer.collectViewStateMaxAge()
		if len(ages) != 2 {
			t.Fatalf("topN metrics = %#v, want exactly two views", ages)
		}
		if ages[0].CollectionID != "20" || ages[0].Rank != "1" || ages[0].AgeSeconds != 150 {
			t.Fatalf("oldest view = %#v, want collection 20 at rank 1 with age 150", ages[0])
		}
		if ages[1].CollectionID != "10" || ages[1].Rank != "2" || ages[1].AgeSeconds != 100 {
			t.Fatalf("second view = %#v, want collection 10 at rank 2 with age 100", ages[1])
		}
	}
	assertGaugeValue(t, metrics.QVViewStates, 2, "coord", qviews.QueryViewStatePreparing.String())
	assertGaugeValue(t, metrics.QVShardLoadStates, 2, shardLoadStateLoading)
	// Progress updates the readiness buckets even when the view stays Preparing.
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 1, "coord", qviews.QueryViewStatePreparing.String(), readyPercentLe0)
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 2, "coord", qviews.QueryViewStatePreparing.String(), readyPercentLe50)
	assertGaugeValue(t, metrics.QVViewReadyPercentBucket, 2, "coord", qviews.QueryViewStatePreparing.String(), readyPercentLe75)
}

func TestMetricsObserverRefreshesTopNCandidateOnStateMove(t *testing.T) {
	now := time.Unix(100, 0)
	observer := newMetricsObserverWithNow(func() time.Time {
		return now
	})
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 10,
		View:         view,
		State:        qviews.QueryViewStatePreparing,
	})
	now = now.Add(100 * time.Second)
	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})
	now = now.Add(3 * time.Second)

	metrics := observer.collectViewStateMaxAge()
	if len(metrics) != 1 {
		t.Fatalf("topN metric count = %d, want 1", len(metrics))
	}
	if metrics[0].State != qviews.QueryViewStateReady.String() || metrics[0].AgeSeconds != 3 {
		t.Fatalf("topN metric after state move = %#v, want Ready age 3", metrics[0])
	}
}

func TestMetricsObserverRenewsWorkerAgeForNewlyReadySegments(t *testing.T) {
	start := time.Unix(100, 0)
	now := start
	observer := newMetricsObserverWithNow(func() time.Time { return now })
	report := func(count int) {
		observer.Observe(context.Background(), QueryNodeSegmentsReadyEvent{
			ViewStateTransition: ViewStateTransition{
				CollectionID: 10,
				View:         testQueryViewKey(),
				From:         qviews.QueryViewStatePreparing,
				To:           qviews.QueryViewStatePreparing,
			},
			ReadySegmentCount: count,
		})
	}
	report(0)
	now = start.Add(100 * time.Second)
	report(1)
	now = start.Add(150 * time.Second)
	// Worker callbacks contain deltas, so another single segment is progress.
	report(1)
	now = start.Add(190 * time.Second)
	report(0)
	now = start.Add(200 * time.Second)
	ages := observer.collectViewStateMaxAge()
	if len(ages) != 1 || ages[0].AgeSeconds != 50 {
		t.Fatalf("worker age = %#v, want one view with age 50", ages)
	}
}

func TestMetricsObserverCompactsTopNCandidatesWithoutScrape(t *testing.T) {
	now := time.Unix(100, 0)
	observer := newMetricsObserverWithNow(func() time.Time {
		return now
	})
	observer.topN = 2

	for i := 0; i < 20; i++ {
		view := testQueryViewKey()
		view.ShardID.ReplicaID = int64(i + 1)
		view.QueryViewVersion.QueryVersion = int64(i + 1)
		observer.Observe(context.Background(), CoordViewCreatedEvent{
			CollectionID: int64(100 + i),
			View:         view,
			State:        qviews.QueryViewStatePreparing,
		})
	}

	for i := 0; i < 20; i++ {
		view := testQueryViewKey()
		view.ShardID.ReplicaID = int64(i + 1)
		view.QueryViewVersion.QueryVersion = int64(i + 1)
		observer.Observe(context.Background(), CoordViewReportAppliedEvent{
			ViewStateTransition: ViewStateTransition{
				CollectionID: int64(100 + i),
				View:         view,
				From:         qviews.QueryViewStateReady,
				To:           qviews.QueryViewStateUp,
			},
			ReportedState:        qviews.QueryViewStateUp,
			ResourceReadyPercent: 100,
		})
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if h, ok := observer.topK["coord"]; ok && h.Len() != 0 {
		t.Fatalf("coord topN candidate heap len = %d, want compacted empty without scrape", h.Len())
	}
}

func TestMetricsObserverDropsTerminalWorkerViewState(t *testing.T) {
	metrics.QVViewStates.Reset()
	metrics.QVViewTransitionTotal.Reset()
	now := time.Unix(100, 0)
	observer := newMetricsObserverWithNow(func() time.Time {
		return now
	})
	view := testQueryViewKey()

	observer.Observe(context.Background(), QueryNodeSegmentsReadyEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 12,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateReady,
		},
		ReadySegmentCount: 10,
	})
	assertGaugeValue(t, metrics.QVViewStates, 1, "queryNode", qviews.QueryViewStateReady.String())

	observer.Observe(context.Background(), QueryNodeReleaseDoneEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 12,
			View:         view,
			From:         qviews.QueryViewStateDropping,
			To:           qviews.QueryViewStateDropped,
		},
	})

	assertGaugeValue(t, metrics.QVViewStates, 0, "queryNode", qviews.QueryViewStateReady.String())
	if got := observer.collectViewStateMaxAge(); len(got) != 0 {
		t.Fatalf("topN metrics after dropped = %#v, want empty", got)
	}
}

func assertGaugeValue(t *testing.T, collector *prometheus.GaugeVec, expected float64, labels ...string) {
	t.Helper()
	got := testutil.ToFloat64(collector.WithLabelValues(labels...))
	if got != expected {
		t.Fatalf("gauge labels %v = %v, want %v", labels, got, expected)
	}
}

func assertCounterValue(t *testing.T, collector *prometheus.CounterVec, expected float64, labels ...string) {
	t.Helper()
	got := testutil.ToFloat64(collector.WithLabelValues(labels...))
	if got != expected {
		t.Fatalf("counter labels %v = %v, want %v", labels, got, expected)
	}
}
