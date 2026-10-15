package observe

import (
	"context"
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

	assertGaugeValue(t, metrics.QVViewStates, 1, "coord", viewStateLabel(qviews.QueryViewStatePreparing))

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReportedState: qviews.QueryViewStateReady,
	})

	assertGaugeValue(t, metrics.QVViewStates, 0, "coord", viewStateLabel(qviews.QueryViewStatePreparing))
	assertGaugeValue(t, metrics.QVViewStates, 1, "coord", viewStateLabel(qviews.QueryViewStateReady))
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
		ReportedState: qviews.QueryViewStateReady,
	})

	assertCounterValue(
		t,
		metrics.QVViewTransitionTotal,
		1,
		"coord",
		viewStateLabel(qviews.QueryViewStatePreparing),
		viewStateLabel(qviews.QueryViewStateReady),
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

	assertGaugeValue(t, metrics.QVViewStates, 1, "coord", viewStateLabel(qviews.QueryViewStatePreparing))
	assertGaugeValue(t, metrics.QVViewStates, 1, "queryNode", viewStateLabel(qviews.QueryViewStateReady))
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
		ReportedState: qviews.QueryViewStateUp,
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
		ReportedState: qviews.QueryViewStateDropped,
	})
	assertGaugeValue(t, metrics.QVShardLoadStates, 0, shardLoadStateRecovering)

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         nextView,
			From:         qviews.QueryViewStateDropping,
			To:           qviews.QueryViewStateDropped,
		},
		ReportedState: qviews.QueryViewStateDropped,
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
		ReportedState: qviews.QueryViewStateReady,
	})
	now = now.Add(5 * time.Second)
	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStateReady,
			To:           qviews.QueryViewStateUp,
		},
		ReportedState: qviews.QueryViewStateUp,
	})
	now = now.Add(20 * time.Second)

	// Up is not a max-age candidate, so nothing is reported yet.
	if got := observer.collectViewStateMaxAge(); len(got) != 0 {
		t.Fatalf("topN metrics while only Up = %#v, want empty", got)
	}

	anotherView := testQueryViewKey()
	anotherView.QueryViewVersion.QueryVersion++
	observer.Observe(context.Background(), CoordViewCreatedEvent{
		CollectionID: 11,
		View:         anotherView,
		State:        qviews.QueryViewStatePreparing,
	})
	now = now.Add(30 * time.Second)

	got := observer.collectViewStateMaxAge()
	if len(got) != 1 {
		t.Fatalf("topN metric count = %d, want 1", len(got))
	}
	metric := got[0]
	if metric.Component != "coord" || metric.State != viewStateLabel(qviews.QueryViewStatePreparing) ||
		metric.Rank != "1" || metric.CollectionID != "11" || metric.ReplicaID != "1" ||
		metric.VChannel != "v1" || metric.QueryViewVersion != "10/20/4" ||
		metric.DataVersion != "10/20" || metric.AgeSeconds != 30 {
		t.Fatalf("topN metric = %#v, want coord preparing rank 1 collection 11 age 30", metric)
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
		ReportedState: qviews.QueryViewStateReady,
	})
	now = now.Add(3 * time.Second)

	metrics := observer.collectViewStateMaxAge()
	if len(metrics) != 1 {
		t.Fatalf("topN metric count = %d, want 1", len(metrics))
	}
	if metrics[0].State != viewStateLabel(qviews.QueryViewStateReady) || metrics[0].AgeSeconds != 3 {
		t.Fatalf("topN metric after state move = %#v, want Ready age 3", metrics[0])
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
			ReportedState: qviews.QueryViewStateUp,
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
	assertGaugeValue(t, metrics.QVViewStates, 1, "queryNode", viewStateLabel(qviews.QueryViewStateReady))

	observer.Observe(context.Background(), QueryNodeReleaseDoneEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 12,
			View:         view,
			From:         qviews.QueryViewStateDropping,
			To:           qviews.QueryViewStateDropped,
		},
	})

	assertGaugeValue(t, metrics.QVViewStates, 0, "queryNode", viewStateLabel(qviews.QueryViewStateReady))
	if got := observer.collectViewStateMaxAge(); len(got) != 0 {
		t.Fatalf("topN metrics after dropped = %#v, want empty", got)
	}
}

func TestMetricsObserverNoOpTransitionPreservesAgeAndSkipsCounter(t *testing.T) {
	metrics.QVViewTransitionTotal.Reset()
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

	// A repeated report of the same state is a no-op: it must not reset the
	// enteredAt clock (a stuck view keeps aging) nor count as a transition.
	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 10,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStatePreparing,
		},
		ReportedState: qviews.QueryViewStatePreparing,
	})

	got := observer.collectViewStateMaxAge()
	if len(got) != 1 || got[0].AgeSeconds != 10 {
		t.Fatalf("topN metrics after no-op report = %#v, want single Preparing entry aged 10s", got)
	}
	assertCounterValue(
		t,
		metrics.QVViewTransitionTotal,
		0,
		"coord",
		viewStateLabel(qviews.QueryViewStatePreparing),
		viewStateLabel(qviews.QueryViewStatePreparing),
		"reportPreparing",
	)
}

func TestMetricsObserverTracksWorkerAcquireAndApplyCoordView(t *testing.T) {
	metrics.QVViewStates.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	// Acquire starts a Preparing entry for the worker view.
	observer.Observe(context.Background(), QueryNodeAcquireSegmentsEvent{
		CollectionID: 12,
		View:         view,
		SegmentCount: 5,
	})
	assertGaugeValue(t, metrics.QVViewStates, 1, "queryNode", viewStateLabel(qviews.QueryViewStatePreparing))

	// The coord push applies the up view: Preparing -> Up with coordDelivered trigger.
	observer.Observe(context.Background(), QueryNodeApplyCoordViewEvent{
		ViewStateTransition: ViewStateTransition{
			CollectionID: 12,
			View:         view,
			From:         qviews.QueryViewStatePreparing,
			To:           qviews.QueryViewStateUp,
		},
	})
	assertGaugeValue(t, metrics.QVViewStates, 0, "queryNode", viewStateLabel(qviews.QueryViewStatePreparing))
	assertGaugeValue(t, metrics.QVViewStates, 1, "queryNode", viewStateLabel(qviews.QueryViewStateUp))
	assertCounterValue(
		t,
		metrics.QVViewTransitionTotal,
		1,
		"queryNode",
		viewStateLabel(qviews.QueryViewStatePreparing),
		viewStateLabel(qviews.QueryViewStateUp),
		"coordDelivered",
	)

	// StreamingNode acquire also starts a Preparing entry.
	observer.Observe(context.Background(), StreamingNodeAcquireResourceEvent{
		CollectionID: 13,
		View:         view,
	})
	assertGaugeValue(t, metrics.QVViewStates, 1, "streamingNode", viewStateLabel(qviews.QueryViewStatePreparing))
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
