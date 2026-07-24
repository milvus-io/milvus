package idempotency

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestIdempotencyMetricsObserveWindowInflightEntriesAndEvents(t *testing.T) {
	paramtable.Init()

	msg := newIdempotentInsertMessage(t, "metrics-idempotency-vchannel", "metrics-key")
	nodeID, vchannel := paramtable.GetStringNodeID(), vchannelOf(msg)
	entryGauge := pkgmetrics.WALIdempotencyWindowEntries.WithLabelValues(nodeID, vchannel)
	inflightGauge := pkgmetrics.WALIdempotencyWindowInflight.WithLabelValues(nodeID, vchannel)
	duplicateCounter := pkgmetrics.WALIdempotencyDuplicateTotal.WithLabelValues(nodeID, vchannel)
	evictionCounter := pkgmetrics.WALIdempotencyEvictionTotal.WithLabelValues(nodeID, vchannel)
	defer pkgmetrics.WALIdempotencyWindowEntries.DeleteLabelValues(nodeID, vchannel)
	defer pkgmetrics.WALIdempotencyWindowInflight.DeleteLabelValues(nodeID, vchannel)
	defer pkgmetrics.WALIdempotencyDuplicateTotal.DeleteLabelValues(nodeID, vchannel)
	defer pkgmetrics.WALIdempotencyEvictionTotal.DeleteLabelValues(nodeID, vchannel)

	duplicateBefore := testutil.ToFloat64(duplicateCounter)
	evictionBefore := testutil.ToFloat64(evictionCounter)

	probe := NewWindow(WindowConfig{})
	probeBegin := probe.Begin("metrics-key", nil)
	require.Equal(t, BeginDecisionOwner, probeBegin.Decision)
	completed, _ := probe.Complete(probeBegin.Pending, CommitResult{CommitTimeTick: 100}, nil)
	require.True(t, completed)
	require.Positive(t, probe.bytes)
	probeNext := NewWindow(WindowConfig{})
	probeNextBegin := probeNext.Begin("metrics-key-2", nil)
	require.Equal(t, BeginDecisionOwner, probeNextBegin.Decision)
	completed, _ = probeNext.Complete(probeNextBegin.Pending, CommitResult{CommitTimeTick: 110}, nil)
	require.True(t, completed)
	maxBytes := probe.bytes
	if probeNext.bytes > maxBytes {
		maxBytes = probeNext.bytes
	}

	window := NewWindow(WindowConfig{MinEntries: 0, MaxBytes: maxBytes})
	begin := window.Begin("metrics-key", msg)
	require.Equal(t, BeginDecisionOwner, begin.Decision)

	require.Equal(t, float64(0), testutil.ToFloat64(entryGauge))
	require.Equal(t, float64(1), testutil.ToFloat64(inflightGauge))

	completed, evicted := window.Complete(begin.Pending, CommitResult{CommitTimeTick: 100}, msg)
	require.True(t, completed)
	require.Zero(t, evicted)

	require.Equal(t, float64(1), testutil.ToFloat64(entryGauge))
	require.Equal(t, float64(0), testutil.ToFloat64(inflightGauge))

	duplicate := window.Begin("metrics-key", msg)
	require.Equal(t, BeginDecisionDuplicate, duplicate.Decision)
	require.Equal(t, duplicateBefore+1, testutil.ToFloat64(duplicateCounter))

	next := window.Begin("metrics-key-2", msg)
	require.Equal(t, BeginDecisionOwner, next.Decision)
	completed, evicted = window.Complete(next.Pending, CommitResult{CommitTimeTick: 110}, msg)
	require.True(t, completed)
	require.Equal(t, 1, evicted)
	require.Equal(t, evictionBefore+1, testutil.ToFloat64(evictionCounter))
}
