package idempotency

import (
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func observeWindowEntries(msg message.MutableMessage, entries int) {
	if msg == nil {
		return
	}
	nodeID, vchannel := idempotencyMetricLabels(msg)
	metrics.WALIdempotencyWindowEntries.WithLabelValues(nodeID, vchannel).Set(float64(entries))
}

func observeWindowInflight(msg message.MutableMessage, inflight int) {
	if msg == nil {
		return
	}
	nodeID, vchannel := idempotencyMetricLabels(msg)
	metrics.WALIdempotencyWindowInflight.WithLabelValues(nodeID, vchannel).Set(float64(inflight))
}

func observeWindowDuplicate(msg message.MutableMessage) {
	if msg == nil {
		return
	}
	nodeID, vchannel := idempotencyMetricLabels(msg)
	metrics.WALIdempotencyDuplicateTotal.WithLabelValues(nodeID, vchannel).Inc()
}

func observeWindowEviction(msg message.MutableMessage, count int) {
	if msg == nil {
		return
	}
	if count <= 0 {
		return
	}
	nodeID, vchannel := idempotencyMetricLabels(msg)
	metrics.WALIdempotencyEvictionTotal.WithLabelValues(nodeID, vchannel).Add(float64(count))
}

// deleteWindowMetrics drops a vchannel's idempotency window metric series so they
// do not accumulate forever in the registry as the interceptor tears down. The
// series are keyed by (node_id, vchannel); vchannel encodes its pchannel.
func deleteWindowMetrics(vchannel string) {
	nodeID := paramtable.GetStringNodeID()
	metrics.WALIdempotencyWindowEntries.DeleteLabelValues(nodeID, vchannel)
	metrics.WALIdempotencyWindowInflight.DeleteLabelValues(nodeID, vchannel)
	metrics.WALIdempotencyDuplicateTotal.DeleteLabelValues(nodeID, vchannel)
	metrics.WALIdempotencyEvictionTotal.DeleteLabelValues(nodeID, vchannel)
}

func idempotencyMetricLabels(msg message.MutableMessage) (string, string) {
	return paramtable.GetStringNodeID(), msg.VChannel()
}
