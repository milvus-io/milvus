package idempotency

import (
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func observeWindowEntries(vchannel string, entries int) {
	if vchannel == "" {
		return
	}
	metrics.WALIdempotencyWindowEntries.WithLabelValues(paramtable.GetStringNodeID(), vchannel).Set(float64(entries))
}

func observeWindowInflight(vchannel string, inflight int) {
	if vchannel == "" {
		return
	}
	metrics.WALIdempotencyWindowInflight.WithLabelValues(paramtable.GetStringNodeID(), vchannel).Set(float64(inflight))
}

func observeWindowDuplicate(vchannel string) {
	if vchannel == "" {
		return
	}
	metrics.WALIdempotencyDuplicateTotal.WithLabelValues(paramtable.GetStringNodeID(), vchannel).Inc()
}

// The helpers key on the vchannel name directly (not a message) so callers
// without a message in hand — the idle-vchannel TTL sweep in particular — still
// report their evictions; an empty vchannel (pchannel-level caller) is skipped.
func observeWindowEviction(vchannel string, count int) {
	if vchannel == "" || count <= 0 {
		return
	}
	metrics.WALIdempotencyEvictionTotal.WithLabelValues(paramtable.GetStringNodeID(), vchannel).Add(float64(count))
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

// vchannelOf extracts the metric label from a possibly-nil message.
func vchannelOf(msg message.MutableMessage) string {
	if msg == nil {
		return ""
	}
	return msg.VChannel()
}
