package pkindex

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// pkindexMetrics binds the primary key index metrics to the labels of this node.
type pkindexMetrics struct {
	hits            prometheus.Counter
	misses          prometheus.Counter
	companionDelete prometheus.Counter
	decideDuration  prometheus.Observer
	lockWait        prometheus.Observer
}

func newMetrics() *pkindexMetrics {
	nodeID := paramtable.GetStringNodeID()
	return &pkindexMetrics{
		hits:            metrics.StreamingNodePKIndexProbedKeysTotal.WithLabelValues(nodeID, "hit"),
		misses:          metrics.StreamingNodePKIndexProbedKeysTotal.WithLabelValues(nodeID, "miss"),
		companionDelete: metrics.StreamingNodePKIndexCompanionDeleteKeysTotal.WithLabelValues(nodeID),
		decideDuration:  metrics.StreamingNodePKIndexDecideDurationSeconds.WithLabelValues(nodeID),
		lockWait:        metrics.StreamingNodePKIndexLockWaitDurationSeconds.WithLabelValues(nodeID),
	}
}

func (m *pkindexMetrics) observeDecide(total, lockWait time.Duration, probed, hit int) {
	m.decideDuration.Observe(total.Seconds())
	m.lockWait.Observe(lockWait.Seconds())
	m.hits.Add(float64(hit))
	m.misses.Add(float64(probed - hit))
}

// observeCommitDecide records the decision of a CommitTxn. It leaves the probed
// key counters alone, see the caller.
func (m *pkindexMetrics) observeCommitDecide(total, lockWait time.Duration) {
	m.decideDuration.Observe(total.Seconds())
	m.lockWait.Observe(lockWait.Seconds())
}

func (m *pkindexMetrics) observeCompanionDelete(keys int) {
	m.companionDelete.Add(float64(keys))
}
