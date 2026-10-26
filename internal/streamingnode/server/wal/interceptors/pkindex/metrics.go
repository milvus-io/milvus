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
