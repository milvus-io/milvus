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

package resource

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// observeAdmissionWait records how long a task sat in Accept before it started.
// That is zero unless the memory safety valve was engaged, which is the point:
// a non-zero quantile here means the node was holding work back on measured
// memory, and nothing else in the system produces that signal. Accepts that did
// not wait are recorded too, so the quantiles describe the node rather than
// only its bad moments.
func observeAdmissionWait(taskType taskcommon.Type, waited time.Duration) {
	metrics.DataNodeTaskAdmissionWait.
		WithLabelValues(paramtable.GetStringNodeID(), taskType).
		Observe(float64(waited.Nanoseconds()) / float64(time.Millisecond))
}

// publishLocked mirrors the ledger into Prometheus.
//
// It is called from the watermark loop rather than from every ledger change on
// purpose: the loop already holds the only measured input, and publishing more
// often than a scrape interval buys no resolution -- Prometheus samples the
// gauge when it scrapes, not when it is set. What it does buy is a consistent
// set: every gauge below comes from one hold of the lock, so a dashboard can
// subtract reserved from budget and get an answer that was true at some
// instant.
//
// Nothing reads these values back. In particular the observed figure published
// here is the estimate-versus-actual signal for tuning the memory factors by
// hand, and admission never consults it -- deciding from measured memory is the
// failure this package exists to prevent.
func (g *guard) publishLocked(observedMemory int64) {
	nodeID := paramtable.GetStringNodeID()

	capacity := g.capacityLocked()
	metrics.DataNodeResourceReservedMemory.WithLabelValues(nodeID).Set(float64(g.committed.Memory))
	metrics.DataNodeResourceReservedCPU.WithLabelValues(nodeID).Set(g.committed.CPU)
	metrics.DataNodeResourceBudgetMemory.WithLabelValues(nodeID).Set(float64(capacity.Memory))
	metrics.DataNodeResourceBudgetCPU.WithLabelValues(nodeID).Set(capacity.CPU)
	// The observed figure is the estimate-versus-actual signal for tuning the
	// memory factors by hand. Comparing it against reserved above is the only
	// way to tell a systematically low estimate from a correct one, and nothing
	// in the admission path consults it.
	metrics.DataNodeResourceObservedMemory.WithLabelValues(nodeID).Set(float64(observedMemory))
	metrics.DataNodeResourceFrozen.WithLabelValues(nodeID).Set(boolGauge(g.frozen))
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
