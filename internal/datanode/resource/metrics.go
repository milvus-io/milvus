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

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Reasons an admission attempt did not reserve. There is no "rejected" among
// them, and no "oversized" either: nothing is refused permanently, so every one
// of these says why a task is waiting rather than why it was turned away.
//
// They are split the way an operator's response splits. A frozen node is a
// memory problem and the tasks are innocent; a node short of budget is doing
// exactly what it should; a node held by one oversized task will not admit
// anything however small, and no amount of waiting changes that until the task
// finishes; and a task held out by the head-of-line reservation is being made
// to queue on purpose. Collapsing any two of these would leave the dashboard
// unable to tell a healthy busy node from a stuck one.
const (
	// reasonFrozen: measured memory is above the high watermark.
	reasonFrozen = "frozen"
	// reasonExclusive: one task larger than the node holds it alone.
	reasonExclusive = "exclusive"
	// reasonAwaitingDrain: this request needs the whole node, and the node is
	// not empty yet.
	reasonAwaitingDrain = "awaiting_drain"
	// reasonHeadOfLine: budget is being held for a task that has waited longer,
	// or the request wants the whole node from behind someone else in the queue.
	reasonHeadOfLine = "head_of_line"
	// reasonInsufficient: the ledger has no room for this request.
	reasonInsufficient = "insufficient"
)

// deferLocked records one deferred admission and returns what tryAcquireLocked
// hands back. Recording it here, at the branch that decided it, is what keeps
// the reason from drifting away from the rule it names.
func (g *guard) deferLocked(taskType taskcommon.Type, reason string, budget taskresource.Capacity) (bool, taskresource.Capacity) {
	metrics.DataNodeTaskAdmissionDeferred.
		WithLabelValues(paramtable.GetStringNodeID(), taskType, reason).
		Inc()
	return false, g.availLocked(budget)
}

// observeAdmissionWait records how long a task sat in Acquire before it
// started. Admissions that did not wait are recorded too: without them every
// quantile drawn from this histogram would describe a permanently congested
// node. A caller whose context ended is not recorded at all -- it never became
// an admission, and a wait that ended in no work does not belong in a
// distribution of waits that ended in work. Those show up as queue depth
// (resource_waiting_tasks) and as deferrals instead.
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
	budget := g.budgetLocked()
	nodeID := paramtable.GetStringNodeID()

	metrics.DataNodeResourceReservedMemory.WithLabelValues(nodeID).Set(float64(g.reserved.Memory))
	metrics.DataNodeResourceReservedCPU.WithLabelValues(nodeID).Set(g.reserved.CPU)
	metrics.DataNodeResourceBudgetMemory.WithLabelValues(nodeID).Set(float64(budget.Memory))
	metrics.DataNodeResourceBudgetCPU.WithLabelValues(nodeID).Set(budget.CPU)
	metrics.DataNodeResourceNonTaskMemory.WithLabelValues(nodeID).Set(float64(g.nonTask))
	metrics.DataNodeResourceObservedMemory.WithLabelValues(nodeID).Set(float64(observedMemory))
	metrics.DataNodeResourceFrozen.WithLabelValues(nodeID).Set(boolGauge(g.frozen))
	metrics.DataNodeResourceExclusive.WithLabelValues(nodeID).Set(boolGauge(g.exclusiveTaskID != 0))
	metrics.DataNodeResourceWaitingTasks.WithLabelValues(nodeID).Set(float64(len(g.waiters)))
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
