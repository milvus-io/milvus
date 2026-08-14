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
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// aboveHighWatermark is a measured figure well above the 0.85 high watermark on
// the incident node, so a sample taken with it freezes admission.
var aboveHighWatermark = int64(0.9 * gibFloat * 64)

// resetResourceMetrics clears every series this package writes. The vectors are
// process-wide, so a test that did not start from zero would be reading
// whatever ran before it.
func resetResourceMetrics(t *testing.T) {
	t.Helper()
	metrics.DataNodeResourceReservedMemory.Reset()
	metrics.DataNodeResourceReservedCPU.Reset()
	metrics.DataNodeResourceBudgetMemory.Reset()
	metrics.DataNodeResourceBudgetCPU.Reset()
	metrics.DataNodeResourceNonTaskMemory.Reset()
	metrics.DataNodeResourceObservedMemory.Reset()
	metrics.DataNodeResourceFrozen.Reset()
	metrics.DataNodeResourceExclusive.Reset()
	metrics.DataNodeResourceWaitingTasks.Reset()
	metrics.DataNodeTaskAdmissionDeferred.Reset()
	metrics.DataNodeTaskAdmissionWait.Reset()
}

// gaugeValue reads the one series a gauge vector is expected to carry, and
// fails if the code under test wrote a different number of them -- a metric
// published under a stray label set would otherwise read as a clean zero.
func gaugeValue(t *testing.T, vec *prometheus.GaugeVec) float64 {
	t.Helper()
	require.Equal(t, 1, testutil.CollectAndCount(vec), "expected exactly one series")
	return testutil.ToFloat64(vec)
}

// deferredCount reads one reason's counter. Reading it through
// WithLabelValues(expected...) is deliberate: if the code under test wrote a
// different reason, this creates a fresh zero child and the assertion fails.
func deferredCount(t *testing.T, taskType taskcommon.Type, reason string) float64 {
	t.Helper()
	return testutil.ToFloat64(metrics.DataNodeTaskAdmissionDeferred.WithLabelValues(
		paramtable.GetStringNodeID(), taskType, reason))
}

// admissionWaitCount returns how many waits were recorded for a task type, and
// their total, read back out of the histogram itself.
func admissionWaitCount(t *testing.T, taskType taskcommon.Type) (uint64, float64) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(metrics.DataNodeTaskAdmissionWait))
	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if !hasLabel(m, "task_type", taskType) {
				continue
			}
			require.True(t, hasLabel(m, "node_id", paramtable.GetStringNodeID()),
				"admission wait must be labeled with this node")
			return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
		}
	}
	return 0, 0
}

func hasLabel(m *dto.Metric, name, value string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}

// seriesLabels collects the label sets a collector currently carries.
func seriesLabels(t *testing.T, c prometheus.Collector) []map[string]string {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	mfs, err := reg.Gather()
	require.NoError(t, err)

	var out []map[string]string
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			out = append(out, labels)
		}
	}
	return out
}

// Every series has to name the node that produced it, or a cluster-wide
// dashboard cannot tell which DataNode is throttling -- which is the question
// these metrics exist to answer.
func TestPublishedSeriesNameTheirNode(t *testing.T) {
	resetResourceMetrics(t)
	previous := paramtable.GetNodeID()
	t.Cleanup(func() { paramtable.SetNodeID(previous) })
	paramtable.SetNodeID(4242)

	mockIncidentNode(t, 0)
	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 1, Memory: gib})
	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: gib})
	ok, _ := g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: gib})
	require.False(t, ok)
	g.sampleOnce()

	for _, c := range []prometheus.Collector{
		metrics.DataNodeResourceReservedMemory,
		metrics.DataNodeResourceReservedCPU,
		metrics.DataNodeResourceBudgetMemory,
		metrics.DataNodeResourceBudgetCPU,
		metrics.DataNodeResourceNonTaskMemory,
		metrics.DataNodeResourceObservedMemory,
		metrics.DataNodeResourceFrozen,
		metrics.DataNodeResourceExclusive,
		metrics.DataNodeResourceWaitingTasks,
		metrics.DataNodeTaskAdmissionDeferred,
	} {
		labels := seriesLabels(t, c)
		require.Len(t, labels, 1)
		assert.Equal(t, "4242", labels[0]["node_id"])
	}

	deferred := seriesLabels(t, metrics.DataNodeTaskAdmissionDeferred)[0]
	assert.Equal(t, taskcommon.Compaction, deferred["task_type"])
	assert.Equal(t, reasonInsufficient, deferred["reason"])
}

// TestPublishedGaugesMirrorTheSnapshot is the whole point of the gauges: an
// operator watching a throttling node must see the same numbers the guard is
// deciding on, not an executor's private counter.
func TestPublishedGaugesMirrorTheSnapshot(t *testing.T) {
	resetResourceMetrics(t)
	baseline := 6 * gib
	mockIncidentNode(t, baseline)

	g := NewGuard()
	req := taskresource.Requirement{CPU: 2, Memory: 5 * gib}
	mustAcquire(t, g, 1, taskcommon.Compaction, req)

	g.sampleOnce()

	snap := g.Snapshot()
	require.Equal(t, baseline-req.Memory, snap.NonTask,
		"setup: the sample must have produced a non-task reservation to report")

	assert.Equal(t, float64(snap.Reserved.Memory), gaugeValue(t, metrics.DataNodeResourceReservedMemory))
	assert.Equal(t, snap.Reserved.CPU, gaugeValue(t, metrics.DataNodeResourceReservedCPU))
	assert.Equal(t, float64(snap.Total.Memory), gaugeValue(t, metrics.DataNodeResourceBudgetMemory))
	assert.Equal(t, snap.Total.CPU, gaugeValue(t, metrics.DataNodeResourceBudgetCPU))
	assert.Equal(t, float64(snap.NonTask), gaugeValue(t, metrics.DataNodeResourceNonTaskMemory))
	assert.Equal(t, float64(baseline), gaugeValue(t, metrics.DataNodeResourceObservedMemory))
	assert.Equal(t, float64(0), gaugeValue(t, metrics.DataNodeResourceFrozen))
	assert.Equal(t, float64(0), gaugeValue(t, metrics.DataNodeResourceExclusive))
	assert.Equal(t, float64(0), gaugeValue(t, metrics.DataNodeResourceWaitingTasks))

	// The budget really is the reduced one, not raw node capacity: publishing
	// capacity here would tell an operator the node has room it does not have.
	assert.Equal(t, float64(taskresource.NodeCapacity().Memory-snap.NonTask),
		gaugeValue(t, metrics.DataNodeResourceBudgetMemory))
}

// A node that has quietly stopped admitting must say so. Both of the states
// that stop admission without the budget being full -- the watermark freeze and
// an oversized task holding the node -- get their own gauge, because from the
// outside they are indistinguishable from "genuinely busy".
func TestPublishedGaugesReportFrozen(t *testing.T) {
	resetResourceMetrics(t)
	mockIncidentNode(t, aboveHighWatermark)

	g := NewGuard()
	g.sampleOnce()

	require.True(t, g.Snapshot().Frozen, "setup: 90% of memory is above the high watermark")
	assert.Equal(t, float64(1), gaugeValue(t, metrics.DataNodeResourceFrozen))
}

func TestPublishedGaugesReportExclusiveOccupancy(t *testing.T) {
	resetResourceMetrics(t)
	mockIncidentNode(t, 0)

	g := NewGuard()
	oversized := taskresource.Requirement{CPU: 1, Memory: 2 * incidentNodeMemory}
	mustAcquire(t, g, 99, taskcommon.Index, oversized)
	require.Equal(t, int64(99), g.Snapshot().ExclusiveTaskID)

	g.sampleOnce()
	assert.Equal(t, float64(1), gaugeValue(t, metrics.DataNodeResourceExclusive))
	// The ledger is over-committed against the budget by design here; the
	// reserved gauge must report that rather than clamp it away.
	assert.Equal(t, float64(oversized.Memory), gaugeValue(t, metrics.DataNodeResourceReservedMemory))
}

func TestPublishedGaugesReportQueueDepth(t *testing.T) {
	resetResourceMetrics(t)
	mockIncidentNode(t, 0)

	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 8 * gib})
	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})
	}()

	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, time.Millisecond)
	g.sampleOnce()
	assert.Equal(t, float64(1), gaugeValue(t, metrics.DataNodeResourceWaitingTasks),
		"a node with tasks parked in Acquire must not look idle")

	cancel()
	<-done
}

// Every way a task can fail to be admitted gets its own reason, because they
// call for different responses: a frozen node is a memory problem, a full one
// is a capacity problem, and a node held by one oversized task is neither.
func TestAdmissionDeferralsAreCountedByReason(t *testing.T) {
	small := taskresource.Requirement{CPU: 1, Memory: gib}

	t.Run("frozen", func(t *testing.T) {
		resetResourceMetrics(t)
		mockIncidentNode(t, aboveHighWatermark)
		g := NewGuard()
		g.sampleOnce()
		require.True(t, g.Snapshot().Frozen)

		ok, _ := g.TryAcquire(1, taskcommon.Compaction, small)
		require.False(t, ok)
		assert.Equal(t, float64(1), deferredCount(t, taskcommon.Compaction, reasonFrozen))
	})

	t.Run("insufficient", func(t *testing.T) {
		resetResourceMetrics(t)
		mockIncidentNode(t, 0)
		g := NewGuard()
		g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 2 * gib})
		mustAcquire(t, g, 1, taskcommon.Compaction, small)
		mustAcquire(t, g, 2, taskcommon.Compaction, small)

		ok, _ := g.TryAcquire(3, taskcommon.Compaction, small)
		require.False(t, ok)
		assert.Equal(t, float64(1), deferredCount(t, taskcommon.Compaction, reasonInsufficient))
	})

	t.Run("exclusive", func(t *testing.T) {
		resetResourceMetrics(t)
		mockIncidentNode(t, 0)
		g := NewGuard()
		mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 2 * incidentNodeMemory})

		ok, _ := g.TryAcquire(2, taskcommon.Compaction, small)
		require.False(t, ok)
		assert.Equal(t, float64(1), deferredCount(t, taskcommon.Compaction, reasonExclusive))
	})

	t.Run("awaiting drain", func(t *testing.T) {
		resetResourceMetrics(t)
		mockIncidentNode(t, 0)
		g := NewGuard()
		mustAcquire(t, g, 1, taskcommon.Compaction, small)

		ok, _ := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 2 * incidentNodeMemory})
		require.False(t, ok)
		assert.Equal(t, float64(1), deferredCount(t, taskcommon.Index, reasonAwaitingDrain),
			"a task that needs the whole node is waiting for it to empty, not short of budget")
	})

	t.Run("head of line", func(t *testing.T) {
		resetResourceMetrics(t)
		mockIncidentNode(t, 0)
		g := NewGuard()
		g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 8 * gib})
		mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})
		}()
		require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, time.Millisecond)

		ok, _ := g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 3 * gib})
		require.False(t, ok)
		assert.Equal(t, float64(1), deferredCount(t, taskcommon.Index, reasonHeadOfLine),
			"budget held back for the longest waiter is not the same as no budget")

		cancel()
		<-done
	})

	// An oversized latecomer is held out by the queue as well, and for a reason
	// of its own: taking the whole node must not be a way to overtake tasks
	// already waiting. That is the queue holding it back, not the state of the
	// ledger -- delete the rule and the very same call comes back as
	// "awaiting_drain" instead, which would send an operator looking at the
	// wrong thing.
	t.Run("head of line beats an oversized latecomer", func(t *testing.T) {
		resetResourceMetrics(t)
		mockIncidentNode(t, 0)
		g := NewGuard()
		g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 8 * gib})
		mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})
		}()
		require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, time.Millisecond)

		ok, _ := g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 100 * gib})
		require.False(t, ok)
		assert.Equal(t, float64(1), deferredCount(t, taskcommon.Index, reasonHeadOfLine))
		assert.Equal(t, float64(0), deferredCount(t, taskcommon.Index, reasonAwaitingDrain))

		cancel()
		<-done
	})
}

// The wait histogram has to contain the admissions that did not wait as well as
// the ones that did, or every quantile drawn from it reads as though the node
// were permanently congested.
func TestAdmissionWaitIsObserved(t *testing.T) {
	resetResourceMetrics(t)
	mockIncidentNode(t, 0)

	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 8 * gib})

	require.NoError(t, g.Acquire(context.Background(), 1, taskcommon.Compaction,
		taskresource.Requirement{CPU: 1, Memory: 6 * gib}))
	count, sum := admissionWaitCount(t, taskcommon.Compaction)
	assert.Equal(t, uint64(1), count, "an admission that did not wait is still an observation")
	assert.Less(t, sum, float64(1000), "and it must be recorded as a short wait, not a long one")

	// Now one that really waits: it is only admissible once task 1 releases.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 6 * gib})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	g.Release(1)
	<-done

	count, sum = admissionWaitCount(t, taskcommon.Compaction)
	require.Equal(t, uint64(2), count)
	assert.Greater(t, sum, float64(5), "the time spent queued must reach the histogram")
}

// A caller whose context ends never reserved anything, so it is not an
// admission: recording it would put a wait that ended in nothing into a
// histogram of waits that ended in work.
func TestAbandonedAcquireIsNotRecordedAsAnAdmission(t *testing.T) {
	resetResourceMetrics(t)
	mockIncidentNode(t, 0)

	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 8 * gib})
	mustAcquire(t, g, 1, taskcommon.Import, taskresource.Requirement{CPU: 1, Memory: 6 * gib})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := g.Acquire(ctx, 2, taskcommon.Import, taskresource.Requirement{CPU: 1, Memory: 6 * gib})
	require.Error(t, err)

	count, _ := admissionWaitCount(t, taskcommon.Import)
	assert.Equal(t, uint64(0), count)
	assert.Greater(t, deferredCount(t, taskcommon.Import, reasonInsufficient), float64(0),
		"the deferrals it did rack up are still counted")
}

// The metrics are a report, never an input: nothing the guard decides may read
// them back. The estimate-versus-reserved signal is the one that would be most
// tempting to close the loop on, so it is asserted directly -- moving the
// observed-memory gauge by hand must not move a single admission.
func TestMetricsNeverFeedBackIntoAdmission(t *testing.T) {
	resetResourceMetrics(t)
	mockIncidentNode(t, 0)

	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 4, Memory: 8 * gib})
	req := taskresource.Requirement{CPU: 1, Memory: 4 * gib}

	metrics.DataNodeResourceObservedMemory.WithLabelValues(paramtable.GetStringNodeID()).Set(1e18)
	metrics.DataNodeResourceBudgetMemory.WithLabelValues(paramtable.GetStringNodeID()).Set(0)
	metrics.DataNodeResourceFrozen.WithLabelValues(paramtable.GetStringNodeID()).Set(1)

	ok, avail := g.TryAcquire(1, taskcommon.Compaction, req)
	assert.True(t, ok, "admission is decided by the ledger, not by whatever the gauges say")
	assert.Equal(t, 4*gib, avail.Memory)
	assert.Equal(t, req.Memory, g.Snapshot().Reserved.Memory)
}
