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

package nodescheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchedulerStatsTrackDelayedAttempts(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	var attempts atomic.Int32
	handle := scheduler.Submit(TaskFunc(func(context.Context) error {
		if attempts.Add(1) == 1 {
			return ErrDelay
		}
		return nil
	}))
	require.NoError(t, handle.Wait(context.Background()))

	snapshot := scheduler.stats.snapshotAndReset()
	assert.Equal(t, 1, snapshot.capacity)
	require.Len(t, snapshot.tasks, 1)
	stats := snapshot.tasks[0]
	assert.Equal(t, int64(1), stats.submitted)
	assert.Equal(t, int64(2), stats.started)
	assert.Equal(t, int64(1), stats.delayed)
	assert.Equal(t, int64(1), stats.completed)
	assert.Zero(t, stats.failed)
	assert.Zero(t, stats.canceled)
	assert.Zero(t, stats.queued)
	assert.Zero(t, stats.running)
	assert.Equal(t, int64(2), stats.queueWaitCount)
	assert.Equal(t, int64(2), stats.executeCount)

	scheduler.resize(2)
	assert.Equal(t, 2, scheduler.stats.snapshotAndReset().capacity)
}

func TestSchedulerMetrics(t *testing.T) {
	nodeID := paramtable.GetStringNodeID()
	for _, taskType := range []string{"blocker", "queued", "retried", "failed", "canceled"} {
		metrics.NodeSchedulerTaskExecutionTotal.DeletePartialMatch(prometheus.Labels{"task_type": taskType})
	}
	capacity := metrics.NodeSchedulerConcurrency.WithLabelValues(nodeID)
	baseline := testutil.ToFloat64(capacity)
	scheduler := New(1)
	t.Cleanup(scheduler.Close)
	require.Equal(t, baseline+1, testutil.ToFloat64(capacity))

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := scheduler.Submit(&metricTask{
		taskType: "blocker",
		execute: func(ctx context.Context) error {
			close(blockerStarted)
			select {
			case <-releaseBlocker:
			case <-ctx.Done():
			}
			return nil
		},
	})
	<-blockerStarted

	queued := scheduler.Submit(&metricTask{
		taskType: "queued",
		execute: func(context.Context) error {
			return nil
		},
	})

	require.Eventually(t, func() bool {
		pending := testutil.ToFloat64(metrics.NodeSchedulerPendingTaskCount.WithLabelValues(nodeID, "queued"))
		running := testutil.ToFloat64(metrics.NodeSchedulerRunningTaskCount.WithLabelValues(nodeID, "blocker"))
		return pending == 1 && running == 1
	}, time.Second, 10*time.Millisecond)

	close(releaseBlocker)
	require.NoError(t, blocker.Wait(context.Background()))
	require.NoError(t, queued.Wait(context.Background()))

	var retryAttempts atomic.Int32
	retried := scheduler.Submit(&metricTask{
		taskType: "retried",
		execute: func(context.Context) error {
			if retryAttempts.Add(1) == 1 {
				return ErrDelay
			}
			return nil
		},
	})
	require.NoError(t, retried.Wait(context.Background()))

	failed := scheduler.Submit(&metricTask{
		taskType: "failed",
		execute: func(context.Context) error {
			return errors.New("metric test failure")
		},
	})
	require.NoError(t, failed.Wait(context.Background()))

	cancelStarted := make(chan struct{})
	canceled := scheduler.Submit(&metricTask{
		taskType: "canceled",
		execute: func(ctx context.Context) error {
			close(cancelStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	<-cancelStarted
	canceled.Cancel()
	require.NoError(t, canceled.Wait(context.Background()))

	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(nodeID, "queued", metrics.SuccessLabel),
	))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(nodeID, "retried", metrics.RetryLabel),
	))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(nodeID, "retried", metrics.SuccessLabel),
	))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(nodeID, "failed", metrics.FailLabel),
	))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(nodeID, "canceled", metrics.CancelLabel),
	))
	require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerPendingTaskCount.WithLabelValues(nodeID, "queued")))
	require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerRunningTaskCount.WithLabelValues(nodeID, "blocker")))

	scheduler.Close()
	require.Equal(t, baseline, testutil.ToFloat64(capacity))
}

func TestSchedulerMetricsFollowSessionRegistration(t *testing.T) {
	originalNodeID := paramtable.GetNodeID()
	t.Cleanup(func() { paramtable.SetNodeID(originalNodeID) })
	paramtable.SetNodeID(0)
	oldCapacity := metrics.NodeSchedulerConcurrency.WithLabelValues("0")
	newCapacity := metrics.NodeSchedulerConcurrency.WithLabelValues("971231")
	oldBaseline, newBaseline := testutil.ToFloat64(oldCapacity), testutil.ToFloat64(newCapacity)
	first, second := New(1), New(2)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	started := make(chan struct{})
	running := first.Submit(&metricTask{
		taskType: "registration_running",
		execute: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	<-started
	queued := first.Submit(&metricTask{taskType: "registration_queued"})
	require.Equal(t, oldBaseline+3, testutil.ToFloat64(oldCapacity))

	paramtable.SetNodeID(971231)
	// The periodic reporter also refreshes an otherwise idle instance.
	second.metrics.refreshNodeID()
	queued.Cancel()
	require.NoError(t, queued.Wait(context.Background()))
	require.Equal(t, oldBaseline, testutil.ToFloat64(oldCapacity))
	require.Equal(t, newBaseline+3, testutil.ToFloat64(newCapacity))
	for _, nodeID := range []string{"0", "971231"} {
		require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerPendingTaskCount.WithLabelValues(nodeID, "registration_queued")))
	}
	require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerRunningTaskCount.WithLabelValues("0", "registration_running")))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.NodeSchedulerRunningTaskCount.WithLabelValues("971231", "registration_running")))

	running.Cancel()
	require.NoError(t, running.Wait(context.Background()))
	first.Close()
	second.Close()
	require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerRunningTaskCount.WithLabelValues("971231", "registration_running")))
	require.Equal(t, oldBaseline, testutil.ToFloat64(oldCapacity))
	require.Equal(t, newBaseline, testutil.ToFloat64(newCapacity))
}

func TestSchedulerCapacityAggregatesInstances(t *testing.T) {
	gauge := metrics.NodeSchedulerConcurrency.WithLabelValues(paramtable.GetStringNodeID())
	baseline := testutil.ToFloat64(gauge)
	first, second := New(2), New(3)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	require.Equal(t, baseline+5, testutil.ToFloat64(gauge))
	first.resize(4)
	require.Equal(t, baseline+7, testutil.ToFloat64(gauge))
	second.resize(1)
	require.Equal(t, baseline+5, testutil.ToFloat64(gauge))
	first.Close()
	require.Equal(t, baseline+1, testutil.ToFloat64(gauge))
	first.Close()
	first.resize(10)
	require.Equal(t, baseline+1, testutil.ToFloat64(gauge))
	second.Close()
	require.Equal(t, baseline, testutil.ToFloat64(gauge))
}

func TestSchedulerMetricsDrainUnexecutedTasks(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "cancel"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			scheduler := New(1)
			t.Cleanup(scheduler.Close)
			nodeID := paramtable.GetStringNodeID()
			delayedType := t.Name() + "/delayed"
			queuedType := t.Name() + "/queued"
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			delayed := scheduler.Submit(&metricTask{taskType: delayedType, execute: func(context.Context) error {
				return DelayUntil(time.Now().Add(time.Hour))
			}})
			require.Eventually(t, func() bool {
				scheduler.mu.Lock()
				defer scheduler.mu.Unlock()
				return len(scheduler.delayed) == 1
			}, time.Second, time.Millisecond)
			started := make(chan struct{})
			blocker := scheduler.Submit(TaskFunc(func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}))
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			queued := scheduler.Submit(&metricTask{taskType: queuedType, execute: func(context.Context) error {
				t.Error("canceled queued task executed")
				return nil
			}})
			if shutdown {
				scheduler.Close()
			} else {
				delayed.Cancel()
				queued.Cancel()
				blocker.Cancel()
			}
			for _, handle := range []TaskHandle{delayed, queued, blocker} {
				require.NoError(t, handle.Wait(ctx))
			}
			for _, taskType := range []string{delayedType, queuedType} {
				require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerPendingTaskCount.WithLabelValues(nodeID, taskType)))
				require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerRunningTaskCount.WithLabelValues(nodeID, taskType)))
				require.Zero(t, testutil.ToFloat64(metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(nodeID, taskType, metrics.CancelLabel)), "unexecuted cancellation is not an execution attempt")
			}
		})
	}
}

type metricTask struct {
	taskType string
	execute  func(context.Context) error
}

func (t *metricTask) SchedulerTaskType() string {
	return t.taskType
}

func (t *metricTask) Execute(ctx context.Context) error {
	return t.execute(ctx)
}
