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

package task

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

// Priority is first read by processQueue.Add after promote's stale/context check.
// Holding it reproduces checker cancellation in that precise promotion window.
type promotionBarrierTask struct {
	*SegmentTask
	entered chan struct{}
	resume  chan struct{}
}

func (work *promotionBarrierTask) Priority() Priority {
	close(work.entered)
	<-work.resume
	return work.SegmentTask.Priority()
}

type completionBarrierTask struct {
	*SegmentTask
	entered chan struct{}
	resume  chan struct{}
}

func (work *completionBarrierTask) IsFinished(dist *meta.DistributionManager) bool {
	finished := work.SegmentTask.IsFinished(dist)
	close(work.entered)
	<-work.resume
	return finished
}

func newTerminalStatusTestTask(t *testing.T) *SegmentTask {
	t.Helper()
	work, err := NewSegmentTask(context.Background(), time.Minute, WrapIDSource(0), 1, meta.NilReplica,
		commonpb.LoadPriority_LOW, NewSegmentAction(2, ActionTypeGrow, "ch", 7))
	require.NoError(t, err)
	t.Cleanup(func() { work.Cancel(nil) })
	return work
}

func awaitTaskBarrier(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not reach the concurrency barrier")
	}
}

func TestTaskCancelDuringPromotionKeepsTerminalState(t *testing.T) {
	work := newTerminalStatusTestTask(t)
	work.SetID(11)
	nodes := session.NewNodeManager()
	nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	scheduler := NewScheduler(context.Background(), nil, nil, nil, nil, nil, nodes)
	scheduler.segmentTasks.Insert(NewReplicaSegmentIndex(work), work)
	require.Len(t, scheduler.PendingSegmentLoads()[2], 1)

	barrier := &promotionBarrierTask{SegmentTask: work, entered: make(chan struct{}), resume: make(chan struct{})}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(barrier.resume) }) }
	t.Cleanup(resume)
	promoted := make(chan error, 1)
	go func() { promoted <- scheduler.promote(barrier) }()
	awaitTaskBarrier(t, barrier.entered)
	work.Cancel(context.DeadlineExceeded)
	resume()
	select {
	case err := <-promoted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("promotion did not finish")
	}

	require.Equal(t, TaskStatusCanceled, work.Status())
	require.False(t, scheduler.preProcess(work), "the canceled Grow must not be dispatched")
	require.Empty(t, scheduler.PendingSegmentLoads(), "a canceled task must release its load reservation")
	require.ErrorIs(t, work.Wait(context.Background()), context.DeadlineExceeded)
}

func TestTaskCancelDuringCompletionKeepsTerminalState(t *testing.T) {
	work := newTerminalStatusTestTask(t)
	work.StepUp()
	scheduler := &taskScheduler{}
	barrier := &completionBarrierTask{SegmentTask: work, entered: make(chan struct{}), resume: make(chan struct{})}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(barrier.resume) }) }
	t.Cleanup(resume)
	processed := make(chan bool, 1)
	go func() { processed <- scheduler.preProcess(barrier) }()
	awaitTaskBarrier(t, barrier.entered)
	work.Cancel(context.DeadlineExceeded)
	resume()
	select {
	case shouldProcess := <-processed:
		require.False(t, shouldProcess)
	case <-time.After(5 * time.Second):
		t.Fatal("completion did not finish")
	}

	require.Equal(t, TaskStatusCanceled, work.Status())
	require.ErrorIs(t, work.Wait(context.Background()), context.DeadlineExceeded)
}

func TestTaskSuccessKeepsNilResultDuringCancelOrFail(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "cancel"
		if fail {
			name = "fail"
		}
		t.Run(name, func(t *testing.T) {
			work := newTerminalStatusTestTask(t)
			entered, resumeCh := make(chan struct{}), make(chan struct{})
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(resumeCh) }) }
			t.Cleanup(resume)
			rootCancel := work.rootCancel
			work.rootCancel = func() {
				close(entered)
				<-resumeCh
				rootCancel()
			}
			finished := make(chan struct{})
			go func() {
				if fail {
					work.Fail(context.DeadlineExceeded)
				} else {
					work.Cancel(context.DeadlineExceeded)
				}
				close(finished)
			}()
			awaitTaskBarrier(t, entered)
			work.SetStatus(TaskStatusSucceeded)
			resume()
			awaitTaskBarrier(t, finished)

			require.Equal(t, TaskStatusSucceeded, work.Status())
			require.NoError(t, work.Wait(context.Background()))
			require.NoError(t, work.Err())
		})
	}
}

func TestTaskTerminalStatusCannotChange(t *testing.T) {
	for _, status := range []Status{TaskStatusSucceeded, TaskStatusCanceled, TaskStatusFailed} {
		t.Run(status, func(t *testing.T) {
			work := newTerminalStatusTestTask(t)
			work.SetStatus(status)
			for _, next := range []Status{TaskStatusStarted, TaskStatusSucceeded, TaskStatusCanceled, TaskStatusFailed} {
				work.SetStatus(next)
				require.Equal(t, status, work.Status())
			}
		})
	}
}
