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

package index

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPipelineWaitingBuildRetainsSlots(t *testing.T) {
	for _, outcome := range []string{"admit", "error", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			sched := newPipelineScheduler(t, 64)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var ready, fail atomic.Bool
			var attempts atomic.Int32
			built, cleaning := make(chan struct{}), make(chan struct{})
			cleanup, releaseCleanup := pipelineGate(t)
			task := &pipelineTestTask{
				id: outcome, ctx: ctx,
				reserve: func() (bool, error) {
					attempts.Add(1)
					if fail.Load() {
						return false, errors.New("reservation failed")
					}
					return ready.Load(), nil
				},
				build:   func() { close(built) },
				cleanup: func() { close(cleaning); <-cleanup },
			}
			require.NoError(t, sched.TaskQueue.Enqueue(task))
			require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 5*time.Second, time.Millisecond)
			require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
			switch outcome {
			case "admit":
				ready.Store(true)
			case "error":
				fail.Store(true)
			case "cancel":
				cancel()
			}
			awaitPipeline(t, cleaning)
			require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
			if outcome == "admit" {
				awaitPipeline(t, built)
			} else {
				select {
				case <-built:
					t.Fatal("failed or canceled task started building")
				default:
				}
			}
			releaseCleanup()
			expected := indexpb.JobState_JobStateFinished
			if outcome != "admit" {
				expected = indexpb.JobState_JobStateRetry
			}
			require.Eventually(t, func() bool {
				return task.GetState() == expected && sched.TaskQueue.GetUsingSlot() == 0
			}, 5*time.Second, time.Millisecond)
		})
	}
}

type pipelineTestTask struct {
	id         string
	slots      int64
	ctx        context.Context
	state      atomic.Int32
	prepare    func()
	build      func()
	upload     func()
	cleanup    func()
	reserve    func() (bool, error)
	prepareErr error
	buildErr   error
	uploadErr  error
}

type scalarPipelineTestTask struct {
	*pipelineTestTask
}

func (t *scalarPipelineTestTask) IsVectorIndex() bool { return false }

func (t *pipelineTestTask) GetSlot() int64 {
	if t.slots != 0 {
		return t.slots
	}
	return 64
}

func (t *pipelineTestTask) Ctx() context.Context             { return t.ctx }
func (t *pipelineTestTask) Name() string                     { return t.id }
func (t *pipelineTestTask) IsVectorIndex() bool              { return true }
func (t *pipelineTestTask) OnEnqueue(context.Context) error  { return nil }
func (t *pipelineTestTask) PreExecute(context.Context) error { return nil }
func (t *pipelineTestTask) SetState(state indexpb.JobState, _ string) {
	t.state.Store(int32(state))
}

func (t *pipelineTestTask) GetState() indexpb.JobState {
	return indexpb.JobState(t.state.Load())
}

func (t *pipelineTestTask) LoadData(context.Context) error {
	if t.prepare != nil {
		t.prepare()
	}
	return t.prepareErr
}

func (t *pipelineTestTask) TryReserveBuild() (bool, error) {
	if t.reserve != nil {
		return t.reserve()
	}
	return true, nil
}

func (t *pipelineTestTask) Execute(context.Context) error {
	if t.build != nil {
		t.build()
	}
	return t.buildErr
}

func (t *pipelineTestTask) PostExecute(context.Context) error {
	if t.upload != nil {
		t.upload()
	}
	return t.uploadErr
}

func (t *pipelineTestTask) Reset() {
	if t.cleanup != nil {
		t.cleanup()
	}
}

func newPipelineScheduler(t *testing.T, slots ...int64) *TaskScheduler {
	t.Helper()
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	capacity := int64(1024)
	if len(slots) != 0 {
		capacity = slots[0]
	}
	sched.totalSlots = func() int64 { return capacity }
	require.NoError(t, sched.Start())
	t.Cleanup(func() {
		sched.Close()
		sched.Wait()
	})
	return sched
}

func pipelineGate(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return ch, release
}

func awaitPipeline(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline stage did not make progress")
	}
}

func TestPipelineDownloadsAllSlotAdmittedTasks(t *testing.T) {
	sched := newPipelineScheduler(t)
	started := make(chan string, 4)
	var releases []func()
	for _, id := range []string{"first", "second", "third", "fourth"} {
		download, releaseDownload := pipelineGate(t)
		releases = append(releases, releaseDownload)
		task := &pipelineTestTask{
			id: id, ctx: context.Background(),
			prepare: func() { started <- id; <-download },
		}
		require.NoError(t, sched.TaskQueue.Enqueue(task))
	}
	var observed []string
	for range releases {
		select {
		case got := <-started:
			observed = append(observed, got)
		case <-time.After(5 * time.Second):
			t.Fatal("queued download did not start")
		}
	}
	require.ElementsMatch(t, []string{"first", "second", "third", "fourth"}, observed)
	require.EqualValues(t, 256, sched.TaskQueue.GetUsingSlot())
	for _, release := range releases {
		release()
	}
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}

func TestPipelineFullQueueBudgetDoesNotBlockBuild(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t, 128)
	download, releaseDownload := pipelineGate(t)
	build, releaseBuild := pipelineGate(t)
	downloading, building := make(chan struct{}), make(chan struct{})
	var ready atomic.Bool
	var attempts atomic.Int32
	first := &pipelineTestTask{
		id: "first", ctx: context.Background(),
		prepare: func() { close(downloading); <-download },
		reserve: func() (bool, error) { attempts.Add(1); return ready.Load(), nil },
		build:   func() { close(building); <-build },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, downloading)
	for _, id := range []string{"second", "third"} {
		require.NoError(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
			id: id, ctx: context.Background(),
		}))
	}
	require.ErrorIs(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
		id: "rejected", ctx: context.Background(),
	}), merr.ErrServiceResourceInsufficient)
	releaseDownload()
	require.Eventually(t, func() bool {
		used, queued := sched.TaskQueue.GetSlotUsage()
		return attempts.Load() != 0 && used == 64 && queued == 128
	}, 5*time.Second, time.Millisecond)
	ready.Store(true)
	awaitPipeline(t, building)
	used, queued := sched.TaskQueue.GetSlotUsage()
	require.EqualValues(t, 64, used)
	require.EqualValues(t, 128, queued)
	releaseBuild()
}

func TestPipelineTaskOrderRetainsSlotsUntilCompletion(t *testing.T) {
	sched := newPipelineScheduler(t, 64)
	download, releaseDownload := pipelineGate(t)
	build, releaseBuild := pipelineGate(t)
	downloading, building, nextDownload := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := &pipelineTestTask{
		id: "first", ctx: context.Background(),
		prepare: func() { close(downloading); <-download },
		build:   func() { close(building); <-build },
	}
	second := &pipelineTestTask{
		id: "second", ctx: context.Background(),
		prepare: func() { close(nextDownload) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, downloading)
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	require.Eventually(t, func() bool {
		sched.slotMu.Lock()
		defer sched.slotMu.Unlock()
		return len(sched.pendingTasks) == 2 && sched.pendingTasks[0].name == first.Name()
	}, 5*time.Second, time.Millisecond)
	// The earlier task retains priority when switching from download to build.
	releaseDownload()
	awaitPipeline(t, building)
	select {
	case <-nextDownload:
		t.Fatal("later download took slots before the earlier build")
	default:
	}
	releaseBuild()
	awaitPipeline(t, nextDownload)
	require.Eventually(t, func() bool {
		return second.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
	}, 5*time.Second, time.Millisecond)
}

func TestPipelineBuildAdmissionPreservesTaskOrder(t *testing.T) {
	for _, outcome := range []string{"admit", "cancel", "error"} {
		t.Run(outcome, func(t *testing.T) {
			sched := newPipelineScheduler(t, 128)
			download, releaseDownload := pipelineGate(t)
			build, releaseBuild := pipelineGate(t)
			downloading, building, nextDownloaded := make(chan struct{}), make(chan struct{}), make(chan struct{})
			nextBuilding := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var ready, fail atomic.Bool
			var attempts, nextAttempts atomic.Int32
			admitted := make(chan string, 2)
			first := &pipelineTestTask{
				id: "first", ctx: ctx,
				prepare: func() { close(downloading); <-download },
				reserve: func() (bool, error) {
					attempts.Add(1)
					if fail.Load() {
						return false, errors.New("reservation failed")
					}
					if !ready.Load() {
						return false, nil
					}
					admitted <- "first"
					return true, nil
				},
				build: func() { close(building); <-build },
			}
			second := &pipelineTestTask{
				id: "second", ctx: context.Background(),
				prepare: func() { close(nextDownloaded) },
				reserve: func() (bool, error) {
					nextAttempts.Add(1)
					admitted <- "second"
					return true, nil
				},
				build: func() { close(nextBuilding) },
			}
			require.NoError(t, sched.TaskQueue.Enqueue(first))
			awaitPipeline(t, downloading)
			require.NoError(t, sched.TaskQueue.Enqueue(second))
			awaitPipeline(t, nextDownloaded)
			require.Zero(t, nextAttempts.Load(), "download completion must not reorder builds")
			releaseDownload()
			require.Eventually(t, func() bool {
				return attempts.Load() >= 2 && sched.TaskQueue.GetUsingSlot() == 128
			}, 5*time.Second, time.Millisecond)
			sched.slotMu.Lock()
			names := make([]string, 0, len(sched.pendingTasks))
			for _, pending := range sched.pendingTasks {
				names = append(names, pending.name)
			}
			sched.slotMu.Unlock()
			require.Equal(t, []string{"first", "second"}, names)
			require.Zero(t, nextAttempts.Load(), "busy builds must retain their place")
			switch outcome {
			case "admit":
				ready.Store(true)
				awaitPipeline(t, building)
				awaitPipeline(t, nextBuilding)
				require.Equal(t, "first", <-admitted)
				releaseBuild()
			case "cancel":
				cancel()
			case "error":
				fail.Store(true)
			}
			awaitPipeline(t, nextBuilding)
			require.Equal(t, "second", <-admitted)
			require.Eventually(t, func() bool {
				return first.GetState() != indexpb.JobState_JobStateNone &&
					second.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
			}, 5*time.Second, time.Millisecond)
			if outcome != "admit" {
				require.Equal(t, indexpb.JobState_JobStateRetry, first.GetState())
				select {
				case <-building:
					t.Fatal("failed or canceled task started building")
				default:
				}
			}
		})
	}
}

func TestPipelineBuildDoesNotReacquireSlots(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 128 }
	var otherSlots atomic.Int64
	sched.SetExternalSlotUsage(otherSlots.Load)
	require.NoError(t, sched.Start())
	t.Cleanup(func() {
		sched.Close()
		sched.Wait()
	})
	download, releaseDownload := pipelineGate(t)
	build, releaseBuild := pipelineGate(t)
	downloading, building := make(chan struct{}), make(chan struct{})
	task := &pipelineTestTask{
		id: "index", ctx: context.Background(),
		prepare: func() { close(downloading); <-download },
		build:   func() { close(building); <-build },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(task))
	awaitPipeline(t, downloading)
	// Other work uses the remaining capacity; the index already owns its slots.
	otherSlots.Store(64)
	releaseDownload()
	awaitPipeline(t, building)
	require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
	releaseBuild()
	require.Eventually(t, func() bool {
		return task.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 64, otherSlots.Load())
}

func TestPipelineWaitingBuildIsNotOvertaken(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "cancel"}[cancelFirst], func(t *testing.T) {
			paramtable.Init()
			sched := NewTaskScheduler(context.Background())
			sched.totalSlots = func() int64 { return 128 }
			require.NoError(t, sched.Start())
			t.Cleanup(func() {
				sched.Close()
				sched.Wait()
			})
			download, releaseDownload := pipelineGate(t)
			build, releaseBuild := pipelineGate(t)
			downloading, building, nextDownload := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var ready atomic.Bool
			var attempts atomic.Int32
			first := &pipelineTestTask{
				id: "large", slots: 128, ctx: ctx,
				prepare: func() { close(downloading); <-download },
				reserve: func() (bool, error) { attempts.Add(1); return ready.Load(), nil },
				build:   func() { close(building); <-build },
			}
			require.NoError(t, sched.TaskQueue.Enqueue(first))
			awaitPipeline(t, downloading)
			releaseDownload()
			require.Eventually(t, func() bool {
				sched.slotMu.Lock()
				defer sched.slotMu.Unlock()
				return attempts.Load() != 0 && len(sched.pendingTasks) == 1 && sched.pendingTasks[0].name == first.Name()
			}, 5*time.Second, time.Millisecond)
			second := &pipelineTestTask{
				id: "small", slots: 32, ctx: context.Background(),
				prepare: func() { close(nextDownload) },
			}
			require.NoError(t, sched.TaskQueue.Enqueue(second))
			select {
			case <-nextDownload:
				t.Fatal("small download bypassed the waiting build")
			case <-time.After(150 * time.Millisecond):
			}
			if cancelFirst {
				cancel()
			} else {
				ready.Store(true)
				awaitPipeline(t, building)
				releaseBuild()
			}
			awaitPipeline(t, nextDownload)
			require.Eventually(t, func() bool {
				return second.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
			}, 5*time.Second, time.Millisecond)
			if cancelFirst {
				select {
				case <-building:
					t.Fatal("canceled build started")
				default:
				}
			}
		})
	}
}

func TestPipelineSlotsAndTaskOrderHaveSeparateLifetimes(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 128 }
	first := &pipelineTestTask{id: "first", ctx: context.Background()}
	second := &pipelineTestTask{id: "second", ctx: context.Background()}
	require.True(t, sched.acquireSlots(first))
	require.True(t, sched.acquireSlots(second))
	require.True(t, sched.isBuildTurn(first))
	require.False(t, sched.isBuildTurn(second))
	// Build admission advances FIFO without returning either task's slots.
	sched.finishAdmission(first.Name())
	require.True(t, sched.isBuildTurn(second))
	require.EqualValues(t, 128, sched.TaskQueue.GetUsingSlot())
	sched.finishAdmission(second.Name())
	require.Empty(t, sched.pendingTasks)
	require.EqualValues(t, 128, sched.TaskQueue.GetUsingSlot())
	sched.releaseSlots(first.Name())
	sched.releaseSlots(second.Name())
	require.Zero(t, sched.TaskQueue.GetUsingSlot())
}

func TestPipelineSharesExistingNodeSlots(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 128 }
	var otherSlots int64 = 128
	sched.SetExternalSlotUsage(func() int64 { return otherSlots })
	task := &pipelineTestTask{id: "index", ctx: context.Background()}
	require.False(t, sched.acquireSlots(task))
	otherSlots = 64
	require.True(t, sched.acquireSlots(task))
	sched.finishAdmission(task.Name())
	sched.releaseSlots(task.Name())
	require.EqualValues(t, 64, otherSlots)
	// Oversized index tasks wait for other work to release its slots.
	large := &pipelineTestTask{id: "large", slots: 256, ctx: context.Background()}
	require.False(t, sched.acquireSlots(large))
	otherSlots = 0
	require.True(t, sched.acquireSlots(large))
	sched.finishAdmission(large.Name())
	sched.releaseSlots(large.Name())
	require.Zero(t, sched.TaskQueue.GetUsingSlot())
	require.Empty(t, sched.pendingTasks)
}

func TestPipelineStageFailureReleasesSlotsAndTaskPermit(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	for _, phase := range []string{"prepare", "build", "upload"} {
		t.Run(phase, func(t *testing.T) {
			sched := newPipelineScheduler(t, 64)
			failure := errors.New("stage failed")
			preparing := make(chan struct{})
			failed := &pipelineTestTask{
				id: "failed", ctx: context.Background(),
				prepare: func() { close(preparing) },
			}
			switch phase {
			case "prepare":
				failed.prepareErr = failure
			case "build":
				failed.buildErr = failure
			case "upload":
				failed.uploadErr = failure
			}
			next := &pipelineTestTask{id: "next", ctx: context.Background()}
			require.NoError(t, sched.TaskQueue.Enqueue(failed))
			awaitPipeline(t, preparing)
			require.NoError(t, sched.TaskQueue.Enqueue(next))
			require.Eventually(t, func() bool {
				return failed.GetState() == indexpb.JobState_JobStateRetry &&
					next.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
			}, 5*time.Second, time.Millisecond)
		})
	}
}

func TestPipelineDownloadsUseSlots(t *testing.T) {
	sched := newPipelineScheduler(t, 128)
	started := make(chan string, 3)
	release, unblock := pipelineGate(t)
	for _, id := range []string{"first", "second", "third"} {
		task := &pipelineTestTask{
			id: id, ctx: context.Background(),
			prepare: func() {
				started <- id
				<-release
			},
		}
		require.NoError(t, sched.TaskQueue.Enqueue(task))
		if id != "third" {
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("download was not admitted")
			}
		}
	}
	require.Equal(t, int64(128), sched.TaskQueue.GetUsingSlot())
	require.Never(t, func() bool { return len(started) != 0 }, 50*time.Millisecond, time.Millisecond)
	unblock()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not resume after slots were released")
	}
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}

func TestPipelineHoldsTaskPermitThroughCleanup(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t)
	download, releaseDownload := pipelineGate(t)
	build, releaseBuild := pipelineGate(t)
	upload, releaseUpload := pipelineGate(t)
	cleanup, releaseCleanup := pipelineGate(t)
	downloading, building := make(chan struct{}), make(chan struct{})
	uploading, cleaning, nextDownload := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := &pipelineTestTask{
		id: "first", ctx: context.Background(),
		prepare: func() { close(downloading); <-download },
		build:   func() { close(building); <-build },
		upload:  func() { close(uploading); <-upload },
		cleanup: func() { close(cleaning); <-cleanup },
	}
	second := &pipelineTestTask{
		id: "second", ctx: context.Background(),
		prepare: func() { close(nextDownload) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, downloading)
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	for _, stage := range []struct {
		entered <-chan struct{}
		release func()
	}{
		{downloading, releaseDownload},
		{building, releaseBuild},
		{uploading, releaseUpload},
		{cleaning, releaseCleanup},
	} {
		awaitPipeline(t, stage.entered)
		require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
		select {
		case <-nextDownload:
			t.Fatal("the task permit was released before cleanup finished")
		case <-time.After(150 * time.Millisecond):
		}
		stage.release()
	}
	awaitPipeline(t, nextDownload)
	require.Eventually(t, func() bool {
		return first.GetState() == indexpb.JobState_JobStateFinished &&
			second.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
	}, 5*time.Second, time.Millisecond)
}

func TestPipelineReducedTaskLimitPreservesTaskOrder(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 128 }
	var otherSlots atomic.Int64
	otherSlots.Store(64)
	sched.SetExternalSlotUsage(otherSlots.Load)
	building := &pipelineTestTask{id: "building", ctx: context.Background()}
	require.True(t, sched.acquireSlots(building))
	queued := &pipelineTestTask{id: "queued", slots: 128, ctx: context.Background()}
	require.NoError(t, sched.TaskQueue.Enqueue(queued))
	require.Nil(t, sched.TaskQueue.PopUnissuedTask(true))
	// Reducing whole-task concurrency must not discard either task's position.
	require.Nil(t, sched.TaskQueue.PopUnissuedTask(false))
	sched.finishAdmission(building.Name())
	sched.releaseSlots(building.Name())
	require.Len(t, sched.pendingTasks, 1)
	require.Equal(t, queued.Name(), sched.pendingTasks[0].name)
	otherSlots.Store(0)
	require.Equal(t, queued, sched.TaskQueue.PopUnissuedTask(true))
	sched.finishAdmission(queued.Name())
	sched.releaseSlots(queued.Name())
	require.Empty(t, sched.pendingTasks)
}

func TestPipelineTaskConcurrencyUpdates(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t)
	started := make(chan string, 3)
	prepared := make(chan struct{}, 3)
	uploading := make(chan struct{})
	upload, releaseUpload := pipelineGate(t)
	releases := make(map[string]func())
	for _, id := range []string{"first", "second", "third"} {
		build, release := pipelineGate(t)
		releases[id] = release
		task := &pipelineTestTask{
			id: id, ctx: context.Background(),
			prepare: func() { prepared <- struct{}{} },
			build:   func() { started <- id; <-build },
		}
		if id == "first" {
			task.upload = func() { close(uploading); <-upload }
		}
		require.NoError(t, sched.TaskQueue.Enqueue(task))
		if id == "first" {
			select {
			case name := <-started:
				require.Equal(t, "first", name)
			case <-time.After(5 * time.Second):
				t.Fatal("first build did not start")
			}
		}
	}
	awaitPipeline(t, prepared)
	select {
	case <-started:
		t.Fatal("task exceeded the initial concurrency")
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, params.Save(key, "2"))
	var second string
	select {
	case second = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("increasing concurrency did not admit another task")
	}
	require.NoError(t, params.Save(key, "1"))
	releases["first"]()
	awaitPipeline(t, uploading)
	for _, value := range []string{"1", "0", "invalid"} {
		require.NoError(t, params.Save(key, value))
		select {
		case <-started:
			t.Fatal("capacity reduction or invalid update admitted another task")
		case <-time.After(150 * time.Millisecond):
		}
	}
	releases[second]()
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 64 }, 5*time.Second, time.Millisecond)
	select {
	case <-started:
		t.Fatal("upload released the task permit")
	case <-time.After(150 * time.Millisecond):
	}
	releaseUpload()
	select {
	case third := <-started:
		require.NotEqual(t, second, third)
		releases[third]()
	case <-time.After(5 * time.Second):
		t.Fatal("completed tasks did not release concurrency")
	}
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}

func TestPipelineCancellationPreservesBuildFailureAndSkipsUpload(t *testing.T) {
	for _, nativeError := range []bool{false, true} {
		t.Run(fmt.Sprintf("native-error=%t", nativeError), func(t *testing.T) {
			sched := newPipelineScheduler(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var uploads atomic.Int32
			task := &pipelineTestTask{
				id: "cancel-during-build", ctx: ctx,
				build:  cancel,
				upload: func() { uploads.Add(1) },
			}
			expected := indexpb.JobState_JobStateRetry
			if nativeError {
				task.buildErr = merr.SegcoreError(int32(merr.CodeDataFormatBroken), "corrupt index input")
				expected = indexpb.JobState_JobStateFailed
			}
			require.NoError(t, sched.TaskQueue.Enqueue(task))
			require.Eventually(t, func() bool {
				return task.GetState() == expected && sched.TaskQueue.GetUsingSlot() == 0
			}, 5*time.Second, time.Millisecond)
			require.Zero(t, uploads.Load())
		})
	}
}

func TestPipelineNonVectorTasksBypassWaitingVectors(t *testing.T) {
	for _, prepared := range []bool{true, false} {
		for _, blockedBy := range []string{"task limit", "build admission", "slots"} {
			t.Run(fmt.Sprintf("prepared=%t/%s", prepared, blockedBy), func(t *testing.T) {
				paramtable.Init()
				params := paramtable.Get()
				key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
				limit := "2"
				if blockedBy == "task limit" {
					limit = "1"
				}
				require.NoError(t, params.Save(key, limit))
				t.Cleanup(func() { _ = params.Reset(key) })
				sched := newPipelineScheduler(t, 256)
				build, releaseBuild := pipelineGate(t)
				building, waiting, nextDownload := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var notified atomic.Bool
				var admitted atomic.Bool
				first := &pipelineTestTask{
					id: "first", slots: 128, ctx: context.Background(),
					build: func() { close(building); <-build },
				}
				if blockedBy == "build admission" {
					first.reserve = func() (bool, error) {
						if notified.CompareAndSwap(false, true) {
							close(waiting)
						}
						return admitted.Load(), nil
					}
				}
				require.NoError(t, sched.TaskQueue.Enqueue(first))
				if blockedBy == "build admission" {
					awaitPipeline(t, waiting)
				} else {
					awaitPipeline(t, building)
				}
				next := &pipelineTestTask{
					id: "next", ctx: context.Background(),
					prepare: func() { close(nextDownload) },
				}
				if blockedBy == "slots" {
					next.slots = 192
				}
				require.NoError(t, sched.TaskQueue.Enqueue(next))
				// Let the slot-blocked vector enter the pending admission list.
				if blockedBy == "slots" {
					require.Eventually(t, func() bool {
						sched.slotMu.Lock()
						defer sched.slotMu.Unlock()
						return len(sched.pendingTasks) == 1
					}, 5*time.Second, time.Millisecond)
				}
				finished := make(chan struct{})
				var task Task = &scalarPipelineTestTask{&pipelineTestTask{
					id: "non-vector", ctx: context.Background(),
					build: func() { close(finished) },
				}}
				if !prepared {
					// Stats/analyze expose Task, without the PreparedTask methods.
					task = struct{ Task }{task}
				}
				require.NoError(t, sched.TaskQueue.Enqueue(task))
				awaitPipeline(t, finished)
				if blockedBy == "task limit" || blockedBy == "slots" {
					select {
					case <-nextDownload:
						t.Fatal("blocked vector started downloading")
					default:
					}
				}
				admitted.Store(true)
				releaseBuild()
				awaitPipeline(t, nextDownload)
			})
		}
	}
}

func TestPipelineNonVectorBypassStillRequiresSlots(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	t.Cleanup(sched.Close)
	sched.totalSlots = func() int64 { return 128 }
	running := &pipelineTestTask{id: "running", slots: 128, ctx: context.Background()}
	require.True(t, sched.acquireSlots(running))
	vector := &pipelineTestTask{id: "vector", slots: 64, ctx: context.Background()}
	scalar := &scalarPipelineTestTask{&pipelineTestTask{
		id: "scalar", slots: 64, ctx: context.Background(),
	}}
	require.NoError(t, sched.TaskQueue.Enqueue(vector))
	require.NoError(t, sched.TaskQueue.Enqueue(scalar))
	require.Nil(t, sched.TaskQueue.PopUnissuedTask(false))
	require.EqualValues(t, 128, sched.TaskQueue.GetUsingSlot())
	sched.finishAdmission(running.Name())
	sched.releaseSlots(running.Name())
	require.Same(t, scalar, sched.TaskQueue.PopUnissuedTask(false))
	require.True(t, sched.isBuildTurn(scalar))
	require.Empty(t, sched.pendingTasks)
	require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
	sched.releaseSlots(scalar.Name())
	require.Same(t, vector, sched.TaskQueue.PopUnissuedTask(true))
	sched.finishAdmission(vector.Name())
	sched.releaseSlots(vector.Name())
}

func TestPipelineCancelsQueuedTaskWithoutWaitingForTaskPermit(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t)
	build, releaseBuild := pipelineGate(t)
	building := make(chan struct{})
	first := &pipelineTestTask{
		id: "busy", ctx: context.Background(),
		build: func() { close(building); <-build },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, building)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	prepared, cleaned, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
	second := &pipelineTestTask{
		id: "cancel-queued", ctx: ctx,
		prepare: func() { close(prepared) },
		build:   func() { close(built) },
		cleanup: func() { close(cleaned) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	cancel()
	awaitPipeline(t, cleaned)
	select {
	case <-prepared:
		t.Fatal("queued task started downloading without a permit")
	default:
	}
	require.Eventually(t, func() bool {
		return sched.TaskQueue.GetUsingSlot() == 64 && second.GetState() == indexpb.JobState_JobStateRetry
	}, 5*time.Second, time.Millisecond)
	select {
	case <-built:
		t.Fatal("canceled task started building")
	default:
	}
	releaseBuild()
}

func TestPipelineCloseDrainsTasksAndRejectsNewTasks(t *testing.T) {
	sched := newPipelineScheduler(t)
	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
			id: id, ctx: context.Background(),
		}))
	}
	sched.Close()
	sched.Wait()
	used, queued := sched.TaskQueue.GetSlotUsage()
	require.Zero(t, used)
	require.Zero(t, queued)
	require.ErrorIs(t, sched.TaskQueue.Enqueue(&pipelineTestTask{id: "late", ctx: context.Background()}), errCancel)
}

func TestPipelineRetainsSlotsThroughNativeCleanup(t *testing.T) {
	sched := newPipelineScheduler(t)
	cleanup, release := pipelineGate(t)
	cleaning := make(chan struct{})
	task := &pipelineTestTask{
		id: "reserved", ctx: context.Background(),
		cleanup: func() { close(cleaning); <-cleanup },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(task))
	awaitPipeline(t, cleaning)
	require.EqualValues(t, task.GetSlot(), sched.TaskQueue.GetUsingSlot())
	release()
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}

func TestPipelineCloseRetainsRunningTaskResources(t *testing.T) {
	for _, stage := range []string{"download", "build", "upload", "cleanup", "unprepared"} {
		t.Run(stage, func(t *testing.T) {
			sched := newPipelineScheduler(t)
			gate, release := pipelineGate(t)
			entered := make(chan struct{})
			var cleaned atomic.Int32
			block := func() { close(entered); <-gate }
			task := &pipelineTestTask{
				id: stage, ctx: context.Background(),
				cleanup: func() { cleaned.Add(1) },
			}
			switch stage {
			case "download":
				task.prepare = block
			case "build", "unprepared":
				task.build = block
			case "upload":
				task.upload = block
			case "cleanup":
				task.cleanup = func() { block(); cleaned.Add(1) }
			}
			var queued Task = task
			if stage == "unprepared" {
				queued = struct{ Task }{task}
			}
			require.NoError(t, sched.TaskQueue.Enqueue(queued))
			awaitPipeline(t, entered)
			closed := make(chan struct{})
			go func() { sched.Close(); close(closed) }()
			awaitPipeline(t, closed)
			require.Zero(t, cleaned.Load())
			require.Equal(t, task.GetSlot(), sched.TaskQueue.GetUsingSlot())
			require.ErrorIs(t, sched.TaskQueue.Enqueue(&pipelineTestTask{id: "late", ctx: context.Background()}), errCancel)
			drained := make(chan struct{})
			go func() { sched.Wait(); close(drained) }()
			select {
			case <-drained:
				t.Fatal("resources were released while the task was still running")
			default:
			}
			release()
			awaitPipeline(t, drained)
			require.EqualValues(t, 1, cleaned.Load())
			require.Zero(t, sched.TaskQueue.GetUsingSlot())
		})
	}
}
