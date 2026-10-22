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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPipelineResidentInputAdmission(t *testing.T) {
	for _, mode := range []string{"during-spill", "at-completion", "after-release", "reservation-error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			sched := newPipelineScheduler(t, 1024)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var resourcesReady, failReservation, spilled atomic.Bool
			var releases atomic.Int32
			tried, writing, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
			allowSpill, completeSpill := pipelineGate(t)
			var triedOnce sync.Once
			task := &pipelineTestTask{
				id: mode, ctx: ctx, residentInput: true,
			}
			task.reserve = func() (bool, error) {
				triedOnce.Do(func() { close(tried) })
				if failReservation.Load() {
					return false, errors.New("reservation failed")
				}
				return resourcesReady.Load(), nil
			}
			task.spill = func(ctx context.Context) (bool, error) {
				close(writing)
				select {
				case <-ctx.Done():
					return false, nil
				case <-allowSpill:
					if mode == "at-completion" {
						resourcesReady.Store(true)
					}
					return true, nil
				}
			}
			task.releaseInputMemory = func() error {
				releases.Add(1)
				task.residentInput = false
				return nil
			}
			task.build = func() {
				spilled.Store(!task.residentInput)
				close(built)
			}
			require.NoError(t, sched.TaskQueue.Enqueue(task))
			awaitPipeline(t, tried)
			require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
			// A busy GPU starts spilling even while host slots are available.
			awaitPipeline(t, writing)
			switch mode {
			case "during-spill":
				resourcesReady.Store(true)
			case "at-completion":
				completeSpill()
			case "after-release":
				completeSpill()
				require.Eventually(t, func() bool {
					return releases.Load() == 1 && sched.TaskQueue.GetUsingSlot() == 0
				}, 5*time.Second, time.Millisecond)
				resourcesReady.Store(true)
			case "reservation-error":
				failReservation.Store(true)
			case "cancel":
				cancel()
			}
			if mode == "reservation-error" || mode == "cancel" {
				require.Eventually(t, func() bool {
					return task.GetState() == indexpb.JobState_JobStateRetry &&
						sched.TaskQueue.GetUsingSlot() == 0
				}, 5*time.Second, time.Millisecond)
				select {
				case <-built:
					t.Fatal("failed or canceled task started building")
				default:
				}
				require.Zero(t, releases.Load())
			} else {
				awaitPipeline(t, built)
				require.Equal(t, mode == "after-release", spilled.Load())
				require.Eventually(t, func() bool {
					return task.GetState() == indexpb.JobState_JobStateFinished &&
						sched.TaskQueue.GetUsingSlot() == 0
				}, 5*time.Second, time.Millisecond)
				if mode != "after-release" {
					require.Zero(t, releases.Load())
				}
			}
		})
	}
}

func TestPipelineSpillRetainsTaskPermit(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "2"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t, 1024)
	buildGate, finishBuild := pipelineGate(t)
	firstBuilding, writing, built, downloading := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var released, reserved atomic.Bool
	first := &pipelineTestTask{
		id: "running", ctx: context.Background(), residentInput: true,
		build: func() { close(firstBuilding); <-buildGate },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, firstBuilding)
	second := &pipelineTestTask{
		id: "waiting", ctx: context.Background(), residentInput: true,
		reserve: func() (bool, error) { return reserved.Load(), nil },
		spill:   func(context.Context) (bool, error) { close(writing); return true, nil },
		build:   func() { close(built) },
	}
	second.releaseInputMemory = func() error {
		second.residentInput = false
		released.Store(true)
		return nil
	}
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	awaitPipeline(t, writing)
	require.Eventually(t, func() bool {
		return released.Load() && sched.TaskQueue.GetUsingSlot() == 64
	}, 5*time.Second, time.Millisecond)
	third := &pipelineTestTask{
		id: "queued", ctx: context.Background(),
		prepare: func() { close(downloading) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(third))
	select {
	case <-downloading:
		t.Fatal("spill released the task permit")
	case <-time.After(150 * time.Millisecond):
	}
	finishBuild()
	select {
	case <-downloading:
		t.Fatal("later download bypassed the waiting build after a task permit was released")
	case <-time.After(150 * time.Millisecond):
	}
	reserved.Store(true)
	sched.buildWakeup <- struct{}{}
	awaitPipeline(t, built)
	awaitPipeline(t, downloading)
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}

type pipelineTestTask struct {
	id                 string
	slots              int64
	ctx                context.Context
	state              atomic.Int32
	prepare            func()
	build              func()
	upload             func()
	cleanup            func()
	residentInput      bool
	reserve            func() (bool, error)
	spill              func(context.Context) (bool, error)
	releaseInputMemory func() error
	prepareErr         error
	buildErr           error
	uploadErr          error
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
func (t *pipelineTestTask) HasResidentInput() bool           { return t.residentInput }
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

func (t *pipelineTestTask) SpillInput(ctx context.Context) (bool, error) {
	if t.spill != nil {
		return t.spill(ctx)
	}
	return false, nil
}

func (t *pipelineTestTask) ReleaseInputMemory() error {
	if t.releaseInputMemory != nil {
		return t.releaseInputMemory()
	}
	t.residentInput = false
	return nil
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

func TestPipelineTaskOrderSurvivesSlotRelease(t *testing.T) {
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
			nextBuilding, nextReleased := make(chan struct{}), make(chan struct{})
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
				id: "second", ctx: context.Background(), residentInput: true,
				prepare: func() { close(nextDownloaded) },
				reserve: func() (bool, error) {
					nextAttempts.Add(1)
					admitted <- "second"
					return true, nil
				},
				spill: func(context.Context) (bool, error) { return true, nil },
				build: func() { close(nextBuilding) },
			}
			second.releaseInputMemory = func() error {
				second.residentInput = false
				close(nextReleased)
				return nil
			}
			require.NoError(t, sched.TaskQueue.Enqueue(first))
			awaitPipeline(t, downloading)
			require.NoError(t, sched.TaskQueue.Enqueue(second))
			awaitPipeline(t, nextDownloaded)
			awaitPipeline(t, nextReleased)
			require.Zero(t, nextAttempts.Load(), "download completion must not reorder builds")
			releaseDownload()
			require.Eventually(t, func() bool {
				return attempts.Load() >= 2 && sched.TaskQueue.GetUsingSlot() == 0
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

func TestPipelineReadyTaskDoesNotHoldSlots(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 64 }
	var otherSlots atomic.Int64
	sched.SetExternalSlotUsage(otherSlots.Load)
	require.NoError(t, sched.Start())
	t.Cleanup(func() {
		sched.Close()
		sched.Wait()
	})
	download, releaseDownload := pipelineGate(t)
	downloading, building, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	task := &pipelineTestTask{
		id: "waiting", ctx: ctx,
		prepare: func() { close(downloading); <-download },
		build:   func() { close(building) },
		cleanup: func() { close(cleaned) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(task))
	awaitPipeline(t, downloading)
	// Force a wait at the stage boundary; the staged file must not retain slots.
	otherSlots.Store(64)
	releaseDownload()
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
	select {
	case <-building:
		t.Fatal("build started without slots")
	default:
	}
	cancel()
	awaitPipeline(t, cleaned)
	require.Eventually(t, func() bool { return task.GetState() == indexpb.JobState_JobStateRetry }, 5*time.Second, time.Millisecond)
	require.Zero(t, sched.TaskQueue.GetUsingSlot())
}

func TestPipelineWaitingBuildIsNotOvertaken(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "cancel"}[cancelFirst], func(t *testing.T) {
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
			downloading, building, nextDownload := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			first := &pipelineTestTask{
				id: "large", slots: 128, ctx: ctx,
				prepare: func() { close(downloading); <-download },
				build:   func() { close(building); <-build },
			}
			require.NoError(t, sched.TaskQueue.Enqueue(first))
			awaitPipeline(t, downloading)
			otherSlots.Store(64)
			releaseDownload()
			require.Eventually(t, func() bool {
				sched.slotMu.Lock()
				defer sched.slotMu.Unlock()
				return len(sched.pendingTasks) == 1 && sched.pendingTasks[0].name == first.Name()
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
				otherSlots.Store(0)
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
	held := &pipelineTestTask{id: "resident", ctx: context.Background()}
	waiting := &pipelineTestTask{id: "large", slots: 128, ctx: context.Background()}
	require.True(t, sched.acquireSlots(held))
	require.False(t, sched.acquireSlots(waiting))
	// Releasing resources retains the task's position until build admission.
	require.True(t, sched.acquireSlots(held))
	sched.releaseSlots(held.Name())
	require.False(t, sched.acquireSlots(waiting))
	require.True(t, sched.acquireSlots(held))
	sched.finishAdmission(held.Name())
	sched.releaseSlots(held.Name())
	require.True(t, sched.acquireSlots(waiting))
	sched.finishAdmission(waiting.Name())
	sched.releaseSlots(waiting.Name())
	require.Zero(t, sched.TaskQueue.GetUsingSlot())
	require.Empty(t, sched.pendingTasks)
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
			failed := &pipelineTestTask{id: "failed", ctx: context.Background()}
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
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("download was not admitted")
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
	for _, resident := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk", true: "memory"}[resident], func(t *testing.T) {
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
				id: "first", ctx: context.Background(), residentInput: resident,
				prepare: func() { close(downloading); <-download },
				build:   func() { close(building); <-build },
				upload:  func() { close(uploading); <-upload },
				cleanup: func() { close(cleaning); <-cleanup },
			}
			second := &pipelineTestTask{
				id: "second", ctx: context.Background(), residentInput: resident,
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
		})
	}
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
	sched.releaseSlots(building.Name())
	queued := &pipelineTestTask{id: "queued", slots: 128, ctx: context.Background()}
	require.NoError(t, sched.TaskQueue.Enqueue(queued))
	require.Nil(t, sched.TaskQueue.PopUnissuedTask(true))
	// Reducing whole-task concurrency must not discard either task's position.
	require.Nil(t, sched.TaskQueue.PopUnissuedTask(false))
	require.True(t, sched.acquireSlots(building))
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
	for _, resident := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk", true: "memory"}[resident], func(t *testing.T) {
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
					id: id, ctx: context.Background(), residentInput: resident,
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
		})
	}
}

func TestPipelineTaskLimitPreservesQueueOrder(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t)
	build, releaseBuild := pipelineGate(t)
	building, downloading, scalarBuilt := make(chan struct{}), make(chan struct{}), make(chan struct{})
	require.NoError(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
		id: "running", ctx: context.Background(),
		build: func() { close(building); <-build },
	}))
	awaitPipeline(t, building)
	require.NoError(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
		id: "queued", ctx: context.Background(),
		prepare: func() { close(downloading) },
	}))
	require.NoError(t, sched.TaskQueue.Enqueue(&scalarPipelineTestTask{&pipelineTestTask{
		id: "scalar", ctx: context.Background(),
		build: func() { close(scalarBuilt) },
	}}))
	select {
	case <-scalarBuilt:
		t.Fatal("scalar task bypassed the queued vector task")
	case <-downloading:
		t.Fatal("vector task started downloading without a permit")
	case <-time.After(150 * time.Millisecond):
	}
	releaseBuild()
	awaitPipeline(t, downloading)
	awaitPipeline(t, scalarBuilt)
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
	require.Zero(t, sched.TaskQueue.GetUsingSlot())
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
	for _, stage := range []string{"download", "build", "upload", "spill", "cleanup", "unprepared"} {
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
			case "spill":
				task.residentInput = true
				task.reserve = func() (bool, error) { return false, nil }
				task.spill = func(context.Context) (bool, error) { block(); return false, nil }
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
