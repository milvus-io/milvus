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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPipelineGPULaneSelection(t *testing.T) {
	for _, tc := range []struct {
		indexType string
		gpuBuild  string
		wantGPU   bool
	}{
		{"HNSW", "", false},
		{"HNSW", "true", vecindexmgr.VectorBuildUsesGPU("HNSW", "true")},
		{"HNSW", "false", false},
		{"HNSW", "invalid", false},
		{"IVF_FLAT", "true", false},
		{"INVERTED", "true", false},
		{"SPARSE_INVERTED_INDEX", "", false},
	} {
		t.Run(tc.indexType+"/"+tc.gpuBuild, func(t *testing.T) {
			task := &indexBuildTask{
				req:            &workerpb.CreateJobRequest{IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: tc.indexType}, {Key: "gpu_build", Value: tc.gpuBuild}}},
				newIndexParams: map[string]string{"index_type": tc.indexType, "gpu_build": tc.gpuBuild},
			}
			require.Equal(t, tc.wantGPU, task.UsesGPU())
		})
	}
}

func TestPipelineResidentInputAdmission(t *testing.T) {
	for _, mode := range []string{"during-spill", "at-completion", "after-release", "reservation-error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			sched := newPipelineScheduler(t, 1024)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var gpuReady, failReservation, spilled atomic.Bool
			var releases atomic.Int32
			tried, writing, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
			allowSpill, completeSpill := pipelineGate(t)
			var triedOnce sync.Once
			task := &pipelineTestTask{
				id: mode, ctx: ctx, gpu: true, residentInput: true,
			}
			task.reserve = func() (bool, error) {
				triedOnce.Do(func() { close(tried) })
				if failReservation.Load() {
					return false, errors.New("reservation failed")
				}
				return gpuReady.Load(), nil
			}
			task.spill = func(ctx context.Context) (bool, error) {
				close(writing)
				select {
				case <-ctx.Done():
					return false, nil
				case <-allowSpill:
					if mode == "at-completion" {
						gpuReady.Store(true)
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
				gpuReady.Store(true)
			case "at-completion":
				completeSpill()
			case "after-release":
				completeSpill()
				require.Eventually(t, func() bool {
					return releases.Load() == 1 && sched.TaskQueue.GetUsingSlot() == 0
				}, 5*time.Second, time.Millisecond)
				gpuReady.Store(true)
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

func TestPipelineCPUWaitSpillsWithoutSlotPressure(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t, 1024)
	buildGate, finishBuild := pipelineGate(t)
	firstBuilding, writing, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var released atomic.Bool
	first := &pipelineTestTask{
		id: "cpu-running", ctx: context.Background(), residentInput: true,
		build: func() { close(firstBuilding); <-buildGate },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, firstBuilding)
	second := &pipelineTestTask{
		id: "cpu-waiting", ctx: context.Background(), residentInput: true,
		spill: func(context.Context) (bool, error) { close(writing); return true, nil },
		build: func() { close(built) },
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
	finishBuild()
	awaitPipeline(t, built)
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}

type pipelineTestTask struct {
	id                 string
	ctx                context.Context
	gpu                bool
	state              atomic.Int32
	prepare            func()
	build              func()
	upload             func()
	cleanup            func()
	downloadBytes      uint64
	plan               func() (uint64, error)
	residentInput      bool
	reserve            func() (bool, error)
	spill              func(context.Context) (bool, error)
	releaseInputMemory func() error
	prepareErr         error
	buildErr           error
	uploadErr          error
}

func (t *pipelineTestTask) PlanDownload(context.Context) (uint64, error) {
	if t.plan != nil {
		return t.plan()
	}
	return t.downloadBytes, nil
}

func (t *pipelineTestTask) Ctx() context.Context             { return t.ctx }
func (t *pipelineTestTask) Name() string                     { return t.id }
func (t *pipelineTestTask) GetSlot() int64                   { return 64 }
func (t *pipelineTestTask) IsVectorIndex() bool              { return true }
func (t *pipelineTestTask) UsesGPU() bool                    { return t.gpu }
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
	t.Cleanup(sched.Close)
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
			id: id, ctx: context.Background(), gpu: true,
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

func TestPipelineBuildTakesSlotsBeforeNextDownload(t *testing.T) {
	sched := newPipelineScheduler(t, 64)
	download, releaseDownload := pipelineGate(t)
	upload, releaseUpload := pipelineGate(t)
	cleanup, releaseCleanup := pipelineGate(t)
	downloading, building, uploading, cleaning, nextDownload := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := &pipelineTestTask{
		id: "first", ctx: context.Background(), gpu: true,
		prepare: func() { close(downloading); <-download },
		build: func() {
			close(building)
		},
		upload:  func() { close(uploading); <-upload },
		cleanup: func() { close(cleaning); <-cleanup },
	}
	second := &pipelineTestTask{
		id: "second", ctx: context.Background(), gpu: true,
		prepare: func() { close(nextDownload) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, downloading)
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
	releaseDownload()
	awaitPipeline(t, building)
	awaitPipeline(t, uploading)
	select {
	case <-nextDownload:
		t.Fatal("download took the build stage's slots")
	default:
	}
	releaseUpload()
	awaitPipeline(t, cleaning)
	require.EqualValues(t, 64, sched.TaskQueue.GetUsingSlot())
	releaseCleanup()
	awaitPipeline(t, nextDownload)
	require.Eventually(t, func() bool {
		return second.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
	}, 5*time.Second, time.Millisecond)
}

func TestPipelineReadyTaskDoesNotHoldSlots(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 64 }
	var otherSlots atomic.Int64
	sched.SetExternalSlotUsage(otherSlots.Load)
	require.NoError(t, sched.Start())
	t.Cleanup(sched.Close)
	download, releaseDownload := pipelineGate(t)
	downloading, building, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	task := &pipelineTestTask{
		id: "waiting", ctx: ctx, gpu: true,
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

func TestPipelineSharesExistingNodeSlots(t *testing.T) {
	paramtable.Init()
	sched := NewTaskScheduler(context.Background())
	sched.totalSlots = func() int64 { return 128 }
	var otherSlots int64
	sched.SetExternalSlotUsage(func() int64 { return otherSlots })
	t.Cleanup(sched.Close)
	task := &pipelineTestTask{id: "index", ctx: context.Background(), gpu: true}
	require.True(t, sched.acquireSlots(task))
	require.NoError(t, sched.AdmitTask(64, func(err error) error {
		if err != nil {
			return err
		}
		otherSlots += 64
		return nil
	}))
	require.Error(t, sched.AdmitTask(1, func(err error) error { require.Error(t, err); return err }))
	// A duplicate request does not need capacity and preserves the owner's result.
	require.NoError(t, sched.AdmitTask(64, func(err error) error {
		require.Error(t, err)
		return nil
	}))
	sched.releaseSlots(task)
	require.EqualValues(t, 64, otherSlots)
	otherSlots = 0
	// Preserve the existing ability to run an oversized task alone.
	require.NoError(t, sched.AdmitTask(256, func(err error) error {
		if err != nil {
			return err
		}
		otherSlots = 256
		return nil
	}))
	require.False(t, sched.acquireSlots(task))
}

func TestPipelinePlanningDoesNotBlockBuild(t *testing.T) {
	sched := newPipelineScheduler(t)
	download, releaseDownload := pipelineGate(t)
	plan, releasePlan := pipelineGate(t)
	downloading, planning, building := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := &pipelineTestTask{
		id: "first", ctx: context.Background(), gpu: true,
		prepare: func() { close(downloading); <-download },
		build:   func() { close(building) },
	}
	second := &pipelineTestTask{
		id: "second", ctx: context.Background(), gpu: true,
		plan: func() (uint64, error) { close(planning); <-plan; return 0, nil },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, downloading)
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	awaitPipeline(t, planning)
	releaseDownload()
	awaitPipeline(t, building)
	releasePlan()
}

func TestPipelineStageFailureReleasesSlots(t *testing.T) {
	for _, phase := range []string{"prepare", "build", "upload"} {
		t.Run(phase, func(t *testing.T) {
			sched := newPipelineScheduler(t, 64)
			failure := errors.New("stage failed")
			failed := &pipelineTestTask{id: "failed", ctx: context.Background(), gpu: true}
			switch phase {
			case "prepare":
				failed.prepareErr = failure
			case "build":
				failed.buildErr = failure
			case "upload":
				failed.uploadErr = failure
			}
			next := &pipelineTestTask{id: "next", ctx: context.Background(), gpu: true}
			require.NoError(t, sched.TaskQueue.Enqueue(failed))
			require.NoError(t, sched.TaskQueue.Enqueue(next))
			require.Eventually(t, func() bool {
				return failed.GetState() == indexpb.JobState_JobStateRetry &&
					next.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
			}, 5*time.Second, time.Millisecond)
		})
	}
}

func TestPipelineDownloadByteWindow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		first   uint64
		second  uint64
		initial int
	}{
		{"crossing segment is admitted", 400 << 20, 2 << 30, 2},
		{"full window blocks next segment", 512 << 20, 8 << 20, 1},
		{"large first segment blocks next segment", 2 << 30, 8 << 20, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sched := newPipelineScheduler(t)
			started := make(chan string, 3)
			first, releaseFirst := pipelineGate(t)
			second, releaseSecond := pipelineGate(t)
			for _, task := range []*pipelineTestTask{
				{
					id: "first", ctx: context.Background(), downloadBytes: tc.first,
					prepare: func() { started <- "first"; <-first },
				},
				{
					id: "second", ctx: context.Background(), downloadBytes: tc.second,
					prepare: func() { started <- "second"; <-second },
				},
				{
					id: "third", ctx: context.Background(), downloadBytes: 8 << 20,
					prepare: func() { started <- "third" },
				},
			} {
				require.NoError(t, sched.TaskQueue.Enqueue(task))
			}
			for range tc.initial {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("download was not admitted")
				}
			}
			require.Never(t, func() bool { return len(started) != 0 }, 50*time.Millisecond, time.Millisecond)
			releaseFirst()
			if tc.initial == 2 {
				// The second segment still occupies more than the full window.
				require.Never(t, func() bool { return len(started) != 0 }, 50*time.Millisecond, time.Millisecond)
			}
			releaseSecond()
			require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
		})
	}
}

func TestPipelineDownloadPlanFailureReleasesTask(t *testing.T) {
	sched := newPipelineScheduler(t)
	failed := &pipelineTestTask{
		id: "failed", ctx: context.Background(),
		plan:    func() (uint64, error) { return 0, errors.New("metadata unavailable") },
		prepare: func() { t.Error("failed plan must not prepare") },
	}
	next := &pipelineTestTask{id: "next", ctx: context.Background(), downloadBytes: 512 << 20}
	require.NoError(t, sched.TaskQueue.Enqueue(failed))
	require.NoError(t, sched.TaskQueue.Enqueue(next))
	require.Eventually(t, func() bool {
		return next.GetState() == indexpb.JobState_JobStateFinished && sched.TaskQueue.GetUsingSlot() == 0
	}, 5*time.Second, time.Millisecond)
	require.NotEqual(t, indexpb.JobState_JobStateFinished, failed.GetState())
	require.Equal(t, indexpb.JobState_JobStateFinished, next.GetState())
}

func TestPipelineReleasesBuildLaneAndHoldsSlotsThroughCleanup(t *testing.T) {
	for _, gpu := range []bool{false, true} {
		t.Run(map[bool]string{false: "cpu", true: "gpu"}[gpu], func(t *testing.T) {
			paramtable.Init()
			params := paramtable.Get()
			key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
			require.NoError(t, params.Save(key, "1"))
			t.Cleanup(func() { _ = params.Reset(key) })
			sched := newPipelineScheduler(t)
			download1, releaseDownload1 := pipelineGate(t)
			download2, releaseDownload2 := pipelineGate(t)
			build1, releaseBuild1 := pipelineGate(t)
			build2, releaseBuild2 := pipelineGate(t)
			upload, releaseUpload := pipelineGate(t)
			cleanup, releaseCleanup := pipelineGate(t)
			started1, started2 := make(chan struct{}), make(chan struct{})
			building1 := make(chan struct{})
			uploading, cleaning, building2 := make(chan struct{}), make(chan struct{}), make(chan struct{})
			first := &pipelineTestTask{
				id: "first", ctx: context.Background(), gpu: gpu,
				prepare: func() { close(started1); <-download1 },
				build:   func() { close(building1); <-build1 },
				upload:  func() { close(uploading); <-upload },
				cleanup: func() { close(cleaning); <-cleanup },
			}
			second := &pipelineTestTask{
				id: "second", ctx: context.Background(), gpu: gpu,
				prepare: func() { close(started2); <-download2 },
				build:   func() { close(building2); <-build2 },
			}
			require.NoError(t, sched.TaskQueue.Enqueue(first))
			require.NoError(t, sched.TaskQueue.Enqueue(second))
			awaitPipeline(t, started1)
			awaitPipeline(t, started2)
			require.EqualValues(t, 128, sched.TaskQueue.GetUsingSlot())
			releaseDownload1()
			awaitPipeline(t, building1)
			awaitPipeline(t, started2)
			releaseDownload2()

			select {
			case <-building2:
				t.Fatal("second task exceeded the configured build concurrency")
			case <-time.After(100 * time.Millisecond):
			}
			releaseBuild1()
			awaitPipeline(t, uploading)
			awaitPipeline(t, building2)
			require.GreaterOrEqual(t, sched.TaskQueue.GetUsingSlot(), int64(128))
			releaseUpload()
			awaitPipeline(t, cleaning)
			require.GreaterOrEqual(t, sched.TaskQueue.GetUsingSlot(), int64(128))
			releaseCleanup()
			releaseBuild2()
			require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
			require.Equal(t, indexpb.JobState_JobStateFinished, first.GetState())
			require.Equal(t, indexpb.JobState_JobStateFinished, second.GetState())
		})
	}
}

func TestPipelineBuildConcurrencyUpdates(t *testing.T) {
	for _, gpu := range []bool{false, true} {
		t.Run(map[bool]string{false: "cpu", true: "gpu"}[gpu], func(t *testing.T) {
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
					id: id, ctx: context.Background(), gpu: gpu,
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
			for range 3 {
				awaitPipeline(t, prepared)
			}
			select {
			case <-started:
				t.Fatal("build exceeded the initial concurrency")
			case <-time.After(150 * time.Millisecond):
			}
			require.NoError(t, params.Save(key, "2"))
			var second string
			select {
			case second = <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("increasing concurrency did not admit another build")
			}
			require.NoError(t, params.Save(key, "1"))
			releases["first"]()
			awaitPipeline(t, uploading)
			for _, value := range []string{"1", "0", "invalid"} {
				require.NoError(t, params.Save(key, value))
				select {
				case <-started:
					t.Fatal("capacity reduction or invalid update admitted another build")
				case <-time.After(150 * time.Millisecond):
				}
			}
			releases[second]()
			select {
			case third := <-started:
				require.NotEqual(t, second, third)
				releases[third]()
			case <-time.After(5 * time.Second):
				t.Fatal("completed builds did not release concurrency while upload was pending")
			}
			releaseUpload()
			require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
		})
	}
}

func TestPipelineCancelsReadyTaskWithoutWaitingForCPULane(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.DataNodeCfg.MaxVecIndexBuildConcurrency.Key
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { _ = params.Reset(key) })
	sched := newPipelineScheduler(t)
	build, releaseBuild := pipelineGate(t)
	building := make(chan struct{})
	first := &pipelineTestTask{
		id: "busy", ctx: context.Background(), gpu: false,
		build: func() { close(building); <-build },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(first))
	awaitPipeline(t, building)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	prepared, cleaned, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
	second := &pipelineTestTask{
		id: "cancel-ready", ctx: ctx, gpu: false,
		prepare: func() { close(prepared) },
		build:   func() { close(built) },
		cleanup: func() { close(cleaned) },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(second))
	awaitPipeline(t, prepared)
	cancel()
	awaitPipeline(t, cleaned)
	require.Eventually(t, func() bool {
		return sched.TaskQueue.GetUsingSlot() == 64 && second.GetState() == indexpb.JobState_JobStateRetry
	}, 5*time.Second, time.Millisecond)
	select {
	case <-built:
		t.Fatal("canceled task entered the CPU build stage")
	default:
	}
	releaseBuild()
}

func TestPipelineCloseDrainsTasksAndRejectsNewTasks(t *testing.T) {
	sched := newPipelineScheduler(t)
	for i, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
			id: id, ctx: context.Background(), gpu: i%2 == 0,
		}))
	}
	sched.Close()
	require.Zero(t, sched.TaskQueue.GetUsingSlot())
	require.ErrorIs(t, sched.TaskQueue.Enqueue(&pipelineTestTask{id: "late", ctx: context.Background()}), errCancel)
}

func TestPipelineRetainsSlotsThroughNativeCleanup(t *testing.T) {
	sched := newPipelineScheduler(t)
	cleanup, release := pipelineGate(t)
	cleaning := make(chan struct{})
	task := &pipelineTestTask{
		id: "reserved", ctx: context.Background(), gpu: true,
		cleanup: func() { close(cleaning); <-cleanup },
	}
	require.NoError(t, sched.TaskQueue.Enqueue(task))
	awaitPipeline(t, cleaning)
	require.EqualValues(t, task.GetSlot(), sched.TaskQueue.GetUsingSlot())
	release()
	require.Eventually(t, func() bool { return sched.TaskQueue.GetUsingSlot() == 0 }, 5*time.Second, time.Millisecond)
}
