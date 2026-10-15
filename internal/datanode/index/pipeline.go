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
	"math"
	"slices"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (sched *TaskScheduler) pipelineLoop() {
	defer sched.wg.Done()
	type download struct {
		exec  *taskExecution
		bytes uint64
		err   error
	}
	type build struct {
		exec          *taskExecution
		reserved      bool
		stopSpill     context.CancelFunc
		spillDone     bool
		spillComplete bool
		err           error
	}
	type eventKind int
	const (
		downloadFinished eventKind = iota
		spillFinished
		buildFinished
		taskFinished
	)
	type completion struct {
		kind          eventKind
		download      *download
		spill         *build
		spillComplete bool
		err           error
		vector        bool
	}
	storageConfig := getIndexBuildStorageConfig()
	window := storageConfig.Concurrency * storageConfig.RequestSize
	ready := make(chan download)
	// Metadata planning can wait on S3; it must not block stage completions.
	go func(planned chan<- download) {
		defer close(planned)
		for t := range sched.downloadTasks {
			d := download{exec: sched.beginTask(t)}
			d.err = sched.runStep(t, t.PreExecute)
			if d.err == nil {
				d.err = sched.runStep(t, func(ctx context.Context) error {
					var err error
					d.bytes, err = t.(PreparedTask).PlanDownload(ctx)
					return err
				})
			}
			planned <- d
		}
	}(ready)
	canceled := sched.ctx.Done()
	completed := make(chan completion, cap(sched.downloadTasks))
	// Compaction/import release their existing counters independently.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var downloads []download
	var builds []*build
	var inflight uint64
	running, buildingVectors := 0, 0
	buildConcurrency := max(1, paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency.GetAsInt())

	consume := func(done completion) {
		switch done.kind {
		case downloadFinished:
			running--
			d := done.download
			inflight -= d.bytes
			if done.err != nil {
				sched.finishTask(d.exec, done.err)
			} else {
				builds = append(builds, &build{exec: d.exec})
				if sched.usesStageSlots(d.exec.task) && !d.exec.task.(PreparedTask).HasResidentInput() {
					sched.releaseSlots(d.exec.task)
				}
			}
		case spillFinished:
			done.spill.stopSpill()
			done.spill.stopSpill = nil
			done.spill.spillDone = true
			done.spill.spillComplete = done.spillComplete
			if done.spill.err == nil {
				done.spill.err = done.err
			}
		case buildFinished:
			if done.vector {
				buildingVectors--
			}
		case taskFinished:
			running--
		}
	}

	for ready != nil || len(downloads) != 0 || len(builds) != 0 || running != 0 {
		// Publish completed downloads before considering any new download.
		for len(completed) != 0 {
			consume(<-completed)
		}
		// Apply valid capacity updates to new builds; running builds finish normally.
		if limit := paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency.GetAsInt(); limit > 0 {
			buildConcurrency = limit
		}
		buildWaiting := false
		for i := 0; i < len(builds); {
			b := builds[i]
			exec := b.exec
			t := exec.task
			prepared := t.(PreparedTask)
			isCanceled := sched.ctx.Err() != nil || t.Ctx().Err() != nil
			if (isCanceled || b.err != nil) && b.stopSpill != nil {
				b.stopSpill()
				i++
				continue
			}
			if !isCanceled && b.err == nil {
				admitted := !t.IsVectorIndex() || buildingVectors < buildConcurrency
				if admitted && !sched.acquireSlots(t) {
					admitted = false
					buildWaiting = buildWaiting || sched.usesStageSlots(t)
				}
				if admitted && exec.usesGPU && !b.reserved {
					b.reserved, b.err = prepared.TryReserveBuild()
					admitted = b.reserved
				}
				if !admitted && b.err == nil {
					// Recheck admission before discarding a completed spill's RAM copy.
					if b.spillComplete {
						b.err = prepared.ReleaseInputMemory()
						b.spillComplete = false
					}
					if sched.usesStageSlots(t) && !prepared.HasResidentInput() {
						sched.releaseSlots(t)
					}
					if b.err == nil {
						i++
						continue
					}
				}
			}
			if b.stopSpill != nil {
				// Stop at a write boundary before build can reorder or release the input.
				b.stopSpill()
				i++
				continue
			}
			builds = slices.Delete(builds, i, i+1)
			if isCanceled || b.err != nil {
				if isCanceled {
					b.err = errCancel
				}
				sched.finishTask(exec, b.err)
				continue
			}
			if t.IsVectorIndex() {
				buildingVectors++
			}
			running++
			go func() {
				err := sched.runStep(t, t.Execute)
				completed <- completion{kind: buildFinished, vector: t.IsVectorIndex()}
				if err == nil {
					err = sched.runStep(t, t.PostExecute)
				}
				sched.finishTask(exec, err)
				completed <- completion{kind: taskFinished}
			}()
		}
		for i := 0; i < len(downloads); {
			d := downloads[i]
			t := d.exec.task
			isCanceled := sched.ctx.Err() != nil || t.Ctx().Err() != nil
			if !isCanceled && ((buildWaiting && sched.usesStageSlots(t)) || inflight >= window) {
				i++
				continue
			}
			if !isCanceled && !sched.acquireSlots(t) {
				i++
				continue
			}
			downloads = slices.Delete(downloads, i, i+1)
			if isCanceled {
				sched.finishTask(d.exec, errCancel)
				continue
			}
			if d.bytes > math.MaxUint64-inflight {
				sched.finishTask(d.exec, merr.WrapErrServiceInternalMsg("index download byte accounting overflow"))
				continue
			}
			inflight += d.bytes
			running++
			go func() {
				err := sched.runStep(t, func(ctx context.Context) error {
					return t.(PreparedTask).LoadData(ctx)
				})
				completed <- completion{kind: downloadFinished, download: &d, err: err}
			}()
		}
		// Inputs that cannot build start spilling immediately. They remain in the
		// build queue so admission can interrupt the write and use the RAM copy.
		for _, b := range builds {
			t := b.exec.task
			prepared := t.(PreparedTask)
			if sched.ctx.Err() != nil || t.Ctx().Err() != nil || b.err != nil ||
				b.reserved || b.stopSpill != nil || b.spillDone || !prepared.HasResidentInput() {
				continue
			}
			ctx, cancel := context.WithCancel(t.Ctx())
			b.stopSpill = cancel
			go func() {
				defer cancel()
				complete, err := prepared.SpillInput(ctx)
				completed <- completion{kind: spillFinished, spill: b, spillComplete: complete, err: err}
			}()
		}
		if ready == nil && len(downloads) == 0 && len(builds) == 0 && running == 0 {
			return
		}
		select {
		case d, ok := <-ready:
			if !ok {
				ready = nil
				continue
			}
			exec, t := d.exec, d.exec.task
			if d.err != nil {
				sched.finishTask(exec, d.err)
				continue
			}
			exec.usesGPU = t.(PreparedTask).UsesGPU()
			exec.stopCancel = context.AfterFunc(t.Ctx(), func() {
				select {
				case sched.buildWakeup <- struct{}{}:
				default:
				}
			})
			downloads = append(downloads, d)
		case done := <-completed:
			consume(done)
		case <-sched.buildWakeup:
		case <-ticker.C:
		case <-canceled:
			canceled = nil
		}
	}
}
