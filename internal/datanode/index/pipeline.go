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
	"slices"
	"time"
)

func (sched *TaskScheduler) pipelineLoop() {
	defer sched.taskWg.Done()
	type download struct {
		exec *taskExecution
		err  error
	}
	type eventKind int
	const (
		downloadFinished eventKind = iota
		taskFinished
	)
	type completion struct {
		kind     eventKind
		download *download
		err      error
	}
	ready := make(chan download)
	// Task preparation must not block stage completions.
	go func(planned chan<- download) {
		defer close(planned)
		for exec := range sched.downloadTasks {
			d := download{exec: exec}
			d.err = sched.runStep(exec.task, exec.task.PreExecute)
			planned <- d
		}
	}(ready)
	canceled := sched.ctx.Done()
	completed := make(chan completion, cap(sched.downloadTasks))
	// Retry backend build reservations while retaining the task's node slots.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var downloads []download
	var builds []*taskExecution
	running := 0

	consume := func(done completion) {
		switch done.kind {
		case downloadFinished:
			running--
			d := done.download
			if done.err != nil {
				sched.finishTask(d.exec, done.err)
			} else {
				builds = append(builds, d.exec)
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
		for i := 0; i < len(builds); {
			exec := builds[i]
			t := exec.task
			isCanceled := sched.ctx.Err() != nil || t.Ctx().Err() != nil
			var err error
			if !isCanceled {
				admitted := sched.isBuildTurn(t)
				if admitted {
					admitted, err = t.(PreparedTask).TryReserveBuild()
				}
				if !admitted && err == nil {
					i++
					continue
				}
			}
			builds = slices.Delete(builds, i, i+1)
			if isCanceled || err != nil {
				if isCanceled {
					err = errCancel
				}
				sched.finishTask(exec, err)
				continue
			}
			sched.finishAdmission(t.Name())
			running++
			go func() {
				err := sched.executeTask(exec)
				if err == nil {
					err = sched.runStep(t, t.PostExecute)
				}
				sched.finishTask(exec, err)
				completed <- completion{kind: taskFinished}
			}()
		}
		for _, d := range downloads {
			t := d.exec.task
			isCanceled := sched.ctx.Err() != nil || t.Ctx().Err() != nil
			if isCanceled {
				sched.finishTask(d.exec, errCancel)
				continue
			}
			running++
			go func() {
				err := sched.runStep(t, func(ctx context.Context) error {
					return t.(PreparedTask).LoadData(ctx)
				})
				completed <- completion{kind: downloadFinished, download: &d, err: err}
			}()
		}
		downloads = nil
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
