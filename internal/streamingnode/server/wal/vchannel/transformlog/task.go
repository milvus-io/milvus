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

package transformlog

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type transformTask interface {
	nodescheduler.Task
	Done() bool
}

type transformTaskBase struct {
	log          *TransformLog
	timetick     uint64
	predecessors []transformTask
	done         atomic.Bool
}

func (t *transformTaskBase) Done() bool {
	return t.done.Load()
}

func (t *transformTaskBase) predecessorsDone() bool {
	for _, predecessor := range t.predecessors {
		if predecessor != nil && !predecessor.Done() {
			return false
		}
	}
	return true
}

func (t *transformTaskBase) execute(ctx context.Context, ready bool, fn func(context.Context) error) error {
	if !ready {
		return nodescheduler.ErrDelay
	}
	err := fn(ctx)
	if err == nil {
		t.done.Store(true)
		return nil
	}
	return errors.Mark(err, nodescheduler.ErrDelay)
}

type transformMaterializeTask struct {
	transformTaskBase
}

func (t *transformMaterializeTask) Execute(ctx context.Context) error {
	// Materialization consumes the in-memory window, which observation (and
	// recovery) feeds directly; it never waits for the summary to persist.
	ready := t.predecessorsDone()
	return t.execute(ctx, ready, func(ctx context.Context) error {
		if _, err := t.log.materialize(ctx, materializeOption{TargetTimeTick: t.timetick}); err != nil {
			return err
		}
		return nil
	})
}

// maybeScheduleMaterializeLocked returns a task for the current window
// frontier, or nil when nothing needs scheduling.
//
// On the observation path (observe=true) at most one task is scheduled per
// observation moment: the cap-batch continuation inside materialize (see
// transform_log.go) chases the frontier, so observation never needs to append
// one task per record.
//
// On the upper-bound path (observe=false) a raise must not be swallowed. A
// raise that lands after the newest pending task committed under the old
// bound — between its continuation decision and its Done flag — would
// otherwise strand the (old bound, new bound] records in the window with no
// successor. A raise that lands before that task commits needs no new task:
// the task's own continuation decision re-reads the bound and covers it.
func (t *TransformLog) maybeScheduleMaterializeLocked(observe bool) *transformMaterializeTask {
	target := t.materializeTargetLocked()
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	if target <= t.materializedTimeTick {
		return nil
	}
	if observe {
		if len(t.materializeTasks) > 0 {
			return nil
		}
		return t.newMaterializeTaskLocked(target)
	}
	if len(t.materializeTasks) == 0 {
		return t.newMaterializeTaskLocked(target)
	}
	pending := t.pendingMaterializeTargetLocked()
	if pending >= target || t.materializedTimeTick < pending {
		return nil
	}
	return t.newMaterializeTaskLocked(target)
}

// newMaterializeTaskLocked appends a materialize task for target. It
// continues a capped batch: the current task is still pending (and becomes a
// predecessor of the new one), so execution order keeps the batches
// sequential.
func (t *TransformLog) newMaterializeTaskLocked(target uint64) *transformMaterializeTask {
	task := &transformMaterializeTask{
		transformTaskBase: transformTaskBase{
			log:          t,
			timetick:     target,
			predecessors: t.taskPredecessorsLocked(),
		},
	}
	t.materializeTasks = append(t.materializeTasks, task)
	return task
}

func (t *TransformLog) taskPredecessorsLocked() []transformTask {
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	// Chain only the newest pending task: it already transitively depends on
	// every earlier one, so the graph stays O(1) per task instead of copying
	// the whole backlog into each continuation.
	if n := len(t.materializeTasks); n > 0 {
		return []transformTask{t.materializeTasks[n-1]}
	}
	return nil
}

func (t *TransformLog) HasPendingMaterializeTask() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	return len(t.materializeTasks) > 0
}

func (t *TransformLog) pendingMaterializeTargetLocked() uint64 {
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	var target uint64
	for _, task := range t.materializeTasks {
		if task.timetick > target {
			target = task.timetick
		}
	}
	return target
}

func compactTransformMaterializeTasks(tasks []*transformMaterializeTask) []*transformMaterializeTask {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	clear(pending[len(pending):])
	return pending
}

var (
	_ nodescheduler.Task = (*transformMaterializeTask)(nil)
)
