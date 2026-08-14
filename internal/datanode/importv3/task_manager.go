// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// State deliberately uses one-word values.  DataCoord maps these internal
// values to the generated worker response enum at the RPC boundary.
type State string

const (
	StatePending   State = "Pending"
	StateRunning   State = "Running"
	StateRetry     State = "Retry"
	StateCompleted State = "Completed"
	StateFailed    State = "Failed"
)

// Result is the small, immutable Query payload.  The result manifest is the
// source of truth; ResultRef/Digest are returned only after the manifest-last
// publish has completed.
type Result struct {
	Ref    string
	Digest []byte
	Rows   int64
	Bytes  int64
}

type Snapshot struct {
	TaskID      int64
	RunID       int64
	State       State
	Progress    float32
	Reason      string
	FailureCode int32
	Result      *Result
}

// Run is the DataNode execution callback.  It must publish its result before
// returning it.  A canceled context is the only cancellation mechanism used
// by V3; it never waits on the legacy global MemoryAllocator.
type Run func(context.Context, int64) (*Result, error)

type task struct {
	mu          sync.RWMutex
	taskID      int64
	runID       int64
	slot        int64
	state       State
	progress    float32
	reason      string
	failureCode int32
	result      *Result
	cancel      context.CancelFunc
}

// TaskManager owns only process-local task execution.  Durable task/run
// fencing remains in DataCoord; this manager's run check prevents late Query
// or completion callbacks from mutating a newer run on the same DataNode.
type TaskManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
	tasks   map[int64]*task
	workers sync.WaitGroup
	closed  bool
}

func NewTaskManager() *TaskManager {
	return NewTaskManagerWithContext(context.Background())

}

func NewTaskManagerWithContext(parent context.Context) *TaskManager {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &TaskManager{ctx: ctx, cancel: cancel, tasks: make(map[int64]*task)}
}

func (m *TaskManager) Add(taskID, runID, slot int64, execute Run) error {
	if m == nil || execute == nil || taskID == 0 || runID == 0 || slot <= 0 {
		return merr.WrapErrImportSysFailedMsg("invalid import V3 task create request")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return merr.WrapErrServiceNotReadyMsg("import V3 task manager is closed")
	}
	if existing, ok := m.tasks[taskID]; ok {
		existing.mu.RLock()
		existingRun := existing.runID
		existing.mu.RUnlock()
		switch {
		case runID == existingRun:
			m.mu.Unlock()
			return nil // Create is idempotent for the same fenced run.
		case runID < existingRun:
			m.mu.Unlock()
			return nil // Older run is stale and must not replace current work.
		default:
			if existing.cancel != nil {
				existing.cancel()
			}
			delete(m.tasks, taskID)
		}
	}
	ctx, cancel := context.WithCancel(m.ctx)
	t := &task{taskID: taskID, runID: runID, slot: slot, state: StatePending, cancel: cancel}
	m.tasks[taskID] = t
	m.workers.Add(1)
	m.mu.Unlock()

	go func() {
		defer m.workers.Done()
		t.mu.Lock()
		if t.state != StatePending {
			t.mu.Unlock()
			return
		}
		t.state = StateRunning
		t.mu.Unlock()
		result, err := execute(ctx, runID)
		t.mu.Lock()
		defer t.mu.Unlock()
		if err != nil {
			t.failureCode = merr.Code(err)
			if ctx.Err() != nil {
				t.state = StateRetry
				t.reason = ctx.Err().Error()
				t.failureCode = merr.Code(ctx.Err())
			} else {
				if merr.IsRetryableErr(err) {
					t.state = StateRetry
				} else {
					t.state = StateFailed
				}
				t.reason = err.Error()
			}
			return
		}
		if result == nil || result.Ref == "" || len(result.Digest) == 0 {
			t.state = StateFailed
			t.reason = "completed import V3 task returned incomplete result"
			t.failureCode = merr.Code(merr.ErrImportSysFailed)
			return
		}
		t.result = cloneResult(result)
		t.progress = 1
		t.state = StateCompleted
	}()
	return nil
}

// Slots returns slots currently occupied by pending or running V3 tasks.
// Completed, failed, and retryable runs have stopped consuming DataNode work.
func (m *TaskManager) Slots() int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var slots int64
	for _, task := range m.tasks {
		task.mu.RLock()
		if task.state == StatePending || task.state == StateRunning {
			slots += task.slot
		}
		task.mu.RUnlock()
	}
	return slots
}

func (m *TaskManager) Query(taskID, runID int64) (Snapshot, bool) {
	m.mu.RLock()
	t := m.tasks[taskID]
	m.mu.RUnlock()
	if t == nil {
		return Snapshot{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if runID != 0 && runID != t.runID {
		// A stale Query is deliberately a no-op.  DataCoord will query the
		// persisted current run again instead of treating an old worker reply
		// as a task failure.
		return Snapshot{}, false
	}
	return Snapshot{
		TaskID:      t.taskID,
		RunID:       t.runID,
		State:       t.state,
		Progress:    t.progress,
		Reason:      t.reason,
		FailureCode: t.failureCode,
		Result:      cloneResult(t.result),
	}, true
}

func (m *TaskManager) UpdateProgress(taskID, runID int64, progress float32) bool {
	m.mu.RLock()
	t := m.tasks[taskID]
	m.mu.RUnlock()
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.runID != runID || t.state != StateRunning {
		return false
	}
	if progress < t.progress {
		return true
	}
	if progress > 1 {
		progress = 1
	}
	t.progress = progress
	return true
}

func (m *TaskManager) Drop(taskID, runID int64) bool {
	m.mu.Lock()
	t := m.tasks[taskID]
	if t == nil {
		m.mu.Unlock()
		return false
	}
	t.mu.RLock()
	matched := runID == 0 || t.runID == runID
	cancel := t.cancel
	t.mu.RUnlock()
	if !matched {
		m.mu.Unlock()
		return false
	}
	delete(m.tasks, taskID)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

// Close stops accepting useful work, cancels every process-local V3 task, and
// waits for the callbacks visible to this DataNode process.  It is a local
// shutdown guarantee only; it does not create a cross-node DropAndWait RPC.
func (m *TaskManager) Close() {
	if m == nil {
		return
	}
	m.cancel()
	m.mu.Lock()
	m.closed = true
	for _, task := range m.tasks {
		if task.cancel != nil {
			task.cancel()
		}
	}
	m.tasks = make(map[int64]*task)
	m.mu.Unlock()
	m.workers.Wait()
}

func cloneResult(result *Result) *Result {
	if result == nil {
		return nil
	}
	clone := *result
	clone.Digest = append([]byte(nil), result.Digest...)
	return &clone
}
