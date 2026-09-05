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

package external

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TaskKey uniquely identifies an external collection task.
type TaskKey struct {
	ClusterID string
	TaskID    int64
}

type taskAdmission uint8

const (
	taskAdmissionSaturated taskAdmission = iota
	taskAdmissionDuplicate
	taskAdmissionAccepted
)

// TaskInfo stores the mutable state of an external collection task.
type TaskInfo struct {
	Cancel          context.CancelFunc
	State           indexpb.JobState
	FailReason      string
	CollID          int64
	KeptSegments    []int64
	UpdatedSegments []*datapb.SegmentInfo
	StartedAt       time.Time
}

// Clone creates a deep copy so callers can freely mutate the result.
func (t *TaskInfo) Clone() *TaskInfo {
	return &TaskInfo{
		Cancel:          t.Cancel,
		State:           t.State,
		FailReason:      t.FailReason,
		CollID:          t.CollID,
		KeptSegments:    cloneSegmentIDs(t.KeptSegments),
		UpdatedSegments: cloneSegments(t.UpdatedSegments),
		StartedAt:       t.StartedAt,
	}
}

func makeTaskKey(clusterID string, taskID int64) TaskKey {
	return TaskKey{
		ClusterID: clusterID,
		TaskID:    taskID,
	}
}

func cloneSegmentIDs(src []int64) []int64 {
	if len(src) == 0 {
		return nil
	}
	dst := make([]int64, len(src))
	copy(dst, src)
	return dst
}

func extractSegmentIDs(segments []*datapb.SegmentInfo) []int64 {
	if len(segments) == 0 {
		return nil
	}
	result := make([]int64, 0, len(segments))
	for _, seg := range segments {
		if seg == nil {
			continue
		}
		result = append(result, seg.GetID())
	}
	return result
}

// cloneSegments returns deep copies of SegmentInfo slices.
func cloneSegments(src []*datapb.SegmentInfo) []*datapb.SegmentInfo {
	if len(src) == 0 {
		return nil
	}
	cloned := make([]*datapb.SegmentInfo, len(src))
	for i, seg := range src {
		if seg == nil {
			continue
		}
		cloned[i] = proto.Clone(seg).(*datapb.SegmentInfo)
	}
	return cloned
}

// ExternalCollectionManager supervises the lifecycle of external collection tasks
// within a single datanode.
type ExternalCollectionManager struct {
	ctx   context.Context
	mu    sync.RWMutex
	tasks map[TaskKey]*TaskInfo
	pool  *conc.Pool[any]
	// slots mirrors the pool's capacity so SubmitTask can refuse work without
	// blocking. pool.Submit parks the caller when every worker is busy, and the
	// caller here is a DataCoord RPC handler: parking it past the RPC deadline
	// makes DataCoord give up on a task this node will still run later. A full
	// pool is normal operation (refreshes are long external scans), so the
	// right answer is a retryable rejection, not a wait.
	slots chan struct{}
	// lifecycleMu closes the gap between checking whether the pool is closed
	// and submitting work. Close must not release the pool in that gap, or the
	// accepted task would keep its map entry and slot without ever running.
	lifecycleMu sync.Mutex
	closeOnce   sync.Once
}

// NewExternalCollectionManager constructs a manager with the provided worker pool size.
func NewExternalCollectionManager(ctx context.Context, poolSize int) *ExternalCollectionManager {
	manager := &ExternalCollectionManager{
		ctx:   ctx,
		tasks: make(map[TaskKey]*TaskInfo),
		pool:  conc.NewPool[any](poolSize),
		slots: make(chan struct{}, poolSize),
	}
	return manager
}

// Close releases all background resources.
func (m *ExternalCollectionManager) Close() {
	m.closeOnce.Do(func() {
		m.lifecycleMu.Lock()
		defer m.lifecycleMu.Unlock()
		if m.pool != nil {
			m.pool.Release()
		}
		mlog.Info(m.ctx, "external collection manager closed")
	})
}

// RemoveExpiredTasks cancels and removes tasks older than cutoff.
func (m *ExternalCollectionManager) RemoveExpiredTasks(ctx context.Context, cutoff time.Time) int {
	type expiredTask struct {
		key    TaskKey
		state  indexpb.JobState
		age    time.Duration
		cancel context.CancelFunc
	}
	expired := make([]expiredTask, 0)
	m.mu.Lock()
	for key, info := range m.tasks {
		if info.StartedAt.IsZero() || info.StartedAt.After(cutoff) {
			continue
		}
		delete(m.tasks, key)
		expired = append(expired, expiredTask{
			key: key, state: info.State, age: time.Since(info.StartedAt), cancel: info.Cancel,
		})
	}
	m.mu.Unlock()

	for _, task := range expired {
		if task.cancel != nil {
			task.cancel()
		}
		mlog.Info(ctx, "reclaiming expired external collection task",
			mlog.FieldTaskID(task.key.TaskID),
			mlog.String("clusterID", task.key.ClusterID),
			mlog.String("state", task.state.String()),
			mlog.Duration("age", task.age))
	}
	return len(expired)
}

// LoadOrStore adds a task entry if absent and returns the existing entry if present.
func (m *ExternalCollectionManager) LoadOrStore(clusterID string, taskID int64, info *TaskInfo) *TaskInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	if oldInfo, ok := m.tasks[key]; ok {
		return oldInfo
	}
	if info.StartedAt.IsZero() {
		info.StartedAt = time.Now()
	}
	m.tasks[key] = info
	return nil
}

// Get returns a cloned snapshot of a task.
func (m *ExternalCollectionManager) Get(clusterID string, taskID int64) *TaskInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := makeTaskKey(clusterID, taskID)
	if info, ok := m.tasks[key]; ok {
		return info.Clone()
	}
	return nil
}

// Delete removes the task entry and returns the previous value.
func (m *ExternalCollectionManager) Delete(clusterID string, taskID int64) *TaskInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	if info, ok := m.tasks[key]; ok {
		delete(m.tasks, key)
		return info
	}
	return nil
}

// tryAdmitTask makes duplicate detection, capacity reservation, and ownership
// publication one atomic state transition. A SubmitTask attempt is never
// visible in m.tasks unless it already owns a worker slot, so a concurrent
// duplicate cannot observe an attempt that will subsequently be rejected for
// saturation.
func (m *ExternalCollectionManager) tryAdmitTask(clusterID string, taskID int64, info *TaskInfo) taskAdmission {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	if _, ok := m.tasks[key]; ok {
		return taskAdmissionDuplicate
	}

	select {
	case m.slots <- struct{}{}:
		if info.StartedAt.IsZero() {
			info.StartedAt = time.Now()
		}
		m.tasks[key] = info
		return taskAdmissionAccepted
	default:
		return taskAdmissionSaturated
	}
}

// UpdateState updates only the state/failReason fields.
func (m *ExternalCollectionManager) UpdateState(clusterID string, taskID int64, state indexpb.JobState, failReason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	if info, ok := m.tasks[key]; ok {
		info.State = state
		info.FailReason = failReason
	}
}

// externalRefreshFailureState decides whether a failed refresh attempt is worth
// another one. Input errors are permanent; system and unknown errors remain
// retriable and consume the coordinator's attempt budget.
func externalRefreshFailureState(err error) indexpb.JobState {
	if merr.GetErrorType(err) == merr.InputError {
		return indexpb.JobState_JobStateFailed
	}
	return indexpb.JobState_JobStateRetry
}

func (m *ExternalCollectionManager) updateResult(clusterID string, taskID int64,
	state indexpb.JobState,
	failReason string,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	if info, ok := m.tasks[key]; ok {
		info.State = state
		info.FailReason = failReason
		info.KeptSegments = append([]int64(nil), keptSegments...)
		info.UpdatedSegments = cloneSegments(updatedSegments)
	}
}

// CancelTask triggers the context cancellation for a task if it exists.
func (m *ExternalCollectionManager) CancelTask(clusterID string, taskID int64) bool {
	key := makeTaskKey(clusterID, taskID)
	m.mu.RLock()
	info, ok := m.tasks[key]
	var cancel context.CancelFunc
	if ok {
		cancel = info.Cancel
	}
	m.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
	return ok
}

// SubmitTask registers and runs a task asynchronously in the manager pool.
func (m *ExternalCollectionManager) SubmitTask(
	clusterID string,
	req *datapb.RefreshExternalCollectionTaskRequest,
	taskFunc func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error),
) error {
	taskID := req.GetTaskID()

	taskCtx, cancel := context.WithCancel(m.ctx)
	keptSegments := extractSegmentIDs(req.GetCurrentSegments())

	info := &TaskInfo{
		Cancel:          cancel,
		State:           indexpb.JobState_JobStateInProgress,
		FailReason:      "",
		CollID:          req.GetCollectionID(),
		KeptSegments:    keptSegments,
		UpdatedSegments: nil,
	}

	// Close, admission, and Submit form one lifecycle transition. Keeping them
	// under one lock prevents both a rejected submission leak and a duplicate
	// caller observing an admission that shutdown is about to roll back.
	m.lifecycleMu.Lock()
	if m.pool.IsClosed() {
		m.lifecycleMu.Unlock()
		cancel()
		return merr.WrapErrServiceUnavailable("external collection worker pool is closed")
	}

	admission := m.tryAdmitTask(clusterID, taskID, info)
	if admission == taskAdmissionDuplicate {
		m.lifecycleMu.Unlock()
		// A duplicate dispatch is idempotent: the registered task keeps its slot.
		mlog.Info(m.ctx, "task already exists, treating as idempotent success",
			mlog.FieldTaskID(taskID),
			mlog.FieldCollectionID(req.GetCollectionID()))
		cancel()
		return nil
	}

	if admission == taskAdmissionSaturated {
		m.lifecycleMu.Unlock()
		cancel()
		mlog.Info(m.ctx, "external collection worker pool saturated, refusing task",
			mlog.FieldTaskID(taskID),
			mlog.FieldCollectionID(req.GetCollectionID()),
			mlog.Int("poolSize", cap(m.slots)))
		return merr.WrapErrTooManyRequests(int32(cap(m.slots)),
			"external collection worker pool saturated, retry later")
	}

	// Submit to pool
	m.pool.Submit(func() (_ any, retErr error) {
		defer func() { <-m.slots }()
		defer cancel()
		// Defense-in-depth: isolate panics in a single task so a buggy
		// external source cannot crash the whole datanode process (e.g.
		// divide-by-zero from a zero-row parquet, fix for #49225).
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				mlog.Error(m.ctx, "external collection task panicked",
					mlog.FieldTaskID(taskID),
					mlog.FieldCollectionID(req.GetCollectionID()),
					mlog.Any("panic", r),
					mlog.ByteString("stack", stack))
				// A panic is a worker-side system failure, not proof that the
				// request is invalid. Convert it to the same structured error used
				// by ordinary task failures and let the existing classifier decide;
				// DataCoord's attempt budget bounds repeated failures.
				retErr = merr.WrapErrServiceInternalMsg("task panicked: %v", r)
				state := externalRefreshFailureState(retErr)
				m.updateResult(clusterID, taskID, state, retErr.Error(), info.KeptSegments, nil)
			}
		}()
		mlog.Info(m.ctx, "executing external collection task in pool",
			mlog.FieldTaskID(taskID),
			mlog.FieldCollectionID(req.GetCollectionID()))

		// Execute the task
		resp, err := taskFunc(taskCtx)
		if err != nil {
			state := externalRefreshFailureState(err)
			m.updateResult(clusterID, taskID, state, err.Error(), info.KeptSegments, nil)
			mlog.Warn(m.ctx, "external collection task failed",
				mlog.FieldTaskID(taskID),
				mlog.String("state", state.String()),
				mlog.Err(err))
			return nil, err
		}

		state := resp.GetState()
		if state == indexpb.JobState_JobStateNone {
			state = indexpb.JobState_JobStateFinished
		}
		failReason := resp.GetFailReason()
		kept := resp.GetKeptSegments()
		m.updateResult(clusterID, taskID, state, failReason, kept, resp.GetUpdatedSegments())
		mlog.Info(m.ctx, "external collection task completed",
			mlog.FieldTaskID(taskID))
		return nil, nil
	})
	m.lifecycleMu.Unlock()

	return nil
}
