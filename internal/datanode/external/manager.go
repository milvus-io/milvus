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
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TaskKey uniquely identifies an external collection task.
type TaskKey struct {
	ClusterID string
	TaskID    int64
}

// TaskInfo stores the mutable state of an external collection task.
type TaskInfo struct {
	Cancel          context.CancelFunc
	Done            chan struct{}
	State           indexpb.JobState
	FailReason      string
	CollID          int64
	KeptSegments    []int64
	UpdatedSegments []*datapb.SegmentInfo
	BaseManifests   map[int64]string
}

// Clone creates a deep copy so callers can freely mutate the result.
func (t *TaskInfo) Clone() *TaskInfo {
	return &TaskInfo{
		Cancel:          t.Cancel,
		Done:            t.Done,
		State:           t.State,
		FailReason:      t.FailReason,
		CollID:          t.CollID,
		KeptSegments:    cloneSegmentIDs(t.KeptSegments),
		UpdatedSegments: cloneSegments(t.UpdatedSegments),
		BaseManifests:   cloneBaseManifests(t.BaseManifests),
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

func cloneBaseManifests(src map[int64]string) map[int64]string {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[int64]string, len(src))
	for segmentID, manifest := range src {
		cloned[segmentID] = manifest
	}
	return cloned
}

func cloneSegments(src []*datapb.SegmentInfo) []*datapb.SegmentInfo {
	if len(src) == 0 {
		return nil
	}
	cloned := make([]*datapb.SegmentInfo, len(src))
	for i, segment := range src {
		if segment != nil {
			cloned[i] = proto.Clone(segment).(*datapb.SegmentInfo)
		}
	}
	return cloned
}

// ExternalCollectionManager supervises the lifecycle of external collection tasks
// within a single datanode.
type ExternalCollectionManager struct {
	ctx       context.Context
	mu        sync.RWMutex
	tasks     map[TaskKey]*TaskInfo
	pool      *conc.Pool[any]
	closeOnce sync.Once
}

// NewExternalCollectionManager constructs a manager with the provided worker pool size.
func NewExternalCollectionManager(ctx context.Context, poolSize int) *ExternalCollectionManager {
	return &ExternalCollectionManager{
		ctx:   ctx,
		tasks: make(map[TaskKey]*TaskInfo),
		pool:  conc.NewPool[any](poolSize),
	}
}

// Close releases all background resources.
func (m *ExternalCollectionManager) Close() {
	m.closeOnce.Do(func() {
		if m.pool != nil {
			m.pool.Release()
		}
		mlog.Info(m.ctx, "external collection manager closed")
	})
}

// registerTask installs a task keyed by (clusterID, taskID). A duplicate is
// rejected: DataCoord must successfully drop the resident task before retrying.
func (m *ExternalCollectionManager) registerTask(clusterID string, taskID int64, info *TaskInfo) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info.Done == nil {
		info.Done = make(chan struct{})
	}
	key := makeTaskKey(clusterID, taskID)
	if _, ok := m.tasks[key]; ok {
		return false
	}
	m.tasks[key] = info
	return true
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

// Delete cancels the task, waits for its worker-pool closure to finish, and only
// then removes the resident entry. Keeping the entry installed while waiting is
// the dispatch fence: registerTask continues rejecting the same taskID, and the
// old closure cannot write through the key into a replacement attempt.
func (m *ExternalCollectionManager) Delete(ctx context.Context, clusterID string, taskID int64) (*TaskInfo, error) {
	m.mu.Lock()
	key := makeTaskKey(clusterID, taskID)
	info, ok := m.tasks[key]
	if !ok {
		m.mu.Unlock()
		return nil, nil
	}
	m.mu.Unlock()
	if info.Cancel != nil {
		info.Cancel()
	}
	if info.Done != nil {
		select {
		case <-info.Done:
		case <-ctx.Done():
			return nil, merr.Wrap(ctx.Err(), "wait canceled external refresh task to exit")
		}
	}
	m.mu.Lock()
	if current, exists := m.tasks[key]; exists && current == info {
		delete(m.tasks, key)
	}
	m.mu.Unlock()
	return info, nil
}

func closeDone(done chan struct{}) {
	if done == nil {
		return
	}
	select {
	case <-done:
	default:
		close(done)
	}
}

func (m *ExternalCollectionManager) finishTask(clusterID string, taskID int64, info *TaskInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.tasks[makeTaskKey(clusterID, taskID)]; ok && current == info {
		closeDone(info.Done)
	}
}

// updateResult commits the latest state, segment payload and manifest-fence
// sidecar atomically.
func (m *ExternalCollectionManager) updateResult(clusterID string, taskID int64,
	state indexpb.JobState,
	failReason string,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
	baseManifests map[int64]string,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info, ok := m.tasks[makeTaskKey(clusterID, taskID)]; ok {
		info.State = state
		info.FailReason = failReason
		info.KeptSegments = append([]int64(nil), keptSegments...)
		info.UpdatedSegments = cloneSegments(updatedSegments)
		info.BaseManifests = cloneBaseManifests(baseManifests)
	}
}

// isKnownRetryableRefreshError reports whether a refresh execution failure is a
// KNOWN-transient condition worth re-dispatching. Everything else — an unknown or
// permanent build/data error (a missing external column, a schema mismatch,
// corrupt data) — must NOT be retried: a rerun reproduces it and would loop the
// refresh forever, so it fails the task instead. This mirrors the DataCoord-side
// isRetryableRefreshFailure; the two must agree on what is retryable.
func isKnownRetryableRefreshError(err error) bool {
	if err == nil {
		return false
	}
	// Compatibility with the current storage-v3 boundary: Loon FFI failures
	// carry this sentinel underneath ErrStorage, whose outer merr code is not
	// retryable. errors.Is still reaches the inner sentinel through that wrap.
	// Keep the generic merr check below as the forward-compatible path for a
	// future storage release that exposes typed retryable errors directly.
	if errors.Is(err, packed.ErrLoonTransient) {
		return true
	}
	// Typed-retriable merr: object-store throttling (ErrIoTooManyRequests),
	// service not-ready / unavailable / rate-limited, node-not-match, the
	// transient segcore I/O / resource codes (S3Error, File*Failed, OOM, …) that
	// the segcore code table flags retriable. A permanent segcore code
	// (FieldIDInvalid "column not found") stays non-retriable and falls through
	// to Failed.
	if merr.IsRetryableErr(err) {
		return true
	}
	// A canceled or timed-out attempt was aborted, not judged — re-dispatch is safe.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// The target worker node is gone; re-dispatch on a live node.
	return errors.Is(err, merr.ErrNodeNotFound)
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
		Done:            make(chan struct{}),
		State:           indexpb.JobState_JobStateInProgress,
		FailReason:      "",
		CollID:          req.GetCollectionID(),
		KeptSegments:    keptSegments,
		UpdatedSegments: nil,
		BaseManifests:   nil,
	}

	if !m.registerTask(clusterID, taskID, info) {
		// An attempt is already resident under this taskID. Report a duplicate
		// instead of absorbing it: this dispatch carries its own pre-allocated
		// segment ID range, so the resident attempt's result is NOT a valid
		// substitute for it. DataCoord classifies ErrTaskDuplicate as retryable,
		// drops the resident attempt, and re-dispatches on the next tick.
		cancel()
		err := merr.WrapErrTaskDuplicate(taskcommon.RefreshExternalCollection,
			fmt.Sprintf("refresh external collection task already existed with %s-%d", clusterID, taskID))
		mlog.Warn(m.ctx, "duplicated refresh external collection task",
			mlog.FieldTaskID(taskID),
			mlog.FieldCollectionID(req.GetCollectionID()),
			mlog.Err(err))
		return err
	}

	// Submit to pool
	m.pool.Submit(func() (_ any, retErr error) {
		defer cancel()
		defer m.finishTask(clusterID, taskID, info)
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
				reason := fmt.Sprintf("task panicked: %v", r)
				// A panic is deterministic for the given input (e.g. a zero-row
				// parquet); retrying would loop, so report a permanent failure.
				m.updateResult(clusterID, taskID, indexpb.JobState_JobStateFailed, reason, info.KeptSegments, nil, nil)
				// A recovered panic is a server-side failure, never caller input.
				retErr = merr.WrapErrServiceInternalMsg("%s", reason)
			}
		}()
		mlog.Info(m.ctx, "executing external collection task in pool",
			mlog.FieldTaskID(taskID),
			mlog.FieldCollectionID(req.GetCollectionID()))

		// Execute the task
		resp, err := taskFunc(taskCtx)
		if err != nil {
			// Retry ONLY a KNOWN-transient failure; an unknown or permanent
			// build/data error (a missing external column, a schema mismatch,
			// corrupt data) is reproduced by any rerun, so it must fail the task
			// rather than loop the refresh forever. The segcore code table already
			// flags the genuinely transient I/O / resource errors retriable, so
			// isKnownRetryableRefreshError keeps object-store throttling and the
			// like retriable while a "column not found" fails fast.
			state := indexpb.JobState_JobStateFailed
			if isKnownRetryableRefreshError(err) {
				state = indexpb.JobState_JobStateRetry
			}
			m.updateResult(clusterID, taskID, state, err.Error(), info.KeptSegments, nil, nil)
			mlog.Warn(m.ctx, "external collection task failed",
				mlog.FieldTaskID(taskID),
				mlog.String("reportedState", state.String()),
				mlog.Err(err))
			return nil, err
		}

		state := resp.GetState()
		if state == indexpb.JobState_JobStateNone {
			state = indexpb.JobState_JobStateFinished
		}
		failReason := resp.GetFailReason()
		kept := resp.GetKeptSegments()
		m.updateResult(clusterID, taskID, state, failReason, kept, resp.GetUpdatedSegments(), resp.GetBaseManifests())
		mlog.Info(m.ctx, "external collection task completed",
			mlog.FieldTaskID(taskID))
		return nil, nil
	})

	return nil
}
