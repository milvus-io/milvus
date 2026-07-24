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

// TaskInfo stores the mutable state of an external collection task.
type TaskInfo struct {
	Cancel     context.CancelFunc
	State      indexpb.JobState
	FailReason string
	CollID     int64
	// Version fences attempts of the same taskID. DataCoord bumps the task
	// version on every dispatch; state/result writes carry the version of the
	// attempt that produced them and are dropped unless it matches the entry's
	// version, so a canceled-but-still-running older attempt cannot overwrite
	// the state of a re-dispatched newer one.
	Version         int64
	KeptSegments    []int64
	UpdatedSegments []*datapb.SegmentInfo
}

// Clone creates a deep copy so callers can freely mutate the result.
func (t *TaskInfo) Clone() *TaskInfo {
	return &TaskInfo{
		Cancel:          t.Cancel,
		State:           t.State,
		FailReason:      t.FailReason,
		CollID:          t.CollID,
		Version:         t.Version,
		KeptSegments:    cloneSegmentIDs(t.KeptSegments),
		UpdatedSegments: cloneSegments(t.UpdatedSegments),
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

// registerAttempt installs a task attempt keyed by (clusterID, taskID) with
// version fencing:
//   - no existing entry → register, return true;
//   - existing entry with version >= the new attempt → duplicate/stale
//     dispatch, keep the existing entry, return false;
//   - existing entry with an older version → a re-dispatch superseding a
//     prior attempt: cancel the old attempt and replace the entry, return
//     true. The old attempt's goroutine may still be running, but all its
//     subsequent state/result writes carry the old version and are dropped
//     by the version guard.
func (m *ExternalCollectionManager) registerAttempt(clusterID string, taskID int64, info *TaskInfo) bool {
	m.mu.Lock()
	key := makeTaskKey(clusterID, taskID)
	oldInfo, ok := m.tasks[key]
	if ok && oldInfo.Version >= info.Version {
		m.mu.Unlock()
		return false
	}
	m.tasks[key] = info
	m.mu.Unlock()
	if ok && oldInfo.Cancel != nil {
		// Cancel outside the lock; the old goroutine's writes are version-fenced.
		oldInfo.Cancel()
	}
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

// DeleteIfVersion cancels and removes the task entry only when its version
// matches (version 0 forces removal, for legacy drops that carry no version).
// A drop issued for a superseded attempt (older version) is ignored so it cannot
// delete the entry of the re-dispatched attempt now occupying the same taskID.
// Returns the removed entry, or nil if nothing was removed.
func (m *ExternalCollectionManager) DeleteIfVersion(clusterID string, taskID int64, version int64) *TaskInfo {
	m.mu.Lock()
	key := makeTaskKey(clusterID, taskID)
	info, ok := m.tasks[key]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	if version != 0 && info.Version != version {
		m.mu.Unlock()
		mlog.Info(m.ctx, "ignore stale drop for a superseded task attempt",
			mlog.FieldTaskID(taskID),
			mlog.Int64("dropVersion", version),
			mlog.Int64("currentVersion", info.Version))
		return nil
	}
	delete(m.tasks, key)
	m.mu.Unlock()
	if info.Cancel != nil {
		info.Cancel()
	}
	return info
}

// UpdateResult commits the latest state plus kept/updated segments atomically.
// The write is dropped when the entry's version does not match: a superseded
// (re-dispatched) attempt must not overwrite the current attempt's result.
func (m *ExternalCollectionManager) UpdateResult(clusterID string, taskID int64, version int64,
	state indexpb.JobState,
	failReason string,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	info, ok := m.tasks[key]
	if !ok || info.Version != version {
		mlog.Info(m.ctx, "dropping stale task result write from a superseded attempt",
			mlog.FieldTaskID(taskID),
			mlog.Int64("writeVersion", version),
			mlog.Int64("currentVersion", func() int64 {
				if ok {
					return info.Version
				}
				return -1
			}()))
		return
	}
	info.State = state
	info.FailReason = failReason
	info.KeptSegments = append([]int64(nil), keptSegments...)
	info.UpdatedSegments = cloneSegments(updatedSegments)
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
	// Typed-retriable merr: object-store throttling (ErrIoTooManyRequests),
	// service not-ready / unavailable / rate-limited, node-not-match, and the
	// transient segcore I/O / resource codes (S3Error, File*Failed, OOM, …),
	// which the segcore code table already flags retriable — while a permanent
	// segcore code (FieldIDInvalid "column not found") stays non-retriable.
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
	version := req.GetTaskVersion()

	taskCtx, cancel := context.WithCancel(m.ctx)
	keptSegments := extractSegmentIDs(req.GetCurrentSegments())

	info := &TaskInfo{
		Cancel:          cancel,
		State:           indexpb.JobState_JobStateInProgress,
		FailReason:      "",
		CollID:          req.GetCollectionID(),
		Version:         version,
		KeptSegments:    keptSegments,
		UpdatedSegments: nil,
	}

	if !m.registerAttempt(clusterID, taskID, info) {
		// Same-or-newer attempt already registered — a duplicate dispatch
		// (e.g. scheduler TOCTOU race between Enqueue dedup check and Push).
		// Treat as idempotent success since that attempt is already running.
		cancel()
		mlog.Info(m.ctx, "task attempt already exists, treating as idempotent success",
			mlog.FieldTaskID(taskID),
			mlog.Int64("version", version),
			mlog.FieldCollectionID(req.GetCollectionID()))
		return nil
	}

	// Submit to pool
	m.pool.Submit(func() (_ any, retErr error) {
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
				reason := fmt.Sprintf("task panicked: %v", r)
				// A panic is deterministic for the given input (e.g. a zero-row
				// parquet); retrying would loop, so report a permanent failure.
				m.UpdateResult(clusterID, taskID, version, indexpb.JobState_JobStateFailed, reason, info.KeptSegments, nil)
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
			m.UpdateResult(clusterID, taskID, version, state, err.Error(), info.KeptSegments, nil)
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
		m.UpdateResult(clusterID, taskID, version, state, failReason, kept, resp.GetUpdatedSegments())
		mlog.Info(m.ctx, "external collection task completed",
			mlog.FieldTaskID(taskID))
		return nil, nil
	})

	return nil
}
