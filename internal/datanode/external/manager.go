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
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
)

// TaskKey uniquely identifies an external collection task.
type TaskKey struct {
	ClusterID string
	TaskID    int64
}

// TaskInfo stores the mutable state of an external collection task.
type TaskInfo struct {
	Cancel          context.CancelFunc
	State           indexpb.JobState
	FailReason      string
	CollID          int64
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
	closeChan chan struct{}
}

// NewExternalCollectionManager constructs a manager with the provided worker pool size.
func NewExternalCollectionManager(ctx context.Context, poolSize int) *ExternalCollectionManager {
	return &ExternalCollectionManager{
		ctx:       ctx,
		tasks:     make(map[TaskKey]*TaskInfo),
		pool:      conc.NewPool[any](poolSize),
		closeChan: make(chan struct{}),
	}
}

// Start currently only logs a message to keep behaviour aligned with other components.
func (m *ExternalCollectionManager) Start() {
	log.Info("external collection manager started")
}

// Close releases all background resources.
func (m *ExternalCollectionManager) Close() {
	m.closeOnce.Do(func() {
		close(m.closeChan)
		if m.pool != nil {
			m.pool.Release()
		}
		log.Info("external collection manager closed")
	})
}

// LoadOrStore adds a task entry if absent and returns the existing entry if present.
func (m *ExternalCollectionManager) LoadOrStore(clusterID string, taskID int64, info *TaskInfo) *TaskInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := makeTaskKey(clusterID, taskID)
	if oldInfo, ok := m.tasks[key]; ok {
		return oldInfo
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

// UpdateResult commits the latest state plus kept/updated segments atomically.
func (m *ExternalCollectionManager) UpdateResult(clusterID string, taskID int64,
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
	req *datapb.UpdateExternalCollectionRequest,
	taskFunc func(context.Context) (*datapb.UpdateExternalCollectionResponse, error),
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

	if oldInfo := m.LoadOrStore(clusterID, taskID, info); oldInfo != nil {
		return fmt.Errorf("task already exists: taskID=%d", taskID)
	}

	// Submit to pool
	m.pool.Submit(func() (any, error) {
		defer cancel()
		log.Info("executing external collection task in pool",
			zap.Int64("taskID", taskID),
			zap.Int64("collectionID", req.GetCollectionID()))

		// Execute the task
		resp, err := taskFunc(taskCtx)
		if err != nil {
			m.UpdateResult(clusterID, taskID, indexpb.JobState_JobStateFailed, err.Error(), info.KeptSegments, nil)
			log.Warn("external collection task failed",
				zap.Int64("taskID", taskID),
				zap.Error(err))
			return nil, err
		}

		state := resp.GetState()
		if state == indexpb.JobState_JobStateNone {
			state = indexpb.JobState_JobStateFinished
		}
		failReason := resp.GetFailReason()
		kept := resp.GetKeptSegments()
		if len(kept) == 0 {
			kept = info.KeptSegments
		}
		m.UpdateResult(clusterID, taskID, state, failReason, kept, resp.GetUpdatedSegments())
		log.Info("external collection task completed",
			zap.Int64("taskID", taskID))
		return nil, nil
	})

	return nil
}
