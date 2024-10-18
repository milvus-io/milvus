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

package datacoord

import (
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type CompactionTask interface {
	Process() bool
	BuildCompactionRequest() (*datapb.CompactionPlan, error)
	GetSlotUsage() int64

	GetTriggerID() UniqueID
	GetPlanID() UniqueID
	GetState() datapb.CompactionTaskState
	GetChannel() string
	GetLabel() string

	GetType() datapb.CompactionType
	GetCollectionID() int64
	GetPartitionID() int64
	GetInputSegments() []int64
	GetStartTime() int64
	GetTimeoutInSeconds() int32
	GetPos() *msgpb.MsgPosition
	GetNodeID() UniqueID
	GetSpan() trace.Span
	ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask
	SetNodeID(UniqueID) error
	SetTask(*datapb.CompactionTask)
	SetSpan(trace.Span)
	EndSpan()
	CleanLogPath()
	NeedReAssignNodeID() bool
	SaveTaskMeta() error
}

type compactionTaskOpt func(task *datapb.CompactionTask)

func setNodeID(nodeID int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.NodeID = nodeID
	}
}

func setFailReason(reason string) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.FailReason = reason
	}
}

func setEndTime(endTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.EndTime = endTime
	}
}

func setTimeoutInSeconds(dur int32) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.TimeoutInSeconds = dur
	}
}

func setResultSegments(segments []int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.ResultSegments = segments
	}
}

func setTmpSegments(segments []int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.TmpSegments = segments
	}
}

func setState(state datapb.CompactionTaskState) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.State = state
	}
}

func setStartTime(startTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.StartTime = startTime
	}
}

func setRetryTimes(retryTimes int32) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.RetryTimes = retryTimes
	}
}

func setLastStateStartTime(lastStateStartTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.LastStateStartTime = lastStateStartTime
	}
}

type compactionTaskBase struct {
	*datapb.CompactionTask

	meta      CompactionMeta
	slotUsage int64

	lock sync.RWMutex
	span trace.Span
}

var _ CompactionTask = (*compactionTaskBase)(nil)

// BuildCompactionRequest implements CompactionTask.
func (t *compactionTaskBase) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	panic("unimplemented")
}

// CleanLogPath implements CompactionTask.
func (t *compactionTaskBase) CleanLogPath() {
	panic("unimplemented")
}

// NeedReAssignNodeID implements CompactionTask.
func (t *compactionTaskBase) NeedReAssignNodeID() bool {
	panic("unimplemented")
}

// Process implements CompactionTask.
func (t *compactionTaskBase) Process() bool {
	panic("unimplemented")
}

func (t *compactionTaskBase) SetTask(task *datapb.CompactionTask) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.CompactionTask = task
}

func (t *compactionTaskBase) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *compactionTaskBase) GetSpan() trace.Span {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.span
}

func (t *compactionTaskBase) SetSpan(span trace.Span) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.span = span
}

func (t *compactionTaskBase) EndSpan() {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.span != nil {
		t.span.End()
	}
}

func (t *compactionTaskBase) SetStartTime(startTime int64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.StartTime = startTime
}

func (t *compactionTaskBase) GetLabel() string {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return fmt.Sprintf("%d-%s", t.PartitionID, t.GetChannel())
}

func (t *compactionTaskBase) GetSlotUsage() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.slotUsage
}

func (t *compactionTaskBase) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *compactionTaskBase) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	task := t.ShadowClone(opts...)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.CompactionTask = task
	err := t.SaveTaskMeta()
	if err != nil {
		log.Warn("Failed to saveTaskMeta", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("updateAndSaveTaskMeta", err) // retryable
	}
	return nil
}

func (t *compactionTaskBase) SaveTaskMeta() error {
	return t.meta.SaveCompactionTask(t.CompactionTask)
}
