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

package task

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/util/merr"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type (
	Status   = int32
	Priority int32
)

const (
	TaskStatusCreated Status = iota + 1
	TaskStatusStarted
	TaskStatusSucceeded
	TaskStatusCanceled
	TaskStatusFailed
)

const (
	TaskPriorityLow    Priority = iota // for balance checker
	TaskPriorityNormal                 // for segment checker
	TaskPriorityHigh                   // for channel checker
)

var TaskPriorityName = map[Priority]string{
	TaskPriorityLow:    "Low",
	TaskPriorityNormal: "Normal",
	TaskPriorityHigh:   "High",
}

func (p Priority) String() string {
	return TaskPriorityName[p]
}

// All task priorities from low to high
var TaskPriorities = []Priority{TaskPriorityLow, TaskPriorityNormal, TaskPriorityHigh}

type Source fmt.Stringer

type Task interface {
	Context() context.Context
	Source() Source
	ID() UniqueID
	CollectionID() UniqueID
	ReplicaID() UniqueID
	SetID(id UniqueID)
	Status() Status
	SetStatus(status Status)
	Err() error
	Priority() Priority
	SetPriority(priority Priority)
	Index() string // dedup indexing string

	// cancel the task as we don't need to continue it
	Cancel(err error)
	// fail the task as we encounter some error so be unable to continue,
	// this error will be recorded for response to user requests
	Fail(err error)
	Wait() error
	Actions() []Action
	Step() int
	StepUp() int
	IsFinished(dist *meta.DistributionManager) bool
	SetReason(reason string)
	String() string
}

type baseTask struct {
	ctx      context.Context
	cancel   context.CancelFunc
	doneCh   chan struct{}
	canceled *atomic.Bool

	id           UniqueID // Set by scheduler
	collectionID UniqueID
	replicaID    UniqueID
	shard        string
	loadType     querypb.LoadType

	source   Source
	status   *atomic.Int32
	priority Priority
	err      error
	actions  []Action
	step     int
	reason   string

	// span for tracing
	span trace.Span
}

func newBaseTask(ctx context.Context, source Source, collectionID, replicaID UniqueID, shard string) *baseTask {
	ctx, cancel := context.WithCancel(ctx)
	ctx, span := otel.Tracer("QueryCoord").Start(ctx, "QueryCoord-BaseTask")

	return &baseTask{
		source:       source,
		collectionID: collectionID,
		replicaID:    replicaID,
		shard:        shard,

		status:   atomic.NewInt32(TaskStatusStarted),
		priority: TaskPriorityNormal,
		ctx:      ctx,
		cancel:   cancel,
		doneCh:   make(chan struct{}),
		canceled: atomic.NewBool(false),
		span:     span,
	}
}

func (task *baseTask) Context() context.Context {
	return task.ctx
}

func (task *baseTask) Source() Source {
	return task.source
}

func (task *baseTask) ID() UniqueID {
	return task.id
}

func (task *baseTask) SetID(id UniqueID) {
	task.id = id
}

func (task *baseTask) CollectionID() UniqueID {
	return task.collectionID
}

func (task *baseTask) ReplicaID() UniqueID {
	return task.replicaID
}

func (task *baseTask) LoadType() querypb.LoadType {
	return task.loadType
}

func (task *baseTask) Status() Status {
	return task.status.Load()
}

func (task *baseTask) SetStatus(status Status) {
	task.status.Store(status)
}

func (task *baseTask) Priority() Priority {
	return task.priority
}

func (task *baseTask) SetPriority(priority Priority) {
	task.priority = priority
}

func (task *baseTask) Index() string {
	return fmt.Sprintf("[replica=%d]", task.replicaID)
}

func (task *baseTask) Err() error {
	select {
	case <-task.doneCh:
		return task.err
	default:
		return nil
	}
}

func (task *baseTask) Cancel(err error) {
	if task.canceled.CompareAndSwap(false, true) {
		task.cancel()
		if task.Status() != TaskStatusSucceeded {
			task.SetStatus(TaskStatusCanceled)
		}
		task.err = err
		close(task.doneCh)
		if task.span != nil {
			task.span.End()
		}
	}
}

func (task *baseTask) Fail(err error) {
	if task.canceled.CompareAndSwap(false, true) {
		task.cancel()
		if task.Status() != TaskStatusSucceeded {
			task.SetStatus(TaskStatusFailed)
		}
		task.err = err
		close(task.doneCh)
	}
}

func (task *baseTask) Wait() error {
	<-task.doneCh
	return task.err
}

func (task *baseTask) Actions() []Action {
	return task.actions
}

func (task *baseTask) Step() int {
	return task.step
}

func (task *baseTask) StepUp() int {
	task.step++
	return task.step
}

func (task *baseTask) IsFinished(distMgr *meta.DistributionManager) bool {
	if task.Status() != TaskStatusStarted {
		return false
	}
	return task.Step() >= len(task.Actions())
}

func (task *baseTask) SetReason(reason string) {
	task.reason = reason
}

func (task *baseTask) String() string {
	var actionsStr string
	for i, action := range task.actions {
		if realAction, ok := action.(*SegmentAction); ok {
			actionsStr += fmt.Sprintf(`{[type=%v][node=%d][streaming=%v]}`, action.Type(), action.Node(), realAction.Scope() == querypb.DataScope_Streaming)
		} else {
			actionsStr += fmt.Sprintf(`{[type=%v][node=%d]}`, action.Type(), action.Node())
		}
		if i != len(task.actions)-1 {
			actionsStr += ", "
		}
	}
	return fmt.Sprintf(
		"[id=%d] [type=%s] [source=%s] [reason=%s] [collectionID=%d] [replicaID=%d] [priority=%s] [actionsCount=%d] [actions=%s]",
		task.id,
		GetTaskType(task).String(),
		task.source.String(),
		task.reason,
		task.collectionID,
		task.replicaID,
		task.priority.String(),
		len(task.actions),
		actionsStr,
	)
}

type SegmentTask struct {
	*baseTask

	segmentID UniqueID
}

// NewSegmentTask creates a SegmentTask with actions,
// all actions must process the same segment,
// empty actions is not allowed
func NewSegmentTask(ctx context.Context,
	timeout time.Duration,
	source Source,
	collectionID,
	replicaID UniqueID,
	actions ...Action,
) (*SegmentTask, error) {
	if len(actions) == 0 {
		return nil, errors.WithStack(merr.WrapErrParameterInvalid("non-empty actions", "no action"))
	}

	segmentID := int64(-1)
	shard := ""
	for _, action := range actions {
		action, ok := action.(*SegmentAction)
		if !ok {
			return nil, errors.WithStack(merr.WrapErrParameterInvalid("SegmentAction", "other action", "all actions must be with the same type"))
		}
		if segmentID == -1 {
			segmentID = action.SegmentID()
			shard = action.Shard()
		} else if segmentID != action.SegmentID() {
			return nil, errors.WithStack(merr.WrapErrParameterInvalid(segmentID, action.SegmentID(), "all actions must operate the same segment"))
		}
	}

	base := newBaseTask(ctx, source, collectionID, replicaID, shard)
	base.actions = actions
	return &SegmentTask{
		baseTask:  base,
		segmentID: segmentID,
	}, nil
}

func (task *SegmentTask) Shard() string {
	return task.shard
}

func (task *SegmentTask) SegmentID() UniqueID {
	return task.segmentID
}

func (task *SegmentTask) Index() string {
	return fmt.Sprintf("%s[segment=%d][growing=%t]", task.baseTask.Index(), task.segmentID, task.Actions()[0].(*SegmentAction).Scope() == querypb.DataScope_Streaming)
}

func (task *SegmentTask) String() string {
	return fmt.Sprintf("%s [segmentID=%d]", task.baseTask.String(), task.segmentID)
}

type ChannelTask struct {
	*baseTask
}

// NewChannelTask creates a ChannelTask with actions,
// all actions must process the same channel, and the same type of channel
// empty actions is not allowed
func NewChannelTask(ctx context.Context,
	timeout time.Duration,
	source Source,
	collectionID,
	replicaID UniqueID,
	actions ...Action,
) (*ChannelTask, error) {
	if len(actions) == 0 {
		return nil, errors.WithStack(merr.WrapErrParameterInvalid("non-empty actions", "no action"))
	}

	channel := ""
	for _, action := range actions {
		channelAction, ok := action.(*ChannelAction)
		if !ok {
			return nil, errors.WithStack(merr.WrapErrParameterInvalid("ChannelAction", "other action", "all actions must be with the same type"))
		}
		if channel == "" {
			channel = channelAction.ChannelName()
		} else if channel != channelAction.ChannelName() {
			return nil, errors.WithStack(merr.WrapErrParameterInvalid(channel, channelAction.ChannelName(), "all actions must operate the same segment"))
		}
	}

	base := newBaseTask(ctx, source, collectionID, replicaID, channel)
	base.actions = actions
	return &ChannelTask{
		baseTask: base,
	}, nil
}

func (task *ChannelTask) Channel() string {
	return task.shard
}

func (task *ChannelTask) Index() string {
	return fmt.Sprintf("%s[channel=%s]", task.baseTask.Index(), task.shard)
}

func (task *ChannelTask) String() string {
	return fmt.Sprintf("%s [channel=%s]", task.baseTask.String(), task.Channel())
}
