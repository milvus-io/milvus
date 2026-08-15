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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type (
	Status   = string
	Priority int32
)

const (
	TaskStatusCreated   = "created"
	TaskStatusStarted   = "started"
	TaskStatusSucceeded = "succeeded"
	TaskStatusCanceled  = "canceled"
	TaskStatusFailed    = "failed"
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
	ID() typeutil.UniqueID
	CollectionID() typeutil.UniqueID
	// Return 0 if the task is a reduce task without given replica.
	ReplicaID() typeutil.UniqueID
	// Return "" if the task is a reduce task without given replica.
	ResourceGroup() string
	Shard() string
	SetID(id typeutil.UniqueID)
	Status() Status
	SetStatus(status Status)
	Err() error
	Priority() Priority
	SetPriority(priority Priority)
	Index() string // dedup indexing string

	// ActivateDeadline starts the task's execution timeout the first time it is
	// actually dispatched to a QueryNode, so time spent waiting for a scheduler
	// slot doesn't count against the RPC budget. No-op after the first call.
	ActivateDeadline()
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

	// MarshalJSON marshal task info to json
	MarshalJSON() ([]byte, error)
	Name() string
	GetReason() string

	RecordStartTs()
	GetTaskLatency() int64
}

type baseTask struct {
	// ctxMu guards ctx/cancel, which are swapped once by ActivateDeadline
	// and read concurrently by Context()/Cancel()/Fail() from the scheduler
	// loop and the executor's per-action goroutines.
	ctxMu   sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	timeout time.Duration
	// deadlineOnce ensures the execution timeout is applied at most once,
	// on the first call to ActivateDeadline.
	deadlineOnce sync.Once

	doneCh   chan struct{}
	canceled *atomic.Bool

	id           typeutil.UniqueID // Set by scheduler
	collectionID typeutil.UniqueID
	replica      *meta.Replica
	shard        string
	loadType     querypb.LoadType

	source   Source
	status   *atomic.String
	priority Priority
	err      error
	actions  []Action
	step     int
	reason   string

	// span for tracing
	span trace.Span
	name string

	// startTs
	startTs atomic.Time
}

func newBaseTask(ctx context.Context, timeout time.Duration, source Source, collectionID typeutil.UniqueID, replica *meta.Replica, shard string, taskTag string) *baseTask {
	// The deadline is deliberately NOT applied here: at construction time the
	// task hasn't been scheduled yet and may sit in the wait queue or be
	// repeatedly rejected by the executor's admission cap. Starting the clock
	// now would burn the RPC budget on queueing delay rather than on the
	// action RPCs it's meant to bound. ActivateDeadline arms it on first
	// dispatch instead.
	ctx, cancel := context.WithCancel(ctx)
	ctx, span := otel.Tracer(typeutil.QueryCoordRole).Start(ctx, taskTag)
	startTs := atomic.Time{}
	startTs.Store(time.Now())

	return &baseTask{
		source:       source,
		collectionID: collectionID,
		replica:      replica,
		shard:        shard,

		status:   atomic.NewString(TaskStatusStarted),
		priority: TaskPriorityNormal,
		ctx:      ctx,
		cancel:   cancel,
		timeout:  timeout,
		doneCh:   make(chan struct{}),
		canceled: atomic.NewBool(false),
		span:     span,
		startTs:  startTs,
	}
}

// ActivateDeadline arms the task's execution timeout, bounding the action
// RPCs issued with task.Context() from this point on so a stuck server-side
// operation cannot pin the scheduler slot forever. It is a no-op if the task
// was constructed with timeout <= 0, or if called more than once (only the
// first dispatch starts the clock).
func (task *baseTask) ActivateDeadline() {
	if task.timeout <= 0 {
		return
	}
	task.deadlineOnce.Do(func() {
		task.ctxMu.Lock()
		defer task.ctxMu.Unlock()
		if task.canceled.Load() {
			return
		}
		parentCtx, parentCancel := task.ctx, task.cancel
		ctx, cancel := context.WithTimeout(parentCtx, task.timeout)
		task.ctx = ctx
		task.cancel = func() {
			cancel()
			parentCancel()
		}
	})
}

func (task *baseTask) Context() context.Context {
	task.ctxMu.Lock()
	defer task.ctxMu.Unlock()
	return task.ctx
}

func (task *baseTask) Source() Source {
	return task.source
}

func (task *baseTask) ID() typeutil.UniqueID {
	return task.id
}

func (task *baseTask) SetID(id typeutil.UniqueID) {
	task.id = id
}

func (task *baseTask) CollectionID() typeutil.UniqueID {
	return task.collectionID
}

func (task *baseTask) ReplicaID() typeutil.UniqueID {
	// replica may be nil, 0 will be generated.
	return task.replica.GetID()
}

func (task *baseTask) ResourceGroup() string {
	// replica may be nil, empty string will be generated.
	return task.replica.GetResourceGroup()
}

func (task *baseTask) Shard() string {
	return task.shard
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
	return fmt.Sprintf("[replica=%d]", task.ReplicaID())
}

func (task *baseTask) RecordStartTs() {
	task.startTs.Store(time.Now())
}

func (task *baseTask) GetTaskLatency() int64 {
	return time.Since(task.startTs.Load()).Milliseconds()
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
		task.ctxMu.Lock()
		cancel := task.cancel
		task.ctxMu.Unlock()
		cancel()
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
		task.ctxMu.Lock()
		cancel := task.cancel
		task.ctxMu.Unlock()
		cancel()
		if task.Status() != TaskStatusSucceeded {
			task.SetStatus(TaskStatusFailed)
		}
		task.err = err
		close(task.doneCh)
		if task.span != nil {
			task.span.End()
		}
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

func (task *baseTask) GetReason() string {
	return task.reason
}

func (task *baseTask) MarshalJSON() ([]byte, error) {
	return marshalJSON(task)
}

func (task *baseTask) String() string {
	var actionsStr string
	for _, action := range task.actions {
		actionsStr += action.String() + ","
	}
	return fmt.Sprintf(
		"[id=%d] [type=%s] [source=%s] [reason=%s] [collectionID=%d] [replicaID=%d] [resourceGroup=%s] [priority=%s] [actionsCount=%d] [actions=%s]",
		task.id,
		GetTaskType(task).String(),
		task.source.String(),
		task.reason,
		task.collectionID,
		task.ReplicaID(),
		task.ResourceGroup(),
		task.priority.String(),
		len(task.actions),
		actionsStr,
	)
}

func (task *baseTask) Name() string {
	return fmt.Sprintf("%s-%s-%d", task.source.String(), GetTaskType(task).String(), task.id)
}

type SegmentTask struct {
	*baseTask

	segmentID    typeutil.UniqueID
	loadPriority commonpb.LoadPriority
	// for balance segment task, expected load and release execution on the same shard leader
	shardLeaderID int64
}

// NewSegmentTask creates a SegmentTask with actions,
// all actions must process the same segment,
// empty actions is not allowed
func NewSegmentTask(ctx context.Context,
	timeout time.Duration,
	source Source,
	collectionID typeutil.UniqueID,
	replica *meta.Replica,
	loadPriority commonpb.LoadPriority,
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
			segmentID = action.GetSegmentID()
			shard = action.GetShard()
		} else if segmentID != action.GetSegmentID() {
			return nil, errors.WithStack(merr.WrapErrParameterInvalid(segmentID, action.GetSegmentID(), "all actions must operate the same segment"))
		}
	}

	base := newBaseTask(ctx, timeout, source, collectionID, replica, shard, fmt.Sprintf("SegmentTask-%s-%d", actions[0].Type().String(), segmentID))
	base.actions = actions
	return &SegmentTask{
		baseTask:      base,
		segmentID:     segmentID,
		loadPriority:  loadPriority,
		shardLeaderID: -1,
	}, nil
}

func (task *SegmentTask) LoadPriority() commonpb.LoadPriority {
	return task.loadPriority
}

func (task *SegmentTask) SegmentID() typeutil.UniqueID {
	return task.segmentID
}

func (task *SegmentTask) Index() string {
	return fmt.Sprintf("%s[segment=%d][growing=%t]", task.baseTask.Index(), task.segmentID, task.Actions()[0].(*SegmentAction).GetScope() == querypb.DataScope_Streaming)
}

func (task *SegmentTask) Name() string {
	return fmt.Sprintf("%s-SegmentTask[%d]-%d", task.source.String(), task.ID(), task.segmentID)
}

func (task *SegmentTask) String() string {
	return fmt.Sprintf("%s [segmentID=%d][loadPriority=%d]", task.baseTask.String(), task.segmentID, task.loadPriority)
}

func (task *SegmentTask) MarshalJSON() ([]byte, error) {
	return marshalJSON(task)
}

func (task *SegmentTask) ShardLeaderID() int64 {
	return task.shardLeaderID
}

func (task *SegmentTask) SetShardLeaderID(id int64) {
	task.shardLeaderID = id
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
	collectionID typeutil.UniqueID,
	replica *meta.Replica,
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

	base := newBaseTask(ctx, timeout, source, collectionID, replica, channel, fmt.Sprintf("ChannelTask-%s-%s", actions[0].Type().String(), channel))
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

func (task *ChannelTask) Name() string {
	return fmt.Sprintf("%s-ChannelTask[%d]-%s", task.source.String(), task.ID(), task.shard)
}

func (task *ChannelTask) String() string {
	return fmt.Sprintf("%s [channel=%s]", task.baseTask.String(), task.Channel())
}

func (task *ChannelTask) MarshalJSON() ([]byte, error) {
	return marshalJSON(task)
}

type LeaderTask struct {
	*baseTask

	segmentID typeutil.UniqueID
	leaderID  int64
	innerName string
}

func NewLeaderSegmentTask(ctx context.Context,
	timeout time.Duration,
	source Source,
	collectionID typeutil.UniqueID,
	replica *meta.Replica,
	leaderID int64,
	action *LeaderAction,
) *LeaderTask {
	segmentID := action.SegmentID()
	base := newBaseTask(ctx, timeout, source, collectionID, replica, action.Shard, fmt.Sprintf("LeaderSegmentTask-%s-%d", action.Type().String(), segmentID))
	base.actions = []Action{action}
	return &LeaderTask{
		baseTask:  base,
		segmentID: segmentID,
		leaderID:  leaderID,
		innerName: fmt.Sprintf("%s-LeaderSegmentTask", source.String()),
	}
}

func NewLeaderPartStatsTask(ctx context.Context,
	timeout time.Duration,
	source Source,
	collectionID typeutil.UniqueID,
	replica *meta.Replica,
	leaderID int64,
	action *LeaderAction,
) *LeaderTask {
	base := newBaseTask(ctx, timeout, source, collectionID, replica, action.Shard, fmt.Sprintf("LeaderPartitionStatsTask-%s", action.Type().String()))
	base.actions = []Action{action}
	return &LeaderTask{
		baseTask:  base,
		leaderID:  leaderID,
		innerName: fmt.Sprintf("%s-LeaderPartitionStatsTask", source.String()),
	}
}

func (task *LeaderTask) SegmentID() typeutil.UniqueID {
	return task.segmentID
}

func (task *LeaderTask) Index() string {
	return fmt.Sprintf("%s[segment=%d][growing=false]", task.baseTask.Index(), task.segmentID)
}

func (task *LeaderTask) String() string {
	return fmt.Sprintf("%s [segmentID=%d][leader=%d]", task.baseTask.String(), task.segmentID, task.leaderID)
}

func (task *LeaderTask) Name() string {
	return fmt.Sprintf("%s[%d]-%d", task.innerName, task.ID(), task.leaderID)
}

func (task *LeaderTask) MarshalJSON() ([]byte, error) {
	return marshalJSON(task)
}

func marshalJSON(task Task) ([]byte, error) {
	return json.Marshal(&metricsinfo.QueryCoordTask{
		TaskName:     task.Name(),
		CollectionID: task.CollectionID(),
		Replica:      task.ReplicaID(),
		TaskType:     GetTaskType(task).String(),
		TaskStatus:   task.Status(),
		Priority:     task.Priority().String(),
		Actions: lo.Map(task.Actions(), func(t Action, i int) string {
			return t.Desc()
		}),
		Step:   task.Step(),
		Reason: task.GetReason(),
	})
}
