package task

import (
	"errors"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	ErrActionCanceled  = errors.New("ActionCanceled")
	ErrActionRPCFailed = errors.New("ActionRPCFailed")
	ErrActionStale     = errors.New("ActionStale")
)

type ActionType = int32

const (
	ActionTypeGrow ActionType = iota + 1
	ActionTypeReduce
)

type Action interface {
	Node() int64
	Type() ActionType
	IsFinished(distMgr *meta.DistributionManager) bool
}

type BaseAction struct {
	nodeID UniqueID
	typ    ActionType

	onDone []func()
}

func NewBaseAction(nodeID UniqueID, typ ActionType) *BaseAction {
	return &BaseAction{
		nodeID: nodeID,
		typ:    typ,
	}
}

func (action *BaseAction) Node() int64 {
	return action.nodeID
}

func (action *BaseAction) Type() ActionType {
	return action.typ
}

type SegmentAction struct {
	*BaseAction

	segmentID UniqueID
	scope     querypb.DataScope

	isReleaseCommitted atomic.Bool
}

func NewSegmentAction(nodeID UniqueID, typ ActionType, segmentID UniqueID, onDone ...func()) *SegmentAction {
	return NewSegmentActionWithScope(nodeID, typ, segmentID, querypb.DataScope_All, onDone...)
}
func NewSegmentActionWithScope(nodeID UniqueID, typ ActionType, segmentID UniqueID, scope querypb.DataScope, onDone ...func()) *SegmentAction {
	base := NewBaseAction(nodeID, typ)
	base.onDone = append(base.onDone, onDone...)
	return &SegmentAction{
		BaseAction:         base,
		segmentID:          segmentID,
		scope:              scope,
		isReleaseCommitted: *atomic.NewBool(false),
	}
}

func (action *SegmentAction) SegmentID() UniqueID {
	return action.segmentID
}

func (action *SegmentAction) Scope() querypb.DataScope {
	return action.scope
}

func (action *SegmentAction) IsFinished(distMgr *meta.DistributionManager) bool {
	if action.Type() == ActionTypeGrow {
		nodes := distMgr.LeaderViewManager.GetSealedSegmentDist(action.SegmentID())
		return lo.Contains(nodes, action.Node())
	}
	// FIXME: Now shard leader's segment view is a map of segment ID to node ID,
	// loading segment replaces the node ID with the new one,
	// which confuses the condition of finishing,
	// the leader should return a map of segment ID to list of nodes,
	// now, we just always commit the release task to executor once.
	// NOTE: DO NOT create a task containing release action and the action is not the last action

	return action.isReleaseCommitted.Load()
}

type ChannelAction struct {
	*BaseAction
	channelName string
}

func NewChannelAction(nodeID UniqueID, typ ActionType, channelName string) *ChannelAction {
	return &ChannelAction{
		BaseAction: NewBaseAction(nodeID, typ),

		channelName: channelName,
	}
}

func (action *ChannelAction) ChannelName() string {
	return action.channelName
}

func (action *ChannelAction) IsFinished(distMgr *meta.DistributionManager) bool {
	nodes := distMgr.LeaderViewManager.GetChannelDist(action.ChannelName())
	hasNode := lo.Contains(nodes, action.Node())
	isGrow := action.Type() == ActionTypeGrow

	return hasNode == isGrow
}
