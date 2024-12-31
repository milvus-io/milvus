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
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ActionType int32

const (
	ActionTypeGrow ActionType = iota + 1
	ActionTypeReduce
	ActionTypeUpdate
)

var ActionTypeName = map[ActionType]string{
	ActionTypeGrow:   "Grow",
	ActionTypeReduce: "Reduce",
	ActionTypeUpdate: "Update",
}

func (t ActionType) String() string {
	return ActionTypeName[t]
}

type Action interface {
	Node() int64
	Type() ActionType
	IsFinished(distMgr *meta.DistributionManager) bool
	Desc() string
	String() string
}

type BaseAction struct {
	NodeID typeutil.UniqueID
	Typ    ActionType
	Shard  string
}

func NewBaseAction(nodeID typeutil.UniqueID, typ ActionType, shard string) *BaseAction {
	return &BaseAction{
		NodeID: nodeID,
		Typ:    typ,
		Shard:  shard,
	}
}

func (action *BaseAction) Node() int64 {
	return action.NodeID
}

func (action *BaseAction) Type() ActionType {
	return action.Typ
}

func (action *BaseAction) GetShard() string {
	return action.Shard
}

func (action *BaseAction) String() string {
	return fmt.Sprintf(`{[type=%v][node=%d][shard=%v]}`, action.Type(), action.Node(), action.Shard)
}

type SegmentAction struct {
	*BaseAction

	SegmentID typeutil.UniqueID
	Scope     querypb.DataScope

	rpcReturned atomic.Bool
}

func NewSegmentAction(nodeID typeutil.UniqueID, typ ActionType, shard string, segmentID typeutil.UniqueID) *SegmentAction {
	return NewSegmentActionWithScope(nodeID, typ, shard, segmentID, querypb.DataScope_All)
}

func NewSegmentActionWithScope(nodeID typeutil.UniqueID, typ ActionType, shard string, segmentID typeutil.UniqueID, scope querypb.DataScope) *SegmentAction {
	base := NewBaseAction(nodeID, typ, shard)
	return &SegmentAction{
		BaseAction:  base,
		SegmentID:   segmentID,
		Scope:       scope,
		rpcReturned: *atomic.NewBool(false),
	}
}

func (action *SegmentAction) GetSegmentID() typeutil.UniqueID {
	return action.SegmentID
}

func (action *SegmentAction) GetScope() querypb.DataScope {
	return action.Scope
}

func (action *SegmentAction) IsFinished(distMgr *meta.DistributionManager) bool {
	if action.Type() == ActionTypeGrow {
		// rpc finished
		if !action.rpcReturned.Load() {
			return false
		}

		// segment found in leader view
		views := distMgr.LeaderViewManager.GetByFilter(
			meta.WithChannelName2LeaderView(action.Shard),
			meta.WithSegment2LeaderView(action.SegmentID, false))
		if len(views) == 0 {
			return false
		}

		// segment found in dist
		segmentInTargetNode := distMgr.SegmentDistManager.GetByFilter(meta.WithNodeID(action.Node()), meta.WithSegmentID(action.SegmentID))
		return len(segmentInTargetNode) > 0
	} else if action.Type() == ActionTypeReduce {
		// FIXME: Now shard leader's segment view is a map of segment ID to node ID,
		// loading segment replaces the node ID with the new one,
		// which confuses the condition of finishing,
		// the leader should return a map of segment ID to list of nodes,
		// now, we just always commit the release task to executor once.
		// NOTE: DO NOT create a task containing release action and the action is not the last action
		sealed := distMgr.SegmentDistManager.GetByFilter(meta.WithNodeID(action.Node()))
		views := distMgr.LeaderViewManager.GetByFilter(meta.WithNodeID2LeaderView(action.Node()))
		growing := lo.FlatMap(views, func(view *meta.LeaderView, _ int) []int64 {
			return lo.Keys(view.GrowingSegments)
		})
		segments := make([]int64, 0, len(sealed)+len(growing))
		for _, segment := range sealed {
			segments = append(segments, segment.GetID())
		}
		segments = append(segments, growing...)
		if !funcutil.SliceContain(segments, action.GetSegmentID()) {
			return true
		}
		return action.rpcReturned.Load()
	} else if action.Type() == ActionTypeUpdate {
		return action.rpcReturned.Load()
	}

	return true
}

func (action *SegmentAction) Desc() string {
	return fmt.Sprintf("type:%s node id: %d, data scope:%s", action.Type().String(), action.Node(), action.Scope.String())
}

func (action *SegmentAction) String() string {
	return action.BaseAction.String() + fmt.Sprintf(`{[segmentID=%d][scope=%d]}`, action.SegmentID, action.Scope)
}

type ChannelAction struct {
	*BaseAction
}

func NewChannelAction(nodeID typeutil.UniqueID, typ ActionType, channelName string) *ChannelAction {
	return &ChannelAction{
		BaseAction: NewBaseAction(nodeID, typ, channelName),
	}
}

func (action *ChannelAction) ChannelName() string {
	return action.Shard
}

func (action *ChannelAction) Desc() string {
	return fmt.Sprintf("type:%s node id: %d", action.Type().String(), action.Node())
}

func (action *ChannelAction) IsFinished(distMgr *meta.DistributionManager) bool {
	views := distMgr.LeaderViewManager.GetByFilter(meta.WithChannelName2LeaderView(action.ChannelName()))
	_, hasNode := lo.Find(views, func(v *meta.LeaderView) bool {
		return v.ID == action.Node()
	})
	isGrow := action.Type() == ActionTypeGrow

	return hasNode == isGrow
}

type LeaderAction struct {
	*BaseAction

	leaderID  typeutil.UniqueID
	segmentID typeutil.UniqueID
	version   typeutil.UniqueID // segment load ts, 0 means not set

	partStatsVersions map[int64]int64
	rpcReturned       atomic.Bool
}

func NewLeaderAction(leaderID, workerID typeutil.UniqueID, typ ActionType, shard string, segmentID typeutil.UniqueID, version typeutil.UniqueID) *LeaderAction {
	action := &LeaderAction{
		BaseAction: NewBaseAction(workerID, typ, shard),

		leaderID:  leaderID,
		segmentID: segmentID,
		version:   version,
	}
	action.rpcReturned.Store(false)
	return action
}

func NewLeaderUpdatePartStatsAction(leaderID, workerID typeutil.UniqueID, typ ActionType, shard string, partStatsVersions map[int64]int64) *LeaderAction {
	action := &LeaderAction{
		BaseAction:        NewBaseAction(workerID, typ, shard),
		leaderID:          leaderID,
		partStatsVersions: partStatsVersions,
	}
	action.rpcReturned.Store(false)
	return action
}

func (action *LeaderAction) SegmentID() typeutil.UniqueID {
	return action.segmentID
}

func (action *LeaderAction) Version() typeutil.UniqueID {
	return action.version
}

func (action *LeaderAction) PartStats() map[int64]int64 {
	return action.partStatsVersions
}

func (action *LeaderAction) Desc() string {
	return fmt.Sprintf("type:%s, node id: %d, segment id:%d ,version:%d, leader id:%d",
		action.Type().String(), action.Node(), action.SegmentID(), action.Version(), action.GetLeaderID())
}

func (action *LeaderAction) String() string {
	partStatsStr := ""
	if action.PartStats() != nil {
		partStatsStr = fmt.Sprintf("%v", action.PartStats())
	}
	return action.BaseAction.String() + fmt.Sprintf(`{[leaderID=%v][segmentID=%d][version=%d][partStats=%s]}`,
		action.GetLeaderID(), action.SegmentID(), action.Version(), partStatsStr)
}

func (action *LeaderAction) GetLeaderID() typeutil.UniqueID {
	return action.leaderID
}

func (action *LeaderAction) IsFinished(distMgr *meta.DistributionManager) bool {
	return action.rpcReturned.Load()
}
