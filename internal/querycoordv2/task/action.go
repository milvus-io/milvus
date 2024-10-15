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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ActionType int32

const (
	ActionTypeGrow ActionType = iota + 1
	ActionTypeReduce
	ActionTypeUpdate
	ActionTypeStatsUpdate
)

var ActionTypeName = map[ActionType]string{
	ActionTypeGrow:        "Grow",
	ActionTypeReduce:      "Reduce",
	ActionTypeUpdate:      "Update",
	ActionTypeStatsUpdate: "StatsUpdate",
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

	// return current action's workload effect on target query node
	// which only works for `Grow` and `Reduce`, cause `Update` won't change query node's workload
	WorkLoadEffect() int
}

type BaseAction struct {
	NodeID typeutil.UniqueID
	Typ    ActionType
	Shard  string

	workloadEffect int
}

func NewBaseAction(nodeID typeutil.UniqueID, typ ActionType, shard string, workLoadEffect int) *BaseAction {
	return &BaseAction{
		NodeID:         nodeID,
		Typ:            typ,
		Shard:          shard,
		workloadEffect: workLoadEffect,
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

func (action *BaseAction) WorkLoadEffect() int {
	return action.workloadEffect
}

type SegmentAction struct {
	*BaseAction

	SegmentID typeutil.UniqueID
	Scope     querypb.DataScope

	rpcReturned atomic.Bool
}

// Deprecate, only for existing unit test
func NewSegmentAction(nodeID typeutil.UniqueID, typ ActionType, shard string, segmentID typeutil.UniqueID) *SegmentAction {
	return NewSegmentActionWithScope(nodeID, typ, shard, segmentID, querypb.DataScope_All, 0)
}

func NewSegmentActionWithScope(nodeID typeutil.UniqueID, typ ActionType, shard string, segmentID typeutil.UniqueID, scope querypb.DataScope, rowCount int) *SegmentAction {
	workloadEffect := 0
	switch typ {
	case ActionTypeGrow:
		workloadEffect = rowCount
	case ActionTypeReduce:
		workloadEffect = -rowCount
	default:
		workloadEffect = 0
	}
	return &SegmentAction{
		BaseAction:  NewBaseAction(nodeID, typ, shard, workloadEffect),
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
	return action.rpcReturned.Load()
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
	workloadEffect := 0
	switch typ {
	case ActionTypeGrow:
		workloadEffect = 1
	case ActionTypeReduce:
		workloadEffect = -1
	default:
		workloadEffect = 0
	}
	return &ChannelAction{
		BaseAction: NewBaseAction(nodeID, typ, channelName, workloadEffect),
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
		BaseAction: NewBaseAction(workerID, typ, shard, 0),
		leaderID:   leaderID,
		segmentID:  segmentID,
		version:    version,
	}
	action.rpcReturned.Store(false)
	return action
}

func NewLeaderUpdatePartStatsAction(leaderID, workerID typeutil.UniqueID, typ ActionType, shard string, partStatsVersions map[int64]int64) *LeaderAction {
	action := &LeaderAction{
		BaseAction:        NewBaseAction(workerID, typ, shard, 0),
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
