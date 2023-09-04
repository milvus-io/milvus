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

package balance

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
)

type NodePriority func(collectionID int64, nodeID int64) int

var (
	RowCountBasedBalancerName = "RowCountBasedBalancer"
	ScoreBasedBalancerName    = "ScoreBasedBalancer"
)

func GetBalancePolicy(taskScheduler task.Scheduler, dist *meta.DistributionManager) BalancePolicy {
	policy := params.Params.QueryCoordCfg.Balancer.GetValue()

	switch policy {
	case RowCountBasedBalancerName:
		return NewRowCountBasedPolicy(taskScheduler, dist)
	case ScoreBasedBalancerName:
		return NewScoreBasedPolicy(taskScheduler, dist)
	}

	log.Info("use default rowCountBased balancer", zap.String("unsupportedPolicy", policy))
	return NewRowCountBasedPolicy(taskScheduler, dist)
}

type SegmentAssignPlan struct {
	Segment   *meta.Segment
	ReplicaID int64
	From      int64 // -1 if empty
	To        int64
}

func (segPlan *SegmentAssignPlan) ToString() string {
	return fmt.Sprintf("SegmentPlan:[collectionID: %d, replicaID: %d, segmentID: %d, from: %d, to: %d]\n",
		segPlan.Segment.CollectionID, segPlan.ReplicaID, segPlan.Segment.ID, segPlan.From, segPlan.To)
}

type ChannelAssignPlan struct {
	Channel   *meta.DmChannel
	ReplicaID int64
	From      int64
	To        int64
}

func (chanPlan *ChannelAssignPlan) ToString() string {
	return fmt.Sprintf("ChannelPlan:[collectionID: %d, channel: %s, replicaID: %d, from: %d, to: %d]\n",
		chanPlan.Channel.CollectionID, chanPlan.Channel.ChannelName, chanPlan.ReplicaID, chanPlan.From, chanPlan.To)
}

// BalancePolicy defines details about how to make distribution more balance.
// 1. how to assign segment/channel to nodes
// 2. which segment/channel should be moved out
// 3. the target average value of segment/channel
type BalancePolicy interface {
	AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan
	AssignChannel(channels []*meta.DmChannel, nodes []int64) []ChannelAssignPlan

	getAverageWithChannel(collectionID int64, nodes []int64) int
	getAverageWithSegment(collectionID int64, nodes []int64) int

	getSegmentsToMove(collectionID int64, nodeID int64, averageCount int) []*meta.Segment
	getChannelsToMove(collectionID int64, nodeID int64, averageCount int) []*meta.DmChannel

	getPriorityChange(s *meta.Segment) int
	getPriorityWithSegment(collectionID int64, nodeID int64) int
}
