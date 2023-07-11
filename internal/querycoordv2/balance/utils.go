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
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	PlanInfoPrefix = "Balance-Plans:"
	DistInfoPrefix = "Balance-Dists:"
)

func CreateSegmentTasksFromPlans(ctx context.Context, checkerID int64, timeout time.Duration, plans []SegmentAssignPlan) []task.Task {
	ret := make([]task.Task, 0)
	for _, p := range plans {
		actions := make([]task.Action, 0)
		if p.To != -1 {
			action := task.NewSegmentActionWithScope(p.To, task.ActionTypeGrow, p.Segment.GetInsertChannel(), p.Segment.GetID(), querypb.DataScope_Historical)
			actions = append(actions, action)
		}
		if p.From != -1 {
			action := task.NewSegmentActionWithScope(p.From, task.ActionTypeReduce, p.Segment.GetInsertChannel(), p.Segment.GetID(), querypb.DataScope_Historical)
			actions = append(actions, action)
		}
		t, err := task.NewSegmentTask(
			ctx,
			timeout,
			checkerID,
			p.Segment.GetCollectionID(),
			p.ReplicaID,
			actions...,
		)
		if err != nil {
			log.Warn("create segment task from plan failed",
				zap.Int64("collection", p.Segment.GetCollectionID()),
				zap.Int64("segmentID", p.Segment.GetID()),
				zap.Int64("replica", p.ReplicaID),
				zap.String("channel", p.Segment.GetInsertChannel()),
				zap.Int64("from", p.From),
				zap.Int64("to", p.To),
				zap.Error(err),
			)
			continue
		}

		log.Info("create segment task",
			zap.Int64("collection", p.Segment.GetCollectionID()),
			zap.Int64("segmentID", p.Segment.GetID()),
			zap.Int64("replica", p.ReplicaID),
			zap.String("channel", p.Segment.GetInsertChannel()),
			zap.Int64("from", p.From),
			zap.Int64("to", p.To))
		if task.GetTaskType(t) == task.TaskTypeMove {
			// from balance checker
			t.SetPriority(task.TaskPriorityLow)
		} else {
			//from segment checker
			t.SetPriority(task.TaskPriorityNormal)
		}
		ret = append(ret, t)
	}
	return ret
}

func CreateChannelTasksFromPlans(ctx context.Context, checkerID int64, timeout time.Duration, plans []ChannelAssignPlan) []task.Task {
	ret := make([]task.Task, 0, len(plans))
	for _, p := range plans {
		actions := make([]task.Action, 0)
		if p.To != -1 {
			action := task.NewChannelAction(p.To, task.ActionTypeGrow, p.Channel.GetChannelName())
			actions = append(actions, action)
		}
		if p.From != -1 {
			action := task.NewChannelAction(p.From, task.ActionTypeReduce, p.Channel.GetChannelName())
			actions = append(actions, action)
		}
		t, err := task.NewChannelTask(ctx, timeout, checkerID, p.Channel.GetCollectionID(), p.ReplicaID, actions...)
		if err != nil {
			log.Warn("create channel task failed",
				zap.Int64("collection", p.Channel.GetCollectionID()),
				zap.Int64("replica", p.ReplicaID),
				zap.String("channel", p.Channel.GetChannelName()),
				zap.Int64("from", p.From),
				zap.Int64("to", p.To),
				zap.Error(err),
			)
			continue
		}

		log.Info("create channel task",
			zap.Int64("collection", p.Channel.GetCollectionID()),
			zap.Int64("replica", p.ReplicaID),
			zap.String("channel", p.Channel.GetChannelName()),
			zap.Int64("from", p.From),
			zap.Int64("to", p.To))
		t.SetPriority(task.TaskPriorityHigh)
		ret = append(ret, t)
	}
	return ret
}

func PrintNewBalancePlans(collectionID int64, replicaID int64, segmentPlans []SegmentAssignPlan,
	channelPlans []ChannelAssignPlan) {
	balanceInfo := fmt.Sprintf("%s new plans:{collectionID:%d, replicaID:%d, ", PlanInfoPrefix, collectionID, replicaID)
	for _, segmentPlan := range segmentPlans {
		balanceInfo += segmentPlan.ToString()
	}
	for _, channelPlan := range channelPlans {
		balanceInfo += channelPlan.ToString()
	}
	balanceInfo += "}"
	log.Info(balanceInfo)
}

func PrintCurrentReplicaDist(replica *meta.Replica,
	stoppingNodesSegments map[int64][]*meta.Segment, nodeSegments map[int64][]*meta.Segment,
	channelManager *meta.ChannelDistManager, segmentDistMgr *meta.SegmentDistManager) {
	distInfo := fmt.Sprintf("%s {collectionID:%d, replicaID:%d, ", DistInfoPrefix, replica.CollectionID, replica.GetID())
	//1. print stopping nodes segment distribution
	distInfo += "[stoppingNodesSegmentDist:"
	for stoppingNodeID, stoppedSegments := range stoppingNodesSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", stoppingNodeID)
		distInfo += "stopped-segments:["
		for _, stoppedSegment := range stoppedSegments {
			distInfo += fmt.Sprintf("%d,", stoppedSegment.GetID())
		}
		distInfo += "]]"
	}
	distInfo += "]"
	//2. print normal nodes segment distribution
	distInfo += "[normalNodesSegmentDist:"
	for normalNodeID, normalNodeCollectionSegments := range nodeSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", normalNodeID)
		distInfo += "loaded-segments:["
		nodeRowSum := int64(0)
		normalNodeSegments := segmentDistMgr.GetByNode(normalNodeID)
		for _, normalNodeSegment := range normalNodeSegments {
			nodeRowSum += normalNodeSegment.GetNumOfRows()
		}
		nodeCollectionRowSum := int64(0)
		for _, normalCollectionSegment := range normalNodeCollectionSegments {
			distInfo += fmt.Sprintf("[segmentID: %d, rowCount: %d] ",
				normalCollectionSegment.GetID(), normalCollectionSegment.GetNumOfRows())
			nodeCollectionRowSum += normalCollectionSegment.GetNumOfRows()
		}
		distInfo += fmt.Sprintf("] nodeRowSum:%d nodeCollectionRowSum:%d]", nodeRowSum, nodeCollectionRowSum)
	}
	distInfo += "]"

	//3. print stopping nodes channel distribution
	distInfo += "[stoppingNodesChannelDist:"
	for stoppingNodeID := range stoppingNodesSegments {
		stoppingNodeChannels := channelManager.GetByCollectionAndNode(replica.GetCollectionID(), stoppingNodeID)
		distInfo += fmt.Sprintf("[nodeID:%d, count:%d,", stoppingNodeID, len(stoppingNodeChannels))
		distInfo += "channels:["
		for _, stoppingChan := range stoppingNodeChannels {
			distInfo += fmt.Sprintf("%s,", stoppingChan.GetChannelName())
		}
		distInfo += "]]"
	}
	distInfo += "]"

	//4. print normal nodes channel distribution
	distInfo += "[normalNodesChannelDist:"
	for normalNodeID := range nodeSegments {
		normalNodeChannels := channelManager.GetByCollectionAndNode(replica.GetCollectionID(), normalNodeID)
		distInfo += fmt.Sprintf("[nodeID:%d, count:%d,", normalNodeID, len(normalNodeChannels))
		distInfo += "channels:["
		for _, normalNodeChan := range normalNodeChannels {
			distInfo += fmt.Sprintf("%s,", normalNodeChan.GetChannelName())
		}
		distInfo += "]]"
	}
	distInfo += "]"

	log.Info(distInfo)
}
