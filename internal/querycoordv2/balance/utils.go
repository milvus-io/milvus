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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
)

const (
	InfoPrefix = "Balance-Info:"
)

func CreateSegmentTasksFromPlans(ctx context.Context, checkerID int64, timeout time.Duration, plans []SegmentAssignPlan) []task.Task {
	ret := make([]task.Task, 0)
	for _, p := range plans {
		actions := make([]task.Action, 0)
		if p.To != -1 {
			action := task.NewSegmentAction(p.To, task.ActionTypeGrow, p.Segment.GetInsertChannel(), p.Segment.GetID())
			actions = append(actions, action)
		}
		if p.From != -1 {
			action := task.NewSegmentAction(p.From, task.ActionTypeReduce, p.Segment.GetInsertChannel(), p.Segment.GetID())
			actions = append(actions, action)
		}
		task, err := task.NewSegmentTask(
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
			zap.Int64("replica", p.ReplicaID),
			zap.String("channel", p.Segment.GetInsertChannel()),
			zap.Int64("from", p.From),
			zap.Int64("to", p.To))
		task.SetPriority(GetTaskPriorityFromWeight(p.Weight))
		ret = append(ret, task)
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
		task, err := task.NewChannelTask(ctx, timeout, checkerID, p.Channel.GetCollectionID(), p.ReplicaID, actions...)
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
		task.SetPriority(GetTaskPriorityFromWeight(p.Weight))
		ret = append(ret, task)
	}
	return ret
}

func PrintNewBalancePlans(collectionID int64, replicaID int64, segmentPlans []SegmentAssignPlan,
	channelPlans []ChannelAssignPlan) {
	balanceInfo := fmt.Sprintf("%s{collectionID:%d, replicaID:%d, ", InfoPrefix, collectionID, replicaID)
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
	channelManager *meta.ChannelDistManager) {
	distInfo := fmt.Sprintf("%s {collectionID:%d, replicaID:%d, ", InfoPrefix, replica.CollectionID, replica.GetID())
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
	distInfo += "]\n"
	//2. print normal nodes segment distribution
	distInfo += "[normalNodesSegmentDist:"
	for normalNodeID, normalNodeSegments := range nodeSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", normalNodeID)
		distInfo += "loaded-segments:["
		nodeRowSum := int64(0)
		for _, normalSegment := range normalNodeSegments {
			distInfo += fmt.Sprintf("[segmentID: %d, rowCount: %d] ",
				normalSegment.GetID(), normalSegment.GetNumOfRows())
			nodeRowSum += normalSegment.GetNumOfRows()
		}
		distInfo += fmt.Sprintf("] nodeRowSum:%d]", nodeRowSum)
	}
	distInfo += "]\n"

	//3. print stopping nodes channel distribution
	distInfo += "[stoppingNodesChannelDist:"
	for stoppingNodeID := range stoppingNodesSegments {
		stoppingNodeChannels := channelManager.GetByNode(stoppingNodeID)
		distInfo += fmt.Sprintf("[nodeID:%d, count:%d,", stoppingNodeID, len(stoppingNodeChannels))
		distInfo += "channels:["
		for _, stoppingChan := range stoppingNodeChannels {
			distInfo += fmt.Sprintf("%s,", stoppingChan.GetChannelName())
		}
		distInfo += "]]"
	}
	distInfo += "]\n"

	//4. print normal nodes channel distribution
	distInfo += "[normalNodesChannelDist:"
	for normalNodeID := range nodeSegments {
		normalNodeChannels := channelManager.GetByNode(normalNodeID)
		distInfo += fmt.Sprintf("[nodeID:%d, count:%d,", normalNodeID, len(normalNodeChannels))
		distInfo += "channels:["
		for _, normalNodeChan := range normalNodeChannels {
			distInfo += fmt.Sprintf("%s,", normalNodeChan.GetChannelName())
		}
		distInfo += "]]"
	}
	distInfo += "]\n"

	log.Info(distInfo)
}
