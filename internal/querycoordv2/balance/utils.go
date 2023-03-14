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
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
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
