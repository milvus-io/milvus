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

package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// Task construction may reject individual plans. Release those provisional
// reservations before planning another collection in the same batch.
func reconcileSegmentPlans(ctx context.Context, plans []assign.SegmentAssignPlan, tasks []task.Task) {
	if len(plans) == 0 {
		return
	}
	ledger := assign.PlanningLedgerFromContext(ctx)
	if ledger == nil {
		return
	}
	retained := make(map[assign.PlanningLoadKey]struct{})
	for _, work := range tasks {
		for _, action := range work.Actions() {
			if grow, ok := action.(*task.SegmentAction); ok && grow.Type() == task.ActionTypeGrow {
				retained[assign.PlanningLoadKey{NodeID: grow.Node(), CollectionID: work.CollectionID(), SegmentID: grow.GetSegmentID()}] = struct{}{}
			}
		}
	}
	for _, plan := range plans {
		if _, ok := retained[assign.PlanningLoadKey{NodeID: plan.To, CollectionID: plan.Segment.GetCollectionID(), SegmentID: plan.Segment.GetID()}]; !ok {
			ledger.UndoSegment(plan)
		}
	}
}

func reconcileChannelPlans(ctx context.Context, plans []assign.ChannelAssignPlan, tasks []task.Task) {
	if len(plans) == 0 {
		return
	}
	ledger := assign.PlanningLedgerFromContext(ctx)
	if ledger == nil {
		return
	}
	type channelKey struct {
		collection, node int64
		channel          string
	}
	retained := make(map[channelKey]struct{})
	for _, work := range tasks {
		for _, action := range work.Actions() {
			if grow, ok := action.(*task.ChannelAction); ok && grow.Type() == task.ActionTypeGrow {
				retained[channelKey{work.CollectionID(), grow.Node(), grow.GetShard()}] = struct{}{}
			}
		}
	}
	for _, plan := range plans {
		if _, ok := retained[channelKey{plan.Channel.GetCollectionID(), plan.To, plan.Channel.GetChannelName()}]; !ok {
			ledger.UndoChannel(plan)
		}
	}
}
