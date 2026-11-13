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

package compaction

import (
	"container/list"
	"context"
	"errors"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func (m *CompactionTriggerManager) reconcileTargets(ctx context.Context) {
	events, err := m.targetReconciler.Reconcile(ctx)
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "compaction target observation failed", mlog.Err(err))
		return
	}
	views := append(events[policy.TriggerTypeSort], events[policy.TriggerTypeSingle]...)
	m.submitTargetViews(ctx, views)
}

func (m *CompactionTriggerManager) submitTargetViews(ctx context.Context, views []view.TargetView) {
	byCollection := make(map[int64]*planCollection)
	byGroup := make(map[view.CompactionGroupLabel]*planGroup)
	var collections list.List
	planCount := 0
	for _, targetView := range views {
		plans, err := targetView.Plan()
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "could not plan compaction target view", mlog.Err(err))
			continue
		}
		if len(plans) == 0 {
			continue
		}
		metrics.DataCoordCompactionAdmission.WithLabelValues(ProducerTBC.String(), admissionTaskType(plans[0].GetType()), "plan", "").Add(float64(len(plans)))
		planCount += len(plans)
		label := *targetView.GetGroupLabel()
		collection := byCollection[label.CollectionID]
		if collection == nil {
			collection = &planCollection{}
			byCollection[label.CollectionID] = collection
			collections.PushBack(collection)
		}
		group := byGroup[label]
		if group == nil {
			group = &planGroup{}
			byGroup[label] = group
			collection.groups.PushBack(group)
		}
		group.pools.PushBack(&planPool{plans: plans})
	}

	limit, reason := m.inspector.attemptLimit(ctx, ProducerTBC, planCount)
	if planCount == 0 {
		return
	}
	if planCount > limit {
		defer m.inspector.recordAdmissionDeferral(ProducerTBC, reason)
	}
	if limit == 0 {
		for _, group := range byGroup {
			for node := group.pools.Front(); node != nil; node = node.Next() {
				pool := node.Value.(*planPool)
				metrics.DataCoordCompactionAdmission.WithLabelValues(ProducerTBC.String(), admissionTaskType(pool.plans[0].GetType()), "selection", reason).Add(float64(len(pool.plans)))
			}
		}
		mlog.RatedInfo(ctx, rate.Limit(1), "compaction target plans deferred before selection",
			mlog.Int("planned", planCount), mlog.String("reason", reason))
		return
	}

	attempts, enqueued := 0, 0
	for attempts < limit && ctx.Err() == nil {
		collectionNode := collections.Front()
		collection := collectionNode.Value.(*planCollection)
		groupNode := collection.groups.Front()
		group := groupNode.Value.(*planGroup)
		poolNode := group.pools.Front()
		pool := poolNode.Value.(*planPool)
		plan := pool.plans[0]
		pool.plans = pool.plans[1:]
		advancePlans(&group.pools, poolNode, len(pool.plans) > 0)
		advancePlans(&collection.groups, groupNode, group.pools.Len() > 0)
		advancePlans(&collections, collectionNode, collection.groups.Len() > 0)

		attempts++
		err := m.SubmitTBC(ctx, plan)
		if errors.Is(err, ErrAdmissionLimited) {
			break
		}
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "compaction target plan submission failed",
				mlog.FieldCollectionID(plan.GetCollectionID()), mlog.Int64s("inputSegments", plan.GetInputSegments()), mlog.Err(err))
			continue
		}
		enqueued++
	}
	mlog.RatedInfo(ctx, rate.Limit(1), "compaction target submission round",
		mlog.Int("planned", planCount), mlog.Int("attemptLimit", limit), mlog.Int("attempted", attempts),
		mlog.Int("enqueued", enqueued), mlog.Int("unselected", planCount-attempts))
}

// SubmitTBC fills the selected plan's identity and execution fields using
// its original estimate, schema, TTL and size parameters.
func (m *CompactionTriggerManager) SubmitTBC(ctx context.Context, plan *datapb.CompactionTask) error {
	const metadataIDs = 2 // PlanID and TriggerID
	block, err := createCompactionIDBlock(m.allocator, plan.GetOutputSegmentCountHint(), metadataIDs)
	if err != nil {
		return err
	}

	task := proto.CloneOf(plan)
	task.PlanID, err = block.take()
	if err != nil {
		return err
	}
	task.TriggerID, err = block.take()
	if err != nil {
		return err
	}
	task.PreAllocatedSegmentIDs = block.segmentIDRange()
	task.State = datapb.CompactionTaskState_pipelining
	task.LastStateStartTime = time.Now().Unix()
	return m.inspector.EnqueueCompaction(ctx, task, AdmissionOptions{Producer: ProducerTBC})
}

type planCollection struct{ groups list.List }
type planGroup struct{ pools list.List }
type planPool struct{ plans []*datapb.CompactionTask }

// Move a collection, group or compatible pool behind its peers while it has
// work, otherwise retire it for the rest of the round.
func advancePlans(queue *list.List, node *list.Element, hasPlans bool) {
	if hasPlans {
		queue.MoveToBack(node)
	} else {
		queue.Remove(node)
	}
}
