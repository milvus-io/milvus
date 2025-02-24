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
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Checker = (*StatsChecker)(nil)

// StatsChecker perform segment stats index check.
type StatsChecker struct {
	*checkerActivation
	meta    *meta.Meta
	dist    *meta.DistributionManager
	broker  meta.Broker
	nodeMgr *session.NodeManager

	targetMgr meta.TargetManagerInterface
}

func NewStatsChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	nodeMgr *session.NodeManager,
	targetMgr meta.TargetManagerInterface,
) *StatsChecker {
	return &StatsChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		broker:            broker,
		nodeMgr:           nodeMgr,
		targetMgr:         targetMgr,
	}
}

func (c *StatsChecker) ID() utils.CheckerType {
	return utils.StatsChecker
}

func (c *StatsChecker) Description() string {
	return "StatsChecker checks stats state change of segments and generates load stats task"
}

func (c *StatsChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}
	collectionIDs := c.meta.CollectionManager.GetAll(ctx)
	var tasks []task.Task

	for _, collectionID := range collectionIDs {
		resp, err := c.broker.DescribeCollection(ctx, collectionID)
		if err != nil {
			log.Warn("describeCollection during check stats", zap.Int64("collection", collectionID))
			continue
		}
		collection := c.meta.CollectionManager.GetCollection(ctx, collectionID)
		if collection == nil {
			log.Warn("collection released during check stats", zap.Int64("collection", collectionID))
			continue
		}
		replicas := c.meta.ReplicaManager.GetByCollection(ctx, collectionID)
		for _, replica := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, collection, replica, resp)...)
		}
	}

	return tasks
}

func (c *StatsChecker) checkReplica(ctx context.Context, collection *meta.Collection, replica *meta.Replica, resp *milvuspb.DescribeCollectionResponse) []task.Task {
	var tasks []task.Task
	segments := c.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithReplica(replica))
	idSegments := make(map[int64]*meta.Segment)
	roNodeSet := typeutil.NewUniqueSet(replica.GetRONodes()...)
	targets := make(map[int64][]int64) // segmentID => FieldID
	for _, segment := range segments {
		// skip update index in read only node
		if roNodeSet.Contain(segment.Node) {
			continue
		}

		// skip update index for l0 segment
		segmentInTarget := c.targetMgr.GetSealedSegment(ctx, collection.GetCollectionID(), segment.GetID(), meta.CurrentTargetFirst)
		if segmentInTarget == nil || segmentInTarget.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		missing := c.checkSegment(segment, resp)
		if len(missing) > 0 {
			targets[segment.GetID()] = missing
			idSegments[segment.GetID()] = segment
		}
	}

	segmentsToUpdate := typeutil.NewSet[int64]()
	for _, segmentIDs := range lo.Chunk(lo.Keys(targets), MaxSegmentNumPerGetIndexInfoRPC) {
		for _, segmentID := range segmentIDs {
			segmentsToUpdate.Insert(segmentID)
		}
	}

	tasks = lo.FilterMap(segmentsToUpdate.Collect(), func(segmentID int64, _ int) (task.Task, bool) {
		return c.createSegmentUpdateTask(ctx, idSegments[segmentID], replica)
	})

	return tasks
}

func (c *StatsChecker) checkSegment(segment *meta.Segment, resp *milvuspb.DescribeCollectionResponse) (missFieldIDs []int64) {
	var result []int64
	for _, field := range resp.GetSchema().GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		if h.EnableJSONKeyIndex() && paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetAsBool() {
			exists := false
			for i := 0; i < len(segment.JSONIndexField); i++ {
				if segment.JSONIndexField[i] == field.FieldID {
					exists = true
					break
				}
			}

			if !exists {
				result = append(result, field.FieldID)
				continue
			}
		}
	}
	return result
}

func (c *StatsChecker) createSegmentUpdateTask(ctx context.Context, segment *meta.Segment, replica *meta.Replica) (task.Task, bool) {
	action := task.NewSegmentActionWithScope(segment.Node, task.ActionTypeStatsUpdate, segment.GetInsertChannel(), segment.GetID(), querypb.DataScope_Historical, int(segment.GetNumOfRows()))
	t, err := task.NewSegmentTask(
		ctx,
		params.Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
		c.ID(),
		segment.GetCollectionID(),
		replica,
		action,
	)
	if err != nil {
		log.Warn("create segment stats update task failed",
			zap.Int64("collection", segment.GetCollectionID()),
			zap.String("channel", segment.GetInsertChannel()),
			zap.Int64("node", segment.Node),
			zap.Error(err),
		)
		return nil, false
	}
	t.SetPriority(task.TaskPriorityLow)
	t.SetReason("missing json stats")
	return t, true
}
