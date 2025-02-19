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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const MaxSegmentNumPerGetIndexInfoRPC = 1024

var _ Checker = (*IndexChecker)(nil)

// IndexChecker perform segment index check.
type IndexChecker struct {
	*checkerActivation
	meta    *meta.Meta
	dist    *meta.DistributionManager
	broker  meta.Broker
	nodeMgr *session.NodeManager

	targetMgr meta.TargetManagerInterface
}

func NewIndexChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	nodeMgr *session.NodeManager,
	targetMgr meta.TargetManagerInterface,
) *IndexChecker {
	return &IndexChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		broker:            broker,
		nodeMgr:           nodeMgr,
		targetMgr:         targetMgr,
	}
}

func (c *IndexChecker) ID() utils.CheckerType {
	return utils.IndexChecker
}

func (c *IndexChecker) Description() string {
	return "SegmentChecker checks index state change of segments and generates load index task"
}

func (c *IndexChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}
	collectionIDs := c.meta.CollectionManager.GetAll(ctx)
	var tasks []task.Task

	for _, collectionID := range collectionIDs {
		indexInfos, err := c.broker.ListIndexes(ctx, collectionID)
		if err != nil {
			log.Warn("failed to list indexes", zap.Int64("collection", collectionID), zap.Error(err))
			continue
		}

		collection := c.meta.CollectionManager.GetCollection(ctx, collectionID)
		if collection == nil {
			log.Warn("collection released during check index", zap.Int64("collection", collectionID))
			continue
		}
		replicas := c.meta.ReplicaManager.GetByCollection(ctx, collectionID)
		for _, replica := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, collection, replica, indexInfos)...)
		}
	}

	return tasks
}

func (c *IndexChecker) checkReplica(ctx context.Context, collection *meta.Collection, replica *meta.Replica, indexInfos []*indexpb.IndexInfo) []task.Task {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collection.GetCollectionID()),
	)
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

		missing := c.checkSegment(segment, indexInfos)
		if len(missing) > 0 {
			targets[segment.GetID()] = missing
			idSegments[segment.GetID()] = segment
		}
	}

	segmentsToUpdate := typeutil.NewSet[int64]()
	for _, segmentIDs := range lo.Chunk(lo.Keys(idSegments), MaxSegmentNumPerGetIndexInfoRPC) {
		segmentIndexInfos, err := c.broker.GetIndexInfo(ctx, collection.GetCollectionID(), segmentIDs...)
		if err != nil {
			log.Warn("failed to get indexInfo for segments", zap.Int64s("segmentIDs", segmentIDs), zap.Error(err))
			continue
		}
		for segmentID, segmentIndexInfo := range segmentIndexInfos {
			fields := targets[segmentID]
			missingFields := typeutil.NewSet(fields...)
			for _, fieldIndexInfo := range segmentIndexInfo {
				if missingFields.Contain(fieldIndexInfo.GetFieldID()) &&
					fieldIndexInfo.GetEnableIndex() &&
					len(fieldIndexInfo.GetIndexFilePaths()) > 0 {
					segmentsToUpdate.Insert(segmentID)
				}
			}
		}
	}

	tasks = lo.FilterMap(segmentsToUpdate.Collect(), func(segmentID int64, _ int) (task.Task, bool) {
		return c.createSegmentUpdateTask(ctx, idSegments[segmentID], replica)
	})

	return tasks
}

func (c *IndexChecker) checkSegment(segment *meta.Segment, indexInfos []*indexpb.IndexInfo) (fieldIDs []int64) {
	var result []int64
	for _, indexInfo := range indexInfos {
		fieldID, indexID := indexInfo.FieldID, indexInfo.IndexID
		info, ok := segment.IndexInfo[indexID]
		if !ok {
			result = append(result, fieldID)
			continue
		}
		if indexID != info.GetIndexID() || !info.GetEnableIndex() {
			result = append(result, fieldID)
		}
	}
	return result
}

func (c *IndexChecker) createSegmentUpdateTask(ctx context.Context, segment *meta.Segment, replica *meta.Replica) (task.Task, bool) {
	action := task.NewSegmentActionWithScope(segment.Node, task.ActionTypeUpdate, segment.GetInsertChannel(), segment.GetID(), querypb.DataScope_Historical, int(segment.GetNumOfRows()))
	t, err := task.NewSegmentTask(
		ctx,
		params.Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
		c.ID(),
		segment.GetCollectionID(),
		replica,
		action,
	)
	if err != nil {
		log.Warn("create segment update task failed",
			zap.Int64("collection", segment.GetCollectionID()),
			zap.String("channel", segment.GetInsertChannel()),
			zap.Int64("node", segment.Node),
			zap.Error(err),
		)
		return nil, false
	}
	// index task shall have lower or equal priority than balance task
	t.SetPriority(task.TaskPriorityLow)
	t.SetReason("missing index")
	return t, true
}
