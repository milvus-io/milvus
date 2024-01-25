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

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ Checker = (*IndexChecker)(nil)

// IndexChecker perform segment index check.
type IndexChecker struct {
	meta    *meta.Meta
	dist    *meta.DistributionManager
	broker  meta.Broker
	nodeMgr *session.NodeManager
}

func NewIndexChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	nodeMgr *session.NodeManager,
) *IndexChecker {
	return &IndexChecker{
		meta:    meta,
		dist:    dist,
		broker:  broker,
		nodeMgr: nodeMgr,
	}
}

func (c *IndexChecker) ID() task.Source {
	return indexChecker
}

func (c *IndexChecker) Description() string {
	return "SegmentChecker checks index state change of segments and generates load index task"
}

func (c *IndexChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	var tasks []task.Task

	for _, collectionID := range collectionIDs {
		collection := c.meta.CollectionManager.GetCollection(collectionID)
		if collection == nil {
			log.Warn("collection released during check index", zap.Int64("collection", collectionID))
			continue
		}
		replicas := c.meta.ReplicaManager.GetByCollection(collectionID)
		for _, replica := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, collection, replica)...)
		}
	}

	return tasks
}

func (c *IndexChecker) checkReplica(ctx context.Context, collection *meta.Collection, replica *meta.Replica) []task.Task {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collection.GetCollectionID()),
	)
	var tasks []task.Task

	segments := c.getSealedSegmentsDist(replica)
	idSegments := make(map[int64]*meta.Segment)

	targets := make(map[int64][]int64) // segmentID => FieldID
	for _, segment := range segments {
		// skip update index in stopping node
		if ok, _ := c.nodeMgr.IsStoppingNode(segment.Node); ok {
			continue
		}
		missing := c.checkSegment(ctx, segment, collection)
		if len(missing) > 0 {
			targets[segment.GetID()] = missing
			idSegments[segment.GetID()] = segment
		}
	}

	segmentsToUpdate := typeutil.NewSet[int64]()
	for segment, fields := range targets {
		missingFields := typeutil.NewSet(fields...)
		infos, err := c.broker.GetIndexInfo(ctx, collection.GetCollectionID(), segment)
		if err != nil {
			log.Warn("failed to get indexInfo for segment", zap.Int64("segmentID", segment), zap.Error(err))
			continue
		}
		for _, info := range infos {
			if missingFields.Contain(info.GetFieldID()) &&
				info.GetEnableIndex() &&
				len(info.GetIndexFilePaths()) > 0 {
				segmentsToUpdate.Insert(segment)
			}
		}
	}

	tasks = lo.FilterMap(segmentsToUpdate.Collect(), func(segmentID int64, _ int) (task.Task, bool) {
		return c.createSegmentUpdateTask(ctx, idSegments[segmentID], replica)
	})

	return tasks
}

func (c *IndexChecker) checkSegment(ctx context.Context, segment *meta.Segment, collection *meta.Collection) (fieldIDs []int64) {
	var result []int64
	for fieldID, indexID := range collection.GetFieldIndexID() {
		info, ok := segment.IndexInfo[fieldID]
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

func (c *IndexChecker) getSealedSegmentsDist(replica *meta.Replica) []*meta.Segment {
	var ret []*meta.Segment
	for _, node := range replica.GetNodes() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func (c *IndexChecker) createSegmentUpdateTask(ctx context.Context, segment *meta.Segment, replica *meta.Replica) (task.Task, bool) {
	action := task.NewSegmentActionWithScope(segment.Node, task.ActionTypeUpdate, segment.GetInsertChannel(), segment.GetID(), querypb.DataScope_Historical)
	t, err := task.NewSegmentTask(
		ctx,
		params.Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
		c.ID(),
		segment.GetCollectionID(),
		replica.GetID(),
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
