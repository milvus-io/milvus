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

package datacoord

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type indexInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	notifyIndexChan chan int64

	meta                      *meta
	scheduler                 task.GlobalScheduler
	allocator                 allocator.Allocator
	handler                   Handler
	storageCli                storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
}

func newIndexInspector(
	ctx context.Context,
	notifyIndexChan chan int64,
	meta *meta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
	handler Handler,
	storageCli storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
) *indexInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &indexInspector{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      meta,
		notifyIndexChan:           notifyIndexChan,
		scheduler:                 scheduler,
		allocator:                 allocator,
		handler:                   handler,
		storageCli:                storageCli,
		indexEngineVersionManager: indexEngineVersionManager,
	}
}

func (i *indexInspector) Start() {
	i.reloadFromMeta()
	i.wg.Add(1)
	go i.createIndexForSegmentLoop(i.ctx)
}

func (i *indexInspector) Stop() {
	i.cancel()
	i.wg.Wait()
}

func (i *indexInspector) createIndexForSegmentLoop(ctx context.Context) {
	log := log.Ctx(ctx)
	log.Info("start create index for segment loop...",
		zap.Int64("TaskCheckInterval", Params.DataCoordCfg.TaskCheckInterval.GetAsInt64()))
	defer i.wg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Warn("DataCoord context done, exit...")
			return
		case <-ticker.C:
			segments := i.getUnIndexTaskSegments(ctx)
			for _, segment := range segments {
				if err := i.createIndexesForSegment(ctx, segment); err != nil {
					log.Warn("create index for segment fail, wait for retry", zap.Int64("segmentID", segment.ID))
					continue
				}
			}
		case collectionID := <-i.notifyIndexChan:
			log.Info("receive create index notify", zap.Int64("collectionID", collectionID))
			segments := i.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
				return isFlush(info) && (!Params.DataCoordCfg.EnableStatsTask.GetAsBool() || info.GetIsSorted())
			}))
			for _, segment := range segments {
				if err := i.createIndexesForSegment(ctx, segment); err != nil {
					log.Warn("create index for segment fail, wait for retry", zap.Int64("segmentID", segment.ID))
					continue
				}
			}
		case segID := <-getBuildIndexChSingleton():
			log.Info("receive new flushed segment", zap.Int64("segmentID", segID))
			segment := i.meta.GetSegment(ctx, segID)
			if segment == nil {
				log.Warn("segment is not exist, no need to build index", zap.Int64("segmentID", segID))
				continue
			}
			if err := i.createIndexesForSegment(ctx, segment); err != nil {
				log.Warn("create index for segment fail, wait for retry", zap.Int64("segmentID", segment.ID))
				continue
			}
		}
	}
}

func (i *indexInspector) getUnIndexTaskSegments(ctx context.Context) []*SegmentInfo {
	flushedSegments := i.meta.SelectSegments(ctx, SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg)
	}))

	unindexedSegments := make([]*SegmentInfo, 0)
	for _, segment := range flushedSegments {
		if i.meta.indexMeta.IsUnIndexedSegment(segment.CollectionID, segment.GetID()) {
			unindexedSegments = append(unindexedSegments, segment)
		}
	}
	return unindexedSegments
}

func (i *indexInspector) createIndexesForSegment(ctx context.Context, segment *SegmentInfo) error {
	if Params.DataCoordCfg.EnableStatsTask.GetAsBool() && !segment.GetIsSorted() && !segment.GetIsImporting() {
		log.Ctx(ctx).Debug("segment is not sorted by pk, skip create indexes", zap.Int64("segmentID", segment.GetID()))
		return nil
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		log.Ctx(ctx).Debug("segment is level zero, skip create indexes", zap.Int64("segmentID", segment.GetID()))
		return nil
	}

	indexes := i.meta.indexMeta.GetIndexesForCollection(segment.CollectionID, "")
	indexIDToSegIndexes := i.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
	for _, index := range indexes {
		if _, ok := indexIDToSegIndexes[index.IndexID]; !ok {
			if err := i.createIndexForSegment(ctx, segment, index.IndexID); err != nil {
				log.Ctx(ctx).Warn("create index for segment fail", zap.Int64("segmentID", segment.ID),
					zap.Int64("indexID", index.IndexID))
				return err
			}
		}
	}
	return nil
}

func (i *indexInspector) createIndexForSegment(ctx context.Context, segment *SegmentInfo, indexID UniqueID) error {
	log.Info("create index for segment", zap.Int64("segmentID", segment.ID), zap.Int64("indexID", indexID))
	buildID, err := i.allocator.AllocID(context.Background())
	if err != nil {
		return err
	}
	taskSlot := calculateIndexTaskSlot(segment.getSegmentSize())
	segIndex := &model.SegmentIndex{
		SegmentID:      segment.ID,
		CollectionID:   segment.CollectionID,
		PartitionID:    segment.PartitionID,
		NumRows:        segment.NumOfRows,
		IndexID:        indexID,
		BuildID:        buildID,
		CreatedUTCTime: uint64(time.Now().Unix()),
		WriteHandoff:   false,
	}
	if err = i.meta.indexMeta.AddSegmentIndex(ctx, segIndex); err != nil {
		return err
	}
	i.scheduler.Enqueue(newIndexBuildTask(model.CloneSegmentIndex(segIndex),
		taskSlot,
		i.meta,
		i.handler,
		i.storageCli,
		i.indexEngineVersionManager))
	return nil
}

func (i *indexInspector) reloadFromMeta() {
	segments := i.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range i.meta.indexMeta.GetSegmentIndexes(segment.GetCollectionID(), segment.ID) {
			if segIndex.IsDeleted || segIndex.IndexState == commonpb.IndexState_Finished ||
				segIndex.IndexState == commonpb.IndexState_Failed {
				continue
			}

			i.scheduler.Enqueue(newIndexBuildTask(
				model.CloneSegmentIndex(segIndex),
				calculateIndexTaskSlot(segment.getSegmentSize()),
				i.meta,
				i.handler,
				i.storageCli,
				i.indexEngineVersionManager,
			))
		}
	}
}
