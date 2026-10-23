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

package index

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type IndexInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	notifyIndexChan chan int64

	meta                      *meta.Meta
	indexMeta                 *IndexMeta
	scheduler                 task.GlobalScheduler
	allocator                 allocator.Allocator
	Handler                   meta.Handler
	storageCli                storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
}

func NewIndexInspector(
	ctx context.Context,
	notifyIndexChan chan int64,
	mt *meta.Meta,
	indexMeta *IndexMeta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
	handler meta.Handler,
	storageCli storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
) *IndexInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &IndexInspector{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      mt,
		indexMeta:                 indexMeta,
		notifyIndexChan:           notifyIndexChan,
		scheduler:                 scheduler,
		allocator:                 allocator,
		Handler:                   handler,
		storageCli:                storageCli,
		indexEngineVersionManager: indexEngineVersionManager,
	}
}

func (i *IndexInspector) Start() {
	i.reloadFromMeta()
	i.wg.Add(1)
	go i.createIndexForSegmentLoop(i.ctx)
}

func (i *IndexInspector) Stop() {
	i.cancel()
	i.wg.Wait()
}

func (i *IndexInspector) createIndexForSegmentLoop(ctx context.Context) {
	mlog.Info(ctx, "start create index for segment loop...",
		mlog.Int64("TaskCheckInterval", Params.DataCoordCfg.TaskCheckInterval.GetAsInt64()))
	defer i.wg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			mlog.Warn(ctx, "DataCoord context done, exit...")
			return
		case <-ticker.C:
			segments := i.getUnIndexTaskSegments(ctx)
			for _, segment := range segments {
				if err := i.createIndexesForSegment(ctx, segment); err != nil {
					mlog.Warn(ctx, "create index for segment fail, wait for retry", mlog.FieldSegmentID(segment.ID))
					continue
				}
			}
		case collectionID := <-i.notifyIndexChan:
			mlog.Info(ctx, "receive create index notify", mlog.FieldCollectionID(collectionID))
			isExternal := i.isExternalCollection(collectionID)
			segments := i.meta.SelectSegments(ctx, meta.WithCollection(collectionID), meta.SegmentFilterFunc(func(info *meta.SegmentInfo) bool {
				return meta.IsFlush(info) && (!meta.EnableSortCompaction() || info.GetIsSorted() || info.GetIsSortedByNamespace() || isExternal)
			}))
			for _, segment := range segments {
				if err := i.createIndexesForSegment(ctx, segment); err != nil {
					mlog.Warn(ctx, "create index for segment fail, wait for retry", mlog.FieldSegmentID(segment.ID))
					continue
				}
			}
		case segID := <-meta.GetBuildIndexChSingleton():
			mlog.Info(ctx, "receive new flushed segment", mlog.FieldSegmentID(segID))
			segment := i.meta.GetSegment(ctx, segID)
			if segment == nil {
				mlog.Warn(ctx, "segment is not exist, no need to build index", mlog.FieldSegmentID(segID))
				continue
			}
			if err := i.createIndexesForSegment(ctx, segment); err != nil {
				mlog.Warn(ctx, "create index for segment fail, wait for retry", mlog.FieldSegmentID(segment.ID))
				continue
			}
		}
	}
}

func (i *IndexInspector) getUnIndexTaskSegments(ctx context.Context) []*meta.SegmentInfo {
	flushedSegments := i.meta.SelectSegments(ctx, meta.SegmentFilterFunc(meta.IsFlush))

	unindexedSegments := make([]*meta.SegmentInfo, 0)
	for _, segment := range flushedSegments {
		if i.indexMeta.IsUnIndexedSegment(segment.CollectionID, segment.GetID()) {
			unindexedSegments = append(unindexedSegments, segment)
		}
	}
	return unindexedSegments
}

func (i *IndexInspector) createIndexesForSegment(ctx context.Context, segment *meta.SegmentInfo) error {
	if meta.EnableSortCompaction() && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() && !i.isExternalCollection(segment.CollectionID) {
		mlog.Debug(ctx, "segment is not sorted by pk, skip create indexes", mlog.FieldSegmentID(segment.GetID()))
		return nil
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		mlog.Debug(ctx, "segment is level zero, skip create indexes", mlog.FieldSegmentID(segment.GetID()))
		return nil
	}

	indexes := i.indexMeta.GetIndexesForCollection(segment.CollectionID, "")
	indexIDToSegIndexes := i.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)

	for _, index := range indexes {
		if _, ok := indexIDToSegIndexes[index.IndexID]; ok {
			continue
		}
		if !i.CanCreateIndexForSegment(ctx, segment, index) {
			continue
		}
		if err := i.createIndexForSegment(ctx, segment, index.IndexID); err != nil {
			mlog.Warn(ctx, "create index for segment fail", mlog.FieldSegmentID(segment.ID),
				mlog.FieldIndexID(index.IndexID))
			return err
		}
	}
	return nil
}

// canCreateIndexForSegment reports whether the segment is ready to build the
// index. The schema is resolved through the handler (lazy-loading on cache miss,
// e.g. right after a datacoord restart); the check fails closed, deferring to
// the next inspection round on an unresolvable or inconsistent view.
func (i *IndexInspector) CanCreateIndexForSegment(ctx context.Context, segment *meta.SegmentInfo, index *model.Index) bool {
	collection, err := i.Handler.GetCollection(ctx, segment.CollectionID)
	if err != nil || collection == nil || collection.Schema == nil {
		mlog.Warn(ctx, "cannot resolve collection schema, defer index build",
			mlog.FieldSegmentID(segment.ID), mlog.FieldFieldID(index.FieldID), mlog.FieldIndexID(index.IndexID), mlog.Err(err))
		return false
	}
	// Function outputs are materialized by schema-bump reconciliation, which
	// advances the segment schema version. A segment behind the collection schema
	// version may lack them, so defer the whole segment until it catches up.
	if len(collection.Schema.GetFunctions()) > 0 &&
		segment.GetSchemaVersion() < collection.Schema.GetVersion() {
		mlog.Debug(ctx, "segment schema behind collection, function outputs may be unmaterialized, defer index build",
			mlog.FieldSegmentID(segment.ID), mlog.FieldFieldID(index.FieldID), mlog.FieldIndexID(index.IndexID),
			mlog.Int32("segmentSchemaVersion", segment.GetSchemaVersion()), mlog.Int32("collectionSchemaVersion", collection.Schema.GetVersion()))
		return false
	}
	if typeutil.GetFieldByID(collection.Schema, index.FieldID) == nil {
		mlog.Warn(ctx, "indexed field not found in cached collection schema, defer index build",
			mlog.FieldSegmentID(segment.ID), mlog.FieldFieldID(index.FieldID), mlog.FieldIndexID(index.IndexID))
		return false
	}
	return true
}

func (i *IndexInspector) createIndexForSegment(ctx context.Context, segment *meta.SegmentInfo, indexID UniqueID) error {
	mlog.Info(ctx, "create index for segment", mlog.FieldSegmentID(segment.ID), mlog.FieldIndexID(indexID))
	buildID, err := i.allocator.AllocID(context.Background())
	if err != nil {
		return err
	}

	indexParams := i.indexMeta.GetIndexParams(segment.CollectionID, indexID)
	indexType := meta.GetIndexType(indexParams)
	isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
	fieldID := i.indexMeta.GetFieldIDByIndexID(segment.CollectionID, indexID)
	fieldSize := segment.GetFieldBinlogSize(fieldID)
	taskSlot := CalculateIndexTaskSlot(fieldSize, segment.NumOfRows, indexParams)

	// rewrite the index type if needed, and this final index type will be persisted in the meta
	if isVectorIndex && Params.KnowhereConfig.Enable.GetAsBool() {
		var err error
		indexParams, err = Params.KnowhereConfig.UpdateIndexParams(indexType, paramtable.BuildStage, indexParams)
		if err != nil {
			return err
		}
	}
	newIndexType := meta.GetIndexType(indexParams)
	if newIndexType != "" && newIndexType != indexType {
		mlog.Info(ctx, "override index type", mlog.String("indexType", indexType), mlog.String("newIndexType", newIndexType))
		indexType = newIndexType
	}

	segIndex := &model.SegmentIndex{
		SegmentID:             segment.ID,
		CollectionID:          segment.CollectionID,
		PartitionID:           segment.PartitionID,
		NumRows:               segment.NumOfRows,
		IndexID:               indexID,
		BuildID:               buildID,
		CreatedUTCTime:        uint64(time.Now().Unix()),
		WriteHandoff:          false,
		IndexType:             indexType,
		IndexStorePathVersion: i.indexEngineVersionManager.GetClusterMinIndexStorePathVersion(),
	}
	if err = i.indexMeta.AddSegmentIndex(ctx, segIndex); err != nil {
		return err
	}
	i.scheduler.Enqueue(NewIndexBuildTask(model.CloneSegmentIndex(segIndex),
		taskSlot,
		i.meta,
		i.indexMeta,
		i.Handler,
		i.storageCli,
		i.indexEngineVersionManager))
	mlog.Info(ctx, "indexInspector create index for segment success",
		mlog.FieldSegmentID(segment.ID),
		mlog.FieldIndexID(indexID),
		mlog.FieldFieldID(fieldID),
		mlog.Int64("segment size", segment.GetSegmentSize()),
		mlog.Int64("field size", fieldSize),
		mlog.Int64("task slot", taskSlot))
	return nil
}

func (i *IndexInspector) isExternalCollection(collectionID int64) bool {
	coll := i.meta.GetCollection(collectionID)
	return coll != nil && coll.IsExternal()
}

func (i *IndexInspector) reloadFromMeta() {
	segments := i.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range i.indexMeta.GetSegmentIndexes(segment.GetCollectionID(), segment.ID) {
			if segIndex.IsDeleted || (segIndex.IndexState != commonpb.IndexState_Unissued &&
				segIndex.IndexState != commonpb.IndexState_Retry &&
				segIndex.IndexState != commonpb.IndexState_InProgress) {
				continue
			}

			indexParams := i.indexMeta.GetIndexParams(segment.CollectionID, segIndex.IndexID)
			fieldID := i.indexMeta.GetFieldIDByIndexID(segment.CollectionID, segIndex.IndexID)
			fieldSize := segment.GetFieldBinlogSize(fieldID)
			taskSlot := CalculateIndexTaskSlot(fieldSize, segment.NumOfRows, indexParams)

			i.scheduler.Enqueue(NewIndexBuildTask(
				model.CloneSegmentIndex(segIndex),
				taskSlot,
				i.meta,
				i.indexMeta,
				i.Handler,
				i.storageCli,
				i.indexEngineVersionManager,
			))
		}
	}
}
