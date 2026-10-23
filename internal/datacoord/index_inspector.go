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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type indexInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	notifyIndexChan     chan int64
	notifiedIndexSegsCh chan struct{}
	pendingIndexMu      sync.Mutex
	// notifiedIndexSegs holds the segments notified since the last round; a
	// notification round processes exactly these.
	notifiedIndexSegs typeutil.UniqueSet
	// pendingIndexSegs holds the segments to retry (startup scan, failed or
	// deferred attempts); only the ticker processes them.
	pendingIndexSegs typeutil.UniqueSet

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
		notifiedIndexSegsCh:       make(chan struct{}, 1),
		notifiedIndexSegs:         typeutil.NewUniqueSet(),
		pendingIndexSegs:          typeutil.NewUniqueSet(),
		scheduler:                 scheduler,
		allocator:                 allocator,
		handler:                   handler,
		storageCli:                storageCli,
		indexEngineVersionManager: indexEngineVersionManager,
	}
}

// notifyIndexSegments queues segments for the next notification round.
func (i *indexInspector) notifyIndexSegments(segIDs ...UniqueID) {
	if len(segIDs) == 0 {
		return
	}

	i.pendingIndexMu.Lock()
	defer i.pendingIndexMu.Unlock()
	i.notifiedIndexSegs.Insert(segIDs...)
	select {
	case i.notifiedIndexSegsCh <- struct{}{}:
	default:
	}
}

// requeuePendingIndexSegments keeps segments for the ticker to retry.
func (i *indexInspector) requeuePendingIndexSegments(segIDs ...UniqueID) {
	i.pendingIndexMu.Lock()
	defer i.pendingIndexMu.Unlock()
	i.pendingIndexSegs.Insert(segIDs...)
}

// popIndexSegments takes the notified segments, plus the pending ones when
// withPending is set. A notified segment supersedes its pending retry.
func (i *indexInspector) popIndexSegments(withPending bool) []UniqueID {
	i.pendingIndexMu.Lock()
	segIDs := i.notifiedIndexSegs.Collect()
	i.notifiedIndexSegs = typeutil.NewUniqueSet()
	if !withPending {
		i.pendingIndexSegs.Remove(segIDs...)
		i.pendingIndexMu.Unlock()
		return segIDs
	}
	pending := i.pendingIndexSegs
	i.pendingIndexSegs = typeutil.NewUniqueSet()
	i.pendingIndexMu.Unlock()

	// The swapped-out backlog is owned here, so merge it outside the lock.
	pending.Insert(segIDs...)
	return pending.Collect()
}

func (i *indexInspector) Start() {
	// Become the buildIndexCh consumer before any producer starts.
	setBuildIndexConsumer(i.ctx.Done())
	i.wg.Add(1)
	go i.forwardBuildIndexCh(i.ctx)
	i.reloadFromMeta()
	i.requeuePendingIndexSegments(i.getUnIndexTaskSegments(i.ctx)...)
	i.wg.Add(1)
	go i.createIndexForSegmentLoop(i.ctx)
}

func (i *indexInspector) Stop() {
	i.cancel()
	i.wg.Wait()
}

func (i *indexInspector) createIndexForSegmentLoop(ctx context.Context) {
	mlog.Info(ctx, "start create index for segment loop...",
		mlog.Int64("TaskCheckInterval", Params.DataCoordCfg.TaskCheckInterval.GetAsInt64()))
	defer i.wg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	reconcileTicker := time.NewTicker(getIndexReconcileInterval())
	defer reconcileTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			mlog.Warn(ctx, "DataCoord context done, exit...")
			return
		case <-ticker.C:
			i.processIndexSegments(ctx, true)
		case <-i.notifiedIndexSegsCh:
			i.processIndexSegments(ctx, false)
		case <-reconcileTicker.C:
			i.notifyIndexSegments(i.getUnIndexTaskSegments(ctx)...)
		case collectionID := <-i.notifyIndexChan:
			mlog.Info(ctx, "receive create index notify", mlog.FieldCollectionID(collectionID))
			i.notifyIndexSegments(i.getUnIndexTaskSegments(ctx, WithCollection(collectionID))...)
		}
	}
}

// forwardBuildIndexCh moves segments from buildIndexCh into the notified set.
// It runs apart from createIndexForSegmentLoop so that a sender waiting on a
// full channel never waits for a create-index round to finish.
func (i *indexInspector) forwardBuildIndexCh(ctx context.Context) {
	defer i.wg.Done()
	ch := getBuildIndexChSingleton()
	for {
		select {
		case <-ctx.Done():
			return
		case segID := <-ch:
			// Take what is already buffered too, so a burst costs one log line
			// and one wake-up of the create-index loop.
			segIDs := []UniqueID{segID}
		drain:
			for len(segIDs) < cap(ch) {
				select {
				case segID = <-ch:
					segIDs = append(segIDs, segID)
				default:
					break drain
				}
			}
			mlog.Info(ctx, "receive segments to build index", mlog.Int64s("segmentIDs", segIDs))
			i.notifyIndexSegments(segIDs...)
		}
	}
}

func getIndexReconcileInterval() time.Duration {
	checkInterval := Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second)
	return checkInterval * 10
}

// getUnIndexTaskSegments returns the IDs of the segments that can build index
// now but still miss an index task.
func (i *indexInspector) getUnIndexTaskSegments(ctx context.Context, filters ...SegmentFilter) []UniqueID {
	sortCompactionEnabled := enableSortCompaction()
	externalCollections := make(map[UniqueID]bool)
	segmentFilters := append([]SegmentFilter{}, filters...)
	segmentFilters = append(segmentFilters, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		if !isFlush(segment) {
			return false
		}
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			return false
		}
		if sortCompactionEnabled && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
			isExternal, ok := externalCollections[segment.CollectionID]
			if !ok {
				isExternal = i.isExternalCollection(segment.CollectionID)
				externalCollections[segment.CollectionID] = isExternal
			}
			return isExternal
		}
		return true
	}))
	segmentIDsByCollection := make(map[UniqueID][]UniqueID)
	for _, segment := range i.meta.SelectSegments(ctx, segmentFilters...) {
		segmentIDsByCollection[segment.CollectionID] = append(segmentIDsByCollection[segment.CollectionID], segment.GetID())
	}

	var unindexedIDs []UniqueID
	for collectionID, segIDs := range segmentIDsByCollection {
		unindexedIDs = append(unindexedIDs, i.meta.indexMeta.GetUnIndexedSegmentIDsForIndexTask(collectionID, segIDs).Collect()...)
	}
	return unindexedIDs
}

func (i *indexInspector) processIndexSegments(ctx context.Context, withPending bool) {
	segIDs := i.popIndexSegments(withPending)
	if len(segIDs) == 0 {
		return
	}

	segmentsByCollection := make(map[UniqueID][]*SegmentInfo)
	for _, segID := range segIDs {
		segment := i.meta.GetSegment(ctx, segID)
		if segment == nil {
			mlog.Warn(ctx, "segment is not exist, no need to build index", mlog.FieldSegmentID(segID))
			continue
		}
		if !i.canCreateIndexForSegmentNow(segment) {
			continue
		}
		segmentsByCollection[segment.CollectionID] = append(segmentsByCollection[segment.CollectionID], segment)
	}

	for collectionID, segments := range segmentsByCollection {
		i.processIndexSegmentsForCollection(ctx, collectionID, segments)
	}
}

func (i *indexInspector) processIndexSegmentsForCollection(ctx context.Context, collectionID UniqueID, segments []*SegmentInfo) {
	segIDs := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		segIDs = append(segIDs, segment.GetID())
	}
	unindexedIDs := i.meta.indexMeta.GetUnIndexedSegmentIDsForIndexTask(collectionID, segIDs)
	if unindexedIDs.Len() == 0 {
		return
	}

	processedIDs := make([]UniqueID, 0, unindexedIDs.Len())
	for _, segment := range segments {
		segID := segment.GetID()
		if !unindexedIDs.Contain(segID) {
			continue
		}
		if err := i.createIndexesForSegment(ctx, segment); err != nil {
			mlog.Warn(ctx, "create index for segment fail, wait for retry",
				mlog.FieldSegmentID(segID),
				mlog.Err(err))
			i.requeuePendingIndexSegments(segID)
			continue
		}
		processedIDs = append(processedIDs, segID)
	}

	stillUnindexedIDs := i.meta.indexMeta.GetUnIndexedSegmentIDsForIndexTask(collectionID, processedIDs)
	i.requeuePendingIndexSegments(stillUnindexedIDs.Collect()...)
}

func (i *indexInspector) canCreateIndexForSegmentNow(segment *SegmentInfo) bool {
	if !isFlush(segment) {
		return false
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if enableSortCompaction() && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() && !i.isExternalCollection(segment.CollectionID) {
		return false
	}
	return true
}

func (i *indexInspector) createIndexesForSegment(ctx context.Context, segment *SegmentInfo) error {
	if enableSortCompaction() && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() && !i.isExternalCollection(segment.CollectionID) {
		mlog.Debug(ctx, "segment is not sorted by pk, skip create indexes", mlog.FieldSegmentID(segment.GetID()))
		return nil
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		mlog.Debug(ctx, "segment is level zero, skip create indexes", mlog.FieldSegmentID(segment.GetID()))
		return nil
	}

	indexes := i.meta.indexMeta.GetIndexesForCollection(segment.CollectionID, "")
	indexIDToSegIndexes := i.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)

	for _, index := range indexes {
		if _, ok := indexIDToSegIndexes[index.IndexID]; ok {
			continue
		}
		collection, ok := i.canCreateIndexForSegment(ctx, segment, index)
		if !ok {
			continue
		}
		if err := i.createIndexForSegment(ctx, collection, segment, index.IndexID); err != nil {
			mlog.Warn(ctx, "create index for segment fail", mlog.FieldSegmentID(segment.ID),
				mlog.FieldIndexID(index.IndexID))
			return err
		}
	}
	return nil
}

// canCreateIndexForSegment reports whether the segment is ready to build the
// index, returning the resolved collection so the caller does not have to look
// it up again. The schema is resolved through the handler (lazy-loading on
// cache miss, e.g. right after a datacoord restart); the check fails closed,
// deferring to the next inspection round on an unresolvable or inconsistent
// view.
func (i *indexInspector) canCreateIndexForSegment(ctx context.Context, segment *SegmentInfo, index *model.Index) (*collectionInfo, bool) {
	collection, err := i.handler.GetCollection(ctx, segment.CollectionID)
	if err != nil || collection == nil || collection.Schema == nil {
		mlog.Warn(ctx, "cannot resolve collection schema, defer index build",
			mlog.FieldSegmentID(segment.ID), mlog.FieldFieldID(index.FieldID), mlog.FieldIndexID(index.IndexID), mlog.Err(err))
		return nil, false
	}
	// Function outputs are materialized by schema-bump reconciliation, which
	// advances the segment schema version. A segment behind the collection schema
	// version may lack them, so defer the whole segment until it catches up.
	if len(collection.Schema.GetFunctions()) > 0 &&
		segment.GetSchemaVersion() < collection.Schema.GetVersion() {
		mlog.Debug(ctx, "segment schema behind collection, function outputs may be unmaterialized, defer index build",
			mlog.FieldSegmentID(segment.ID), mlog.FieldFieldID(index.FieldID), mlog.FieldIndexID(index.IndexID),
			mlog.Int32("segmentSchemaVersion", segment.GetSchemaVersion()), mlog.Int32("collectionSchemaVersion", collection.Schema.GetVersion()))
		return nil, false
	}
	if typeutil.GetFieldByID(collection.Schema, index.FieldID) == nil {
		mlog.Warn(ctx, "indexed field not found in cached collection schema, defer index build",
			mlog.FieldSegmentID(segment.ID), mlog.FieldFieldID(index.FieldID), mlog.FieldIndexID(index.IndexID))
		return nil, false
	}
	return collection, true
}

func (i *indexInspector) createIndexForSegment(ctx context.Context, collection *collectionInfo, segment *SegmentInfo, indexID UniqueID) error {
	mlog.Info(ctx, "create index for segment", mlog.FieldSegmentID(segment.ID), mlog.FieldIndexID(indexID))
	buildID, err := i.allocator.AllocID(context.Background())
	if err != nil {
		return err
	}

	indexParams := i.meta.indexMeta.GetIndexParams(segment.CollectionID, indexID)
	indexType := GetIndexType(indexParams)
	isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
	fieldID := i.meta.indexMeta.GetFieldIDByIndexID(segment.CollectionID, indexID)
	fieldSize := i.estimateIndexFieldSize(ctx, collection, segment, fieldID)
	taskSlot := calculateIndexTaskSlot(fieldSize, segment.NumOfRows, indexParams)

	// rewrite the index type if needed, and this final index type will be persisted in the meta
	if isVectorIndex && Params.KnowhereConfig.Enable.GetAsBool() {
		var err error
		indexParams, err = Params.KnowhereConfig.UpdateIndexParams(indexType, paramtable.BuildStage, indexParams)
		if err != nil {
			return err
		}
	}
	newIndexType := GetIndexType(indexParams)
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
	if err = i.meta.indexMeta.AddSegmentIndex(ctx, segIndex); err != nil {
		return err
	}
	i.scheduler.Enqueue(newIndexBuildTask(model.CloneSegmentIndex(segIndex),
		taskSlot,
		i.meta,
		i.handler,
		i.storageCli,
		i.indexEngineVersionManager))
	mlog.Info(ctx, "indexInspector create index for segment success",
		mlog.FieldSegmentID(segment.ID),
		mlog.FieldIndexID(indexID),
		mlog.FieldFieldID(fieldID),
		mlog.Int64("segment size", segment.getSegmentSize()),
		mlog.Int64("field size", fieldSize),
		mlog.Int64("task slot", taskSlot))
	return nil
}

func (i *indexInspector) isExternalCollection(collectionID int64) bool {
	coll := i.meta.GetCollection(collectionID)
	return coll != nil && coll.IsExternal()
}

// estimateIndexFieldSize returns the amount of data the index build task reads
// for fieldID, which drives the task slot estimation.
//
// A manifest-backed StorageV3 index build projects the indexed column, while
// the binlog size covers the whole column group holding it. StorageV2 reads the
// whole group and therefore keeps the binlog size. An unresolved collection
// also keeps that previous conservative behavior.
//
// V3 segments do not persist their FieldBinlog arrays (the catalog skips the
// per-FieldBinlog KVs, see isV3Segment), so the per-column attribution only
// applies to segments flushed or refreshed during the current DataCoord
// process lifetime; a segment recovered after a restart carries no binlogs and
// keeps the whole-segment size until it is rewritten.
func (i *indexInspector) estimateIndexFieldSize(ctx context.Context, coll *collectionInfo, segment *SegmentInfo, fieldID int64) int64 {
	binlogSize := segment.getFieldBinlogSize(fieldID)
	if !supportsFieldProjection(segment) {
		return binlogSize
	}
	if len(segment.GetBinlogs()) == 0 {
		// a manifest-backed segment recovered from the catalog: there is
		// nothing to attribute, keep the whole-segment size
		mlog.Debug(ctx, "no binlogs recorded for manifest-backed segment, fallback to segment size for index task slot",
			mlog.FieldSegmentID(segment.GetID()),
			mlog.Int64("binlogSize", binlogSize))
		return binlogSize
	}
	if coll == nil {
		// The collection is not cached yet, e.g. task recovery right after a
		// restart, before the async cache reload catches up. Keep the
		// conservative binlog size instead of loading through rootcoord here.
		mlog.Debug(ctx, "collection not cached, fallback to binlog size for index task slot",
			mlog.FieldCollectionID(segment.GetCollectionID()),
			mlog.FieldSegmentID(segment.GetID()),
			mlog.Int64("binlogSize", binlogSize))
		return binlogSize
	}

	fieldSize, err := estimateFieldsReadSize(coll.Schema, segment, []int64{fieldID})
	if err != nil {
		mlog.Warn(ctx, "failed to estimate index field size, fallback to binlog size",
			mlog.FieldSegmentID(segment.GetID()),
			mlog.FieldFieldID(fieldID),
			mlog.Int64("binlogSize", binlogSize),
			mlog.Err(err))
		return binlogSize
	}
	return fieldSize
}

func (i *indexInspector) reloadFromMeta() {
	segments := i.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range i.meta.indexMeta.GetSegmentIndexes(segment.GetCollectionID(), segment.ID) {
			if segIndex.IsDeleted || (segIndex.IndexState != commonpb.IndexState_Unissued &&
				segIndex.IndexState != commonpb.IndexState_Retry &&
				segIndex.IndexState != commonpb.IndexState_InProgress) {
				continue
			}

			indexParams := i.meta.indexMeta.GetIndexParams(segment.CollectionID, segIndex.IndexID)
			fieldID := i.meta.indexMeta.GetFieldIDByIndexID(segment.CollectionID, segIndex.IndexID)
			// Only the local cache is consulted: recovery runs synchronously in
			// Start(), so it must not load collections through rootcoord. On a
			// cold cache the estimation falls back to the conservative binlog
			// size, which is the pre-optimization behavior.
			fieldSize := i.estimateIndexFieldSize(i.ctx, i.meta.GetCollection(segment.GetCollectionID()), segment, fieldID)
			taskSlot := calculateIndexTaskSlot(fieldSize, segment.NumOfRows, indexParams)

			i.scheduler.Enqueue(newIndexBuildTask(
				model.CloneSegmentIndex(segIndex),
				taskSlot,
				i.meta,
				i.handler,
				i.storageCli,
				i.indexEngineVersionManager,
			))
		}
	}
}
