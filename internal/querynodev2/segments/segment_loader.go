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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	UsedDiskMemoryRatio      = 4
	UsedDiskMemoryRatioAisaq = 64
)

var errRetryTimerNotified = errors.New("retry timer notified")

type Loader interface {
	// Load loads binlogs, and spawn segments,
	// NOTE: make sure the ref count of the corresponding collection will never go down to 0 during this
	Load(ctx context.Context, collectionID int64, segmentType SegmentType, version int64, segments ...*querypb.SegmentLoadInfo) ([]Segment, error)

	// LoadDeltaLogs load deltalog and write delta data into provided segment.
	// it also executes resource protection logic in case of OOM.
	LoadDeltaLogs(ctx context.Context, segment Segment, loadInfo *querypb.SegmentLoadInfo) error

	// LoadBloomFilterSet loads needed statslog for RemoteSegment.
	LoadBloomFilterSet(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error)

	// GetChunkManager returns the chunk manager for remote storage access.
	GetChunkManager() storage.ChunkManager

	// GetLocalDiskUsage returns the cached size of the local storage directory.
	GetLocalDiskUsage() (int64, error)

	// ReopenSegments update segment data according to new load info.
	ReopenSegments(ctx context.Context,
		loadInfos []*querypb.SegmentLoadInfo,
	) error
}

type requestResourceResult struct {
	Resource          LoadResource
	LogicalResource   LoadResource
	CommittedResource LoadResource
	ConcurrencyLevel  int
}

type LoadResource struct {
	MemorySize uint64
	DiskSize   uint64
}

func (r *LoadResource) Add(resource LoadResource) {
	r.MemorySize += resource.MemorySize
	r.DiskSize += resource.DiskSize
}

func (r *LoadResource) Sub(resource LoadResource) {
	r.MemorySize -= resource.MemorySize
	r.DiskSize -= resource.DiskSize
}

func (r *LoadResource) IsZero() bool {
	return r.MemorySize == 0 && r.DiskSize == 0
}

type resourceEstimateFactor struct {
	memoryUsageFactor               float64
	memoryIndexUsageFactor          float64
	EnableInterminSegmentIndex      bool
	tempSegmentIndexFactor          float64
	deltaDataExpansionFactor        float64
	jsonKeyStatsExpansionFactor     float64
	textIndexExpansionFactor        float64
	TieredEvictionEnabled           bool
	TieredEvictableMemoryCacheRatio float64
	TieredEvictableDiskCacheRatio   float64
	// externalRawDataFactor is the peak-memory safety factor for external
	// segments when tiered eviction is disabled. With tiered eviction enabled,
	// the caching layer reserves transient loading overhead from the sampled
	// external row size, so applying this factor in Go would reserve the same
	// raw-data memory twice. Defaults to 2.0 via paramtable
	// queryNode.externalCollection.rawDataFactor.
	externalRawDataFactor float64
}

func NewLoader(ctx context.Context, manager *Manager, cm storage.ChunkManager) *segmentLoader {
	return NewLoaderWithResourceBudget(manager, cm, NewLoadResourceBudget(ctx))
}

// NewLoaderWithResourceBudget shares node admission with other load paths.
func NewLoaderWithResourceBudget(manager *Manager, cm storage.ChunkManager, budget *LoadResourceBudget) *segmentLoader {
	return &segmentLoader{
		manager:            manager,
		cm:                 cm,
		loadingSegments:    typeutil.NewConcurrentMap[int64, *loadResult](),
		LoadResourceBudget: budget,
	}
}

type loadStatus = int32

const (
	loading loadStatus = iota + 1
	success
	failure
)

type loadResult struct {
	status *atomic.Int32
	cond   *sync.Cond
}

func newLoadResult() *loadResult {
	return &loadResult{
		status: atomic.NewInt32(loading),
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

func (r *loadResult) SetResult(status loadStatus) {
	r.status.CompareAndSwap(loading, status)
	r.cond.Broadcast()
}

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	manager *Manager
	cm      storage.ChunkManager

	// The channel will be closed as the segment loaded
	loadingSegments *typeutil.ConcurrentMap[int64, *loadResult]

	*LoadResourceBudget
}

var _ Loader = (*segmentLoader)(nil)

func (loader *segmentLoader) GetLocalDiskUsage() (int64, error) {
	return loader.duf.GetDiskUsage()
}

func (loader *segmentLoader) Load(ctx context.Context,
	collectionID int64,
	segmentType SegmentType,
	version int64,
	segments ...*querypb.SegmentLoadInfo,
) ([]Segment, error) {
	if len(segments) == 0 {
		mlog.Info(context.TODO(), "no segment to load")
		return nil, nil
	}

	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(collectionID)
		mlog.Warn(context.TODO(), "failed to get collection", mlog.Err(err))
		return nil, err
	}
	for _, segment := range segments {
		configureUseTakeForOutput(segment, collection.Schema())
	}
	// Filter out loaded & loading segments
	infos := loader.prepare(ctx, segmentType, segments...)
	defer loader.unregister(infos...)

	// continue to wait other task done
	mlog.Info(context.TODO(), "start loading...", mlog.Int("segmentNum", len(segments)), mlog.Int("afterFilter", len(infos)))

	var err error
	var requestResourceResult requestResourceResult

	// Check memory & storage limit
	// no need to check resource for lazy load here
	requestResourceResult, err = loader.requestResource(ctx, infos...)
	if err != nil {
		mlog.Warn(context.TODO(), "request resource failed", mlog.Err(err))
		return nil, err
	}
	defer loader.freeRequestResource(requestResourceResult)

	newSegments := typeutil.NewConcurrentMap[int64, Segment]()
	loaded := typeutil.NewConcurrentMap[int64, Segment]()
	defer func() {
		newSegments.Range(func(segmentID int64, s Segment) bool {
			mlog.Warn(context.TODO(), "release new segment created due to load failure",
				mlog.Int64("segmentID", segmentID),
				mlog.Err(err),
			)
			s.Release(context.Background())
			return true
		})
	}()

	for _, info := range infos {
		loadInfo := info

		if err := prepareIndexLoadParams(loadInfo.GetIndexInfos()); err != nil {
			return nil, err
		}

		segment, err := NewSegment(
			ctx,
			collection,
			loader.manager.Segment,
			segmentType,
			version,
			loadInfo,
		)
		if err != nil {
			mlog.Warn(context.TODO(), "load segment failed when create new segment",
				mlog.Int64("partitionID", loadInfo.GetPartitionID()),
				mlog.Int64("segmentID", loadInfo.GetSegmentID()),
				mlog.Err(err),
			)
			return nil, err
		}

		newSegments.Insert(loadInfo.GetSegmentID(), segment)
	}

	loadSegmentFunc := func(idx int) (err error) {
		loadInfo := infos[idx]
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		segment, _ := newSegments.Get(segmentID)

		logger := mlog.With(mlog.Int64("partitionID", partitionID),
			mlog.Int64("segmentID", segmentID),
			mlog.String("segmentType", loadInfo.GetLevel().String()))
		metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(paramtable.GetStringNodeID(), "LoadSegment").Inc()
		defer func() {
			metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(paramtable.GetStringNodeID(), "LoadSegment").Dec()
			if err != nil {
				logger.Warn(ctx, "load segment failed when load data into memory", mlog.Err(err))
			}
			logger.Info(ctx, "load segment done")
		}()
		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		logger.Info(ctx, "load segment...")

		// L0 segment has no index or data to be load.
		if loadInfo.GetLevel() != datapb.SegmentLevel_L0 {
			// lazy load segment do not load segment at first time.
			if err = loader.LoadSegment(ctx, segment, loadInfo); err != nil {
				return merr.Wrap(err, "At LoadSegment")
			}
		}
		if err = loader.loadDeltalogs(ctx, segment, loadInfo); err != nil {
			return merr.Wrap(err, "At LoadDeltaLogs")
		}

		schema := collection.Schema()
		isExternalCollection := typeutil.IsExternalCollection(schema)
		isMilvusTableRealPK := typeutil.NewStorageColumnResolver(schema).IsMilvusTable() &&
			HasExternalPrimaryKey(schema)
		if !segment.PkCandidateExist() {
			mlog.Debug(context.TODO(), "loading PK candidate for segment", mlog.Int64("segmentID", segment.ID()))
			if isExternalCollection {
				var candidate pkoracle.Candidate
				if isMilvusTableRealPK {
					bfs, err := loader.loadSingleBloomFilterSet(ctx, loadInfo.GetCollectionID(), loadInfo, segment.Type())
					if err != nil {
						return merr.Wrap(err, "At LoadBloomFilter")
					}
					if bfs.PkCandidateExist() {
						segment.SetPKCandidate(bfs)
						bfs.Charge()
						mlog.Info(context.TODO(), "using external real-PK bloom filter candidate",
							mlog.FieldSegmentID(loadInfo.GetSegmentID()))
					}
					if !segment.PkCandidateExist() {
						return merr.WrapErrServiceInternalMsg("milvus-table real-PK segment missing bloom filter stats")
					}
				} else {
					candidate = pkoracle.NewExternalSegmentCandidate(
						loadInfo.GetSegmentID(),
						loadInfo.GetPartitionID(),
						segment.Type(),
					)
				}
				if candidate != nil {
					segment.SetPKCandidate(candidate)
					mlog.Info(context.TODO(), "using external collection PK candidate",
						mlog.FieldSegmentID(loadInfo.GetSegmentID()),
						mlog.Bool("realPK", isMilvusTableRealPK))
				}

				// Check for truncated segment ID collision with other segments being loaded.
				if !isMilvusTableRealPK {
					collisions := detectVirtualPKCollisions(loadInfo.GetSegmentID(), infos)
					for _, collidingID := range collisions {
						mlog.Warn(context.TODO(), "virtual PK collision detected: two segments share truncated segment ID",
							mlog.Int64("segmentID1", loadInfo.GetSegmentID()),
							mlog.Int64("segmentID2", collidingID),
							mlog.Int64("truncatedID", loadInfo.GetSegmentID()&0xFFFFFFFF))
					}
				}
			} else if paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() {
				bfs, err := loader.loadSingleBloomFilterSet(ctx, loadInfo.GetCollectionID(), loadInfo, segment.Type())
				if err != nil {
					return merr.Wrap(err, "At LoadBloomFilter")
				}
				segment.SetPKCandidate(bfs)
				// Charge bloom filter resource
				bfs.Charge()
			}
		}

		if segment.Level() != datapb.SegmentLevel_L0 {
			loader.manager.Segment.Put(ctx, segmentType, segment)
		}
		newSegments.GetAndRemove(segmentID)
		loaded.Insert(segmentID, segment)
		loader.notifyLoadFinish(loadInfo)
		if localSegment, ok := segment.(*LocalSegment); ok {
			localSegment.compactLoadInfoForRuntime()
		}

		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil
	}

	// Start to load,
	// Make sure we can always benefit from concurrency, and not spawn too many idle goroutines
	mlog.Info(context.TODO(), "start to load segments in parallel",
		mlog.Int("segmentNum", len(infos)),
		mlog.Int("concurrencyLevel", requestResourceResult.ConcurrencyLevel))

	err = funcutil.ProcessFuncParallel(len(infos),
		requestResourceResult.ConcurrencyLevel, loadSegmentFunc, "loadSegmentFunc")
	if err != nil {
		mlog.Warn(context.TODO(), "failed to load some segments", mlog.Err(err))
		return nil, err
	}

	// Wait for all segments loaded
	segmentIDs := lo.Map(segments, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })
	if err := loader.waitSegmentLoadDone(ctx, segmentType, segmentIDs, version); err != nil {
		mlog.Warn(context.TODO(), "failed to wait the filtered out segments load done", mlog.Err(err))
		return nil, err
	}

	mlog.Info(context.TODO(), "all segment load done")
	var result []Segment
	loaded.Range(func(_ int64, s Segment) bool {
		result = append(result, s)
		return true
	})
	return result, nil
}

func (loader *segmentLoader) prepare(ctx context.Context, segmentType SegmentType, segments ...*querypb.SegmentLoadInfo) []*querypb.SegmentLoadInfo {
	// filter out loaded & loading segments
	infos := make([]*querypb.SegmentLoadInfo, 0, len(segments))
	for _, segment := range segments {
		// Only active loaded segments should be skipped here. SegmentManager.Exist()
		// also reports detached/on-releasing segments, which are no longer active
		// and must be allowed to load again.
		isLoaded := loader.manager.Segment.GetWithType(segment.GetSegmentID(), segmentType) != nil
		isLoading := loader.loadingSegments.Contain(segment.GetSegmentID())
		if !isLoaded && !isLoading {
			infos = append(infos, segment)
			loader.loadingSegments.Insert(segment.GetSegmentID(), newLoadResult())
		} else {
			mlog.Info(context.TODO(), "skip loaded/loading segment",
				mlog.Int64("segmentID", segment.GetSegmentID()),
				mlog.Bool("isLoaded", isLoaded),
				mlog.Bool("isLoading", isLoading),
			)
		}
	}

	return infos
}

func (loader *segmentLoader) unregister(segments ...*querypb.SegmentLoadInfo) {
	for i := range segments {
		result, ok := loader.loadingSegments.GetAndRemove(segments[i].GetSegmentID())
		if ok {
			result.SetResult(failure)
		}
	}
}

func (loader *segmentLoader) notifyLoadFinish(segments ...*querypb.SegmentLoadInfo) {
	for _, loadInfo := range segments {
		result, ok := loader.loadingSegments.Get(loadInfo.GetSegmentID())
		if ok {
			result.SetResult(success)
		}
	}
}

// requestResource requests memory & storage to load segments,
// returns the memory usage, disk usage and concurrency with the gained memory.
func (loader *segmentLoader) requestResource(ctx context.Context, infos ...*querypb.SegmentLoadInfo) (requestResourceResult, error) {
	// we need to deal with empty infos case separately,
	// because the following judgement for requested resources are based on current status and static config
	// which may block empty-load operations by accident
	if len(infos) == 0 {
		return requestResourceResult{}, nil
	}

	segmentIDs := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
		return info.GetSegmentID()
	})
	logger := mlog.With(
		mlog.Int64s("segmentIDs", segmentIDs),
	)

	loadingUsage, maxSegmentSize, err := loader.estimateSegmentLoadingResourceUsage(ctx, infos...)
	if err != nil {
		logger.Warn(ctx, "no sufficient physical resource to load segments", mlog.Err(err))
		return requestResourceResult{}, err
	}

	return loader.reserve(ctx, logger, loadingUsage, maxSegmentSize, len(infos))
}

// freeRequestResource returns request memory & storage usage request.
func (loader *segmentLoader) waitSegmentLoadDone(ctx context.Context, segmentType SegmentType, segmentIDs []int64, version int64) error {
	for _, segmentID := range segmentIDs {
		if loader.manager.Segment.GetWithType(segmentID, segmentType) != nil {
			continue
		}

		result, ok := loader.loadingSegments.Get(segmentID)
		if !ok {
			mlog.Warn(context.TODO(), "segment was removed from the loading map early", mlog.Int64("segmentID", segmentID))
			return merr.WrapErrServiceInternalMsg("segment was removed from the loading map early")
		}

		mlog.Info(context.TODO(), "wait segment loaded...", mlog.Int64("segmentID", segmentID))

		signal := make(chan struct{})
		go func() {
			select {
			case <-signal:
			case <-ctx.Done():
				result.cond.Broadcast()
			}
		}()
		result.cond.L.Lock()
		for result.status.Load() == loading && ctx.Err() == nil {
			result.cond.Wait()
		}
		result.cond.L.Unlock()
		close(signal)

		if ctx.Err() != nil {
			mlog.Warn(context.TODO(), "failed to wait segment loaded due to context done", mlog.Int64("segmentID", segmentID))
			return ctx.Err()
		}

		if result.status.Load() == failure {
			mlog.Warn(context.TODO(), "failed to wait segment loaded", mlog.Int64("segmentID", segmentID))
			return merr.WrapErrSegmentLack(segmentID, "failed to wait segment loaded")
		}

		// try to update segment version after wait segment loaded
		loader.manager.Segment.UpdateBy(IncreaseVersion(version), WithType(segmentType), WithID(segmentID))

		mlog.Info(context.TODO(), "segment loaded...", mlog.Int64("segmentID", segmentID))
	}
	return nil
}

func (loader *segmentLoader) GetChunkManager() storage.ChunkManager {
	return loader.cm
}

// load single bloom filter
func (loader *segmentLoader) loadSingleBloomFilterSet(ctx context.Context, collectionID int64, loadInfo *querypb.SegmentLoadInfo, segtype SegmentType) (*pkoracle.BloomFilterSet, error) {
	partitionID := loadInfo.PartitionID
	segmentID := loadInfo.SegmentID
	bfs := pkoracle.NewBloomFilterSet(segmentID, partitionID, segtype)

	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(collectionID)
		mlog.Warn(context.TODO(), "failed to get collection while loading segment", mlog.Err(err))
		return nil, err
	}

	mlog.Debug(ctx, "start loading remote...", mlog.Int("segmentNum", 1))

	schema := collection.Schema()
	isExternalCollection := typeutil.IsExternalCollection(schema)
	isMilvusTableRealPK := typeutil.NewStorageColumnResolver(schema).IsMilvusTable() &&
		HasExternalPrimaryKey(schema)
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() && !isMilvusTableRealPK {
		mlog.Info(context.TODO(), "skip loading bloom filter for remote segment because bloom filter is disabled")
		return bfs, nil
	}
	if isExternalCollection && !isMilvusTableRealPK {
		mlog.Debug(context.TODO(), "virtual-PK external collection: returning empty bloom filter set")
		return bfs, nil
	}

	pkField := GetPkField(schema)
	mlog.Debug(ctx, "loading bloom filter for remote...")
	pkStatsBinlogs, err := packed.NewStatsResolverFromLoadInfo(loadInfo).BloomFilterPaths(pkField.GetFieldID())
	if err != nil {
		return nil, err
	}
	err = loader.loadBloomFilter(ctx, segmentID, bfs, pkStatsBinlogs, loader.bloomFilterDownloader(collection, isMilvusTableRealPK))
	if err != nil {
		mlog.Warn(context.TODO(), "load remote segment bloom filter failed",
			mlog.Int64("partitionID", partitionID),
			mlog.Int64("segmentID", segmentID),
			mlog.Err(err),
		)
		return nil, err
	}
	if isMilvusTableRealPK && !bfs.PkCandidateExist() {
		return nil, merr.WrapErrServiceInternalMsg("milvus-table real-PK segment missing bloom filter stats")
	}

	return bfs, nil
}

func (loader *segmentLoader) LoadBloomFilterSet(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error) {
	if len(infos) == 0 {
		return nil, nil
	}
	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	return LoadSegmentBloomFilters(ctx, collection.Schema(), collectionID, loader.cm, infos...)
}

func separateIndexAndBinlog(loadInfo *querypb.SegmentLoadInfo) (map[int64]*IndexedFieldInfo, []*datapb.FieldBinlog) {
	fieldID2IndexInfo := make(map[int64][]*querypb.FieldIndexInfo)
	for _, indexInfo := range loadInfo.IndexInfos {
		if len(indexInfo.GetIndexFilePaths()) > 0 {
			fieldID := indexInfo.FieldID
			fieldID2IndexInfo[fieldID] = append(fieldID2IndexInfo[fieldID], indexInfo)
		}
	}

	preferFieldData := paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool()

	indexedFieldInfos := make(map[int64]*IndexedFieldInfo)
	fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(loadInfo.BinlogPaths))

	for _, fieldBinlog := range loadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		// check num rows of data meta and index meta are consistent
		if indexInfo, ok := fieldID2IndexInfo[fieldID]; ok {
			for _, index := range indexInfo {
				fieldInfo := &IndexedFieldInfo{
					FieldBinlog: fieldBinlog,
					IndexInfo:   index,
				}
				indexedFieldInfos[index.IndexID] = fieldInfo
			}
			if preferFieldData {
				fieldBinlogs = append(fieldBinlogs, fieldBinlog)
			}
		} else {
			fieldBinlogs = append(fieldBinlogs, fieldBinlog)
		}
	}

	return indexedFieldInfos, fieldBinlogs
}

// detectVirtualPKCollisions checks if any segments in infos share the same
// truncated (lower 32 bits) segment ID as segmentID. A collision means two
// segments produce overlapping virtual PK spaces.
func detectVirtualPKCollisions(segmentID int64, infos []*querypb.SegmentLoadInfo) []int64 {
	truncatedID := segmentID & 0xFFFFFFFF
	var collisions []int64
	for _, info := range infos {
		if info.GetSegmentID() != segmentID &&
			(info.GetSegmentID()&0xFFFFFFFF) == truncatedID {
			collisions = append(collisions, info.GetSegmentID())
		}
	}
	return collisions
}

func separateLoadInfoV2(loadInfo *querypb.SegmentLoadInfo, schema *schemapb.CollectionSchema) (
	map[int64]*IndexedFieldInfo, // indexed info
	[]*datapb.FieldBinlog, // fields info
	map[int64]*datapb.TextIndexStats, // text indexed info
	map[int64]struct{}, // unindexed text fields
	map[int64]*datapb.JsonKeyStats, // json key stats info
	map[int64]string, // text index base paths
	map[int64]string, // json key stats base paths
) {
	storageVersion := loadInfo.GetStorageVersion()

	// Build a map of external field IDs for quick lookup
	// External fields are skipped during loading (lazy loaded on demand)
	externalFieldIDs := make(map[int64]bool)
	isExternalColl := typeutil.IsExternalCollection(schema)
	if isExternalColl {
		for _, field := range schema.GetFields() {
			if IsExternalField(field) {
				externalFieldIDs[field.GetFieldID()] = true
			}
		}
	}

	fieldID2IndexInfo := make(map[int64][]*querypb.FieldIndexInfo)
	for _, indexInfo := range loadInfo.IndexInfos {
		if len(indexInfo.GetIndexFilePaths()) > 0 {
			fieldID := indexInfo.FieldID
			fieldID2IndexInfo[fieldID] = append(fieldID2IndexInfo[fieldID], indexInfo)
		}
	}

	preferFieldData := paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool()

	indexedFieldInfos := make(map[int64]*IndexedFieldInfo)
	fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(loadInfo.BinlogPaths))

	if storageVersion == storage.StorageV2 || storageVersion == storage.StorageV3 {
		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID

			// Skip external fields - they are lazy loaded on demand
			if externalFieldIDs[fieldID] {
				continue
			}

			if fieldID == storagecommon.DefaultShortColumnGroupID {
				allFields := typeutil.GetAllFieldSchemas(schema)
				// for short column group, we need to load all fields in the group
				for _, field := range allFields {
					// Skip external fields in short column group
					if externalFieldIDs[field.GetFieldID()] {
						continue
					}
					if infos, ok := fieldID2IndexInfo[field.GetFieldID()]; ok {
						for _, indexInfo := range infos {
							fieldInfo := &IndexedFieldInfo{
								FieldBinlog: fieldBinlog,
								IndexInfo:   indexInfo,
							}
							indexedFieldInfos[indexInfo.IndexID] = fieldInfo
						}
					}
				}
				fieldBinlogs = append(fieldBinlogs, fieldBinlog)
			} else {
				// for single file field, such as vector field, text field
				if infos, ok := fieldID2IndexInfo[fieldID]; ok {
					for _, indexInfo := range infos {
						fieldInfo := &IndexedFieldInfo{
							FieldBinlog: fieldBinlog,
							IndexInfo:   indexInfo,
						}
						indexedFieldInfos[indexInfo.IndexID] = fieldInfo
					}
					if preferFieldData {
						fieldBinlogs = append(fieldBinlogs, fieldBinlog)
					}
				} else {
					fieldBinlogs = append(fieldBinlogs, fieldBinlog)
				}
			}
		}
	} else {
		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID

			// Skip external fields - they are lazy loaded on demand
			if externalFieldIDs[fieldID] {
				continue
			}

			if infos, ok := fieldID2IndexInfo[fieldID]; ok {
				for _, indexInfo := range infos {
					fieldInfo := &IndexedFieldInfo{
						FieldBinlog: fieldBinlog,
						IndexInfo:   indexInfo,
					}
					indexedFieldInfos[indexInfo.IndexID] = fieldInfo
				}
				if preferFieldData {
					fieldBinlogs = append(fieldBinlogs, fieldBinlog)
				}
			} else {
				fieldBinlogs = append(fieldBinlogs, fieldBinlog)
			}
		}
	}

	// For external table segments (ManifestPath set, BinlogPaths empty), extract
	// indexes directly from fieldID2IndexInfo without a corresponding FieldBinlog,
	// because the segment data lives in the external store rather than Milvus binlogs.
	if loadInfo.GetManifestPath() != "" {
		for _, infos := range fieldID2IndexInfo {
			for _, indexInfo := range infos {
				if _, exists := indexedFieldInfos[indexInfo.IndexID]; !exists {
					indexedFieldInfos[indexInfo.IndexID] = &IndexedFieldInfo{
						FieldBinlog: &datapb.FieldBinlog{},
						IndexInfo:   indexInfo,
					}
				}
			}
		}
	}

	statsResult := packed.NewStatsResolverFromLoadInfo(loadInfo).TextAndJSONIndexStatsWithBasePaths()
	textIndexedInfo := statsResult.TextIndexStats
	jsonKeyIndexInfo := statsResult.JSONKeyStats
	textBasePaths := statsResult.TextBasePaths
	jsonBasePaths := statsResult.JSONBasePaths
	if statsResult.Err() != nil {
		mlog.Warn(context.TODO(), "failed to load text/json stats from manifest",
			mlog.String("manifestPath", loadInfo.GetManifestPath()), mlog.Err(statsResult.Err()))
		textIndexedInfo = make(map[int64]*datapb.TextIndexStats)
		jsonKeyIndexInfo = make(map[int64]*datapb.JsonKeyStats)
		textBasePaths = make(map[int64]string)
		jsonBasePaths = make(map[int64]string)
	}

	if textBasePaths == nil {
		textBasePaths = make(map[int64]string)
	}
	if jsonBasePaths == nil {
		jsonBasePaths = make(map[int64]string)
	}

	// For V2 (non-manifest) segments, compute basePaths from metadata.
	// The resolver returns empty basePaths for V2; we compute them here.
	// Match the writer's primary storage root; local stats do not use MinIO's prefix.
	rootPath := binlog.GetRootPath()
	for fieldID, stats := range textIndexedInfo {
		if _, ok := textBasePaths[fieldID]; !ok {
			textBasePaths[fieldID] = metautil.BuildTextIndexPrefix(rootPath,
				stats.GetBuildID(), stats.GetVersion(),
				loadInfo.GetCollectionID(), loadInfo.GetPartitionID(), loadInfo.GetSegmentID(), fieldID)
		}
	}
	for fieldID, stats := range jsonKeyIndexInfo {
		if _, ok := jsonBasePaths[fieldID]; !ok {
			jsonBasePaths[fieldID] = metautil.BuildJSONKeyStatsPrefix(rootPath, stats.GetJsonKeyStatsDataFormat(),
				stats.GetBuildID(), stats.GetVersion(),
				loadInfo.GetCollectionID(), loadInfo.GetPartitionID(), loadInfo.GetSegmentID(), fieldID)
		}
	}

	unindexedTextFields := make(map[int64]struct{})
	// todo(SpadeA): consider struct fields when index is ready
	for _, field := range schema.GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		_, textIndexExist := textIndexedInfo[field.GetFieldID()]
		if h.EnableMatch() && !textIndexExist {
			unindexedTextFields[field.GetFieldID()] = struct{}{}
		}
	}

	return indexedFieldInfos, fieldBinlogs, textIndexedInfo, unindexedTextFields, jsonKeyIndexInfo, textBasePaths, jsonBasePaths
}

func (loader *segmentLoader) loadSealedSegment(ctx context.Context, loadInfo *querypb.SegmentLoadInfo, segment *LocalSegment) (err error) {
	// TODO: we should create a transaction-like api to load segment for segment interface,
	// but not do many things in segment loader.
	stateLockGuard, err := segment.StartLoadData()
	// segment can not do load now.
	if err != nil {
		return err
	}
	if stateLockGuard == nil {
		return nil
	}
	defer func() {
		if err != nil {
			// Release partial loaded segment data if load failed.
			segment.ReleaseSegmentData()
		}
		stateLockGuard.Done(err)
	}()

	tr := timerecord.NewTimeRecorder("segmentLoader.loadSealedSegment")
	if mlog.LevelEnabled(mlog.DebugLevel) {
		collection := segment.GetCollection()
		indexedFieldInfos, _, textIndexes, unindexedTextFields, jsonKeyStats, _, _ := separateLoadInfoV2(loadInfo, collection.Schema())

		mlog.Debug(ctx, "Start loading fields...",
			mlog.Int("indexedFields count", len(indexedFieldInfos)),
			mlog.Int64s("indexed text fields", lo.Keys(textIndexes)),
			mlog.Int64s("unindexed text fields", lo.Keys(unindexedTextFields)),
			mlog.Int64s("indexed json key fields", lo.Keys(jsonKeyStats)),
		)
	}
	_, err = GetLoadPool().Submit(func() (any, error) {
		if err = segment.Load(ctx); err != nil {
			return struct{}{}, merr.Wrap(err, "At Load")
		}

		return struct{}{}, nil
	}).Await()
	if err != nil {
		return err
	}

	for _, indexInfo := range loadInfo.IndexInfos {
		segment.fieldIndexes.Insert(indexInfo.GetIndexID(), &IndexedFieldInfo{
			FieldBinlog: &datapb.FieldBinlog{
				FieldID: indexInfo.GetFieldID(),
			},
			IndexInfo: indexInfo,
			IsLoaded:  true,
		})
	}

	// 4. rectify entries number for binlog in very rare cases
	// https://github.com/milvus-io/milvus/23654
	// legacy entry num = 0
	if err := loader.patchEntryNumber(ctx, segment, loadInfo); err != nil {
		return err
	}
	patchEntryNumberSpan := tr.RecordSpan()
	mlog.Debug(ctx, "Finish loading segment",
		mlog.Duration("patchEntryNumberSpan", patchEntryNumberSpan),
	)
	return nil
}

func (loader *segmentLoader) LoadSegment(ctx context.Context,
	seg Segment,
	loadInfo *querypb.SegmentLoadInfo,
) (err error) {
	segment, ok := seg.(*LocalSegment)
	if !ok {
		return merr.WrapErrParameterInvalid("LocalSegment", fmt.Sprintf("%T", seg))
	}
	mlog.Debug(ctx, "start loading segment files",
		mlog.Int64("rowNum", loadInfo.GetNumOfRows()),
		mlog.String("segmentType", segment.Type().String()),
		mlog.Int32("priority", int32(loadInfo.GetPriority())))

	collection := loader.manager.Collection.Get(segment.Collection())
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(segment.Collection())
		mlog.Warn(context.TODO(), "failed to get collection while loading segment", mlog.Err(err))
		return err
	}
	pkField := GetPkField(collection.Schema())

	if segment.Type() == SegmentTypeSealed {
		if err := loader.loadSealedSegment(ctx, loadInfo, segment); err != nil {
			return err
		}
	} else {
		if err := segment.Load(ctx); err != nil {
			return err
		}
	}

	relatedDataSize := calculateSegmentLogSize(segment.LoadInfo())
	segment.relatedDataSize.Store(relatedDataSize)
	binlogSize := calculateSegmentMemorySize(segment.LoadInfo())
	segment.manager.AddLoadedBinlogSize(binlogSize)
	segment.binlogSize.Store(binlogSize)

	// load statslog if it's growing segment
	if segment.segmentType == SegmentTypeGrowing {
		if bf, ok := segment.pkCandidate.(*pkoracle.BloomFilterSet); ok {
			mlog.Info(context.TODO(), "loading statslog...")
			resolver := packed.NewStatsResolverFromLoadInfo(loadInfo)
			bfPaths, err := resolver.BloomFilterPaths(pkField.GetFieldID())
			if err != nil {
				return err
			}
			if err := loader.loadBloomFilter(ctx, segment.ID(), bf, bfPaths, loader.cm.MultiRead); err != nil {
				return err
			}

			bm25Paths, err := resolver.BM25StatsPaths()
			if err != nil {
				return err
			}
			bm25Stats := make(map[int64]*storage.BM25Stats)
			if err := loader.loadBm25Stats(ctx, segment.ID(), bm25Stats, bm25Paths); err != nil {
				return err
			}
			segment.UpdateBM25Stats(bm25Stats)
		}
	}
	return nil
}

func loadSealedSegmentFields(ctx context.Context, collection *Collection, segment *LocalSegment, fields []*datapb.FieldBinlog, rowCount int64) error {
	runningGroup, _ := errgroup.WithContext(ctx)
	for _, field := range fields {
		fieldBinLog := field
		fieldID := field.FieldID
		runningGroup.Go(func() error {
			return segment.LoadFieldData(ctx, fieldID, rowCount, fieldBinLog)
		})
	}
	err := runningGroup.Wait()
	if err != nil {
		return err
	}

	mlog.Info(ctx, "load field binlogs done for sealed segment",
		mlog.Int64("collection", segment.Collection()),
		mlog.Int64("segment", segment.ID()),
		mlog.Int("len(field)", len(fields)),
		mlog.String("segmentType", segment.Type().String()))

	return nil
}

func (loader *segmentLoader) loadBm25Stats(ctx context.Context, segmentID int64, stats map[int64]*storage.BM25Stats, binlogPaths map[int64][]string) error {
	if len(binlogPaths) == 0 {
		mlog.Info(context.TODO(), "there are no bm25 stats logs saved with segment")
		return nil
	}

	pathList := []string{}
	fieldList := []int64{}
	fieldOffset := []int{}
	for fieldId, logpaths := range binlogPaths {
		pathList = append(pathList, logpaths...)
		fieldList = append(fieldList, fieldId)
		fieldOffset = append(fieldOffset, len(logpaths))
	}

	startTs := time.Now()
	values, err := loader.cm.MultiRead(ctx, pathList)
	if err != nil {
		return err
	}

	cnt := 0
	for i, fieldID := range fieldList {
		newStats, ok := stats[fieldID]
		if !ok {
			newStats = storage.NewBM25Stats()
			stats[fieldID] = newStats
		}

		for j := 0; j < fieldOffset[i]; j++ {
			err := newStats.Deserialize(values[cnt+j])
			if err != nil {
				return err
			}
		}
		cnt += fieldOffset[i]
		mlog.Info(context.TODO(), "Successfully load bm25 stats", mlog.Duration("time", time.Since(startTs)), mlog.Int64("numRow", newStats.NumRow()), mlog.Int64("fieldID", fieldID))
	}

	return nil
}

func (loader *segmentLoader) loadBloomFilter(
	ctx context.Context,
	segmentID int64,
	bfs *pkoracle.BloomFilterSet,
	binlogPaths []string,
	downloader func(context.Context, []string) ([][]byte, error),
) error {
	return loadBloomFilterWithDownloader(ctx, segmentID, bfs, binlogPaths, downloader)
}

// bloomFilterDownloader returns the byte downloader used for PK bloom-filter
// stats. Milvus-table real-PK stats live in the external source filesystem;
// ordinary internal stats stay on the local chunk manager.
func (loader *segmentLoader) bloomFilterDownloader(collection *Collection, external bool) func(context.Context, []string) ([][]byte, error) {
	return bloomFilterDownloader(collection.Schema(), collection.ID(), loader.cm, external)
}

// loadDeltalogs performs the internal actions of `LoadDeltaLogs`
// this function does not perform resource check and is meant be used among other load APIs.
func (loader *segmentLoader) loadDeltalogs(ctx context.Context, segment Segment, loadInfo *querypb.SegmentLoadInfo) error {
	collection := loader.manager.Collection.Get(segment.Collection())
	return LoadSegmentDeltaLogs(ctx, collection.Schema(), segment.Collection(), loader.cm, segment, loadInfo)
}

func milvusTableDeltalogPaths(deltaLogs []*datapb.FieldBinlog) []string {
	paths := make([]string, 0)
	for _, deltaLog := range deltaLogs {
		for _, binlog := range deltaLog.GetBinlogs() {
			if binlog.GetLogPath() != "" {
				paths = append(paths, binlog.GetLogPath())
			}
		}
	}
	return paths
}

// validateMilvusTableRealPKDeltalogPaths rejects target-owned deltalogs from a
// real-PK milvus-table manifest. Real-PK load may consume source StorageV3
// deltas and legacy snapshot L0 deltas; target-owned deltas are reserved for
// virtual-PK translation.
func validateMilvusTableRealPKDeltalogPaths(manifestPath string, deltaPaths []string) error {
	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "parse milvus-table manifest path")
	}
	targetDeltaPrefix := strings.TrimRight(basePath, "/") + "/_delta/"
	for _, deltaPath := range deltaPaths {
		if deltaPath == "" {
			continue
		}
		if strings.HasPrefix(deltaPath, targetDeltaPrefix) {
			return merr.WrapErrServiceInternalMsg("milvus-table real-PK manifest must not contain target-owned deltalog %s", deltaPath)
		}
		if err := packed.ValidateMilvusTableSourceDeltalogPath(deltaPath); err != nil {
			return err
		}
	}
	return nil
}

// readExternalFiles reads whole files through packed external-spec filesystem
// aliases and checks ctx before each potentially large read.
func readExternalFiles(
	ctx context.Context,
	storageConfig *indexpb.StorageConfig,
	extfs packed.ExternalSpecContext,
	paths []string,
) ([][]byte, error) {
	data := make([][]byte, len(paths))
	for i, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		content, err := packed.ReadFileWithExternalSpec(storageConfig, path, extfs)
		if err != nil {
			return nil, err
		}
		data[i] = content
	}
	return data, nil
}

// LoadDeltaLogs load deltalog and write delta data into provided segment.
// it also executes resource protection logic in case of OOM.
func (loader *segmentLoader) LoadDeltaLogs(ctx context.Context, segment Segment, loadInfo *querypb.SegmentLoadInfo) error {
	// Check memory & storage limit
	requestResourceResult, err := loader.requestResource(ctx, loadInfo)
	if err != nil {
		mlog.Warn(context.TODO(), "request resource failed", mlog.Err(err))
		return err
	}
	defer loader.freeRequestResource(requestResourceResult)
	return loader.loadDeltalogs(ctx, segment, loadInfo)
}

func createStorageConfig() *indexpb.StorageConfig {
	params := paramtable.Get()
	if params.CommonCfg.StorageType.GetValue() == "local" {
		return &indexpb.StorageConfig{
			RootPath:    params.LocalStorageCfg.Path.GetValue(),
			StorageType: params.CommonCfg.StorageType.GetValue(),
			// External collections may reference an s3:// source even when the
			// primary storage is local, so the connection cap still applies.
			MaxConnections:              uint32(params.MinioCfg.MaxConnections.GetAsInt()),
			TalonMode:                   params.CommonCfg.StorageTalonMode.GetAsUint32(),
			TalonSmallReadThreshold:     params.CommonCfg.StorageTalonSmallReadThreshold.GetAsUint32(),
			TalonCoordinator:            params.CommonCfg.StorageTalonCoordinator.GetValue(),
			TalonBlockSize:              params.CommonCfg.StorageTalonBlockSize.GetAsUint32(),
			TalonMaxIdlePerAddr:         params.CommonCfg.StorageTalonMaxIdlePerAddr.GetAsUint32(),
			TalonEnableForExternalTable: params.CommonCfg.StorageTalonEnableForExternalTable.GetAsBool(),
		}
	}
	return &indexpb.StorageConfig{
		Address:                     params.MinioCfg.Address.GetValue(),
		AccessKeyID:                 params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:             params.MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:                      params.MinioCfg.UseSSL.GetAsBool(),
		SslCACert:                   params.MinioCfg.SslCACert.GetValue(),
		BucketName:                  params.MinioCfg.BucketName.GetValue(),
		RootPath:                    params.MinioCfg.RootPath.GetValue(),
		UseIAM:                      params.MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:                 params.MinioCfg.IAMEndpoint.GetValue(),
		StorageType:                 params.CommonCfg.StorageType.GetValue(),
		Region:                      params.MinioCfg.Region.GetValue(),
		UseVirtualHost:              params.MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:               params.MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs:            params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
		MaxConnections:              uint32(params.MinioCfg.MaxConnections.GetAsInt()),
		GcpCredentialJSON:           params.MinioCfg.GcpCredentialJSON.GetValue(),
		SslTlsMinVersion:            params.MinioCfg.SslTLSMinVersion.GetValue(),
		UseCrc32CChecksum:           params.MinioCfg.UseCRC32C.GetAsBool(),
		TalonMode:                   params.CommonCfg.StorageTalonMode.GetAsUint32(),
		TalonSmallReadThreshold:     params.CommonCfg.StorageTalonSmallReadThreshold.GetAsUint32(),
		TalonCoordinator:            params.CommonCfg.StorageTalonCoordinator.GetValue(),
		TalonBlockSize:              params.CommonCfg.StorageTalonBlockSize.GetAsUint32(),
		TalonMaxIdlePerAddr:         params.CommonCfg.StorageTalonMaxIdlePerAddr.GetAsUint32(),
		TalonEnableForExternalTable: params.CommonCfg.StorageTalonEnableForExternalTable.GetAsBool(),
	}
}

func (loader *segmentLoader) patchEntryNumber(ctx context.Context, segment *LocalSegment, loadInfo *querypb.SegmentLoadInfo) error {
	var needReset bool

	segment.fieldIndexes.Range(func(indexID int64, info *IndexedFieldInfo) bool {
		for _, info := range info.FieldBinlog.GetBinlogs() {
			if info.GetEntriesNum() == 0 {
				needReset = true
				return false
			}
		}
		return true
	})
	if !needReset {
		return nil
	}

	mlog.Warn(context.TODO(), "legacy segment binlog found, start to patch entry num", mlog.Int64("segmentID", segment.ID()))
	rowIDField := lo.FindOrElse(loadInfo.BinlogPaths, nil, func(binlog *datapb.FieldBinlog) bool {
		return binlog.GetFieldID() == common.RowIDField
	})

	if rowIDField == nil {
		return merr.WrapErrDataIntegrityMsg("rowID field binlog not found")
	}

	counts := make([]int64, 0, len(rowIDField.GetBinlogs()))
	for _, binlog := range rowIDField.GetBinlogs() {
		// binlog.LogPath has already been filled
		bs, err := loader.cm.Read(ctx, binlog.LogPath)
		if err != nil {
			return err
		}

		// get binlog entry num from rowID field
		// since header does not store entry numb, we have to read all data here

		reader, err := storage.NewBinlogReader(bs)
		if err != nil {
			return err
		}
		er, err := reader.NextEventReader()
		if err != nil {
			return err
		}

		rowIDs, _, err := er.GetInt64FromPayload()
		if err != nil {
			return err
		}
		counts = append(counts, int64(len(rowIDs)))
	}

	var err error
	segment.fieldIndexes.Range(func(indexID int64, info *IndexedFieldInfo) bool {
		if len(info.FieldBinlog.GetBinlogs()) != len(counts) {
			err = merr.WrapErrDataIntegrityMsg("rowID & index binlog number not matched")
			return false
		}
		for i, binlog := range info.FieldBinlog.GetBinlogs() {
			binlog.EntriesNum = counts[i]
		}
		return true
	})
	return err
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...int64) string {
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}

// After introducing the caching layer's lazy loading and eviction mechanisms, most parts of a segment won't be
// loaded into memory or disk immediately, even if the segment is marked as LOADED. This means physical resource
// usage may be very low.
// However, we still need to reserve enough resources for the segments marked as LOADED. The reserved resource is
// treated as the logical resource usage. Logical resource usage is based on the segment final resource usage.
// checkLogicalSegmentSize checks whether the memory & disk is sufficient to load the segments,
// returns the memory & disk logical usage while loading if possible to load, otherwise, returns error
func (loader *segmentLoader) checkLogicalSegmentSize(ctx context.Context, segmentLoadInfos []*querypb.SegmentLoadInfo, totalMem uint64) (uint64, uint64, error) {
	if !paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		return 0, 0, nil
	}

	if len(segmentLoadInfos) == 0 {
		return 0, 0, nil
	}

	logicalMemUsage := loader.manager.Segment.GetLogicalResource().MemorySize
	logicalDiskUsage := loader.manager.Segment.GetLogicalResource().DiskSize

	logicalMemUsage += loader.committedLogicalResource.MemorySize
	logicalDiskUsage += loader.committedLogicalResource.DiskSize

	// logical resource usage is based on the segment final resource usage,
	// so we need to estimate the final resource usage of the segments
	finalFactor := resourceEstimateFactor{
		deltaDataExpansionFactor:        paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
		jsonKeyStatsExpansionFactor:     paramtable.Get().QueryNodeCfg.JSONKeyStatsExpansionFactor.GetAsFloat(),
		textIndexExpansionFactor:        paramtable.Get().QueryNodeCfg.TextIndexExpansionFactor.GetAsFloat(),
		TieredEvictionEnabled:           paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool(),
		TieredEvictableMemoryCacheRatio: paramtable.Get().QueryNodeCfg.TieredEvictableMemoryCacheRatio.GetAsFloat(),
		TieredEvictableDiskCacheRatio:   paramtable.Get().QueryNodeCfg.TieredEvictableDiskCacheRatio.GetAsFloat(),
	}
	predictLogicalMemUsage := logicalMemUsage
	predictLogicalDiskUsage := logicalDiskUsage
	for _, loadInfo := range segmentLoadInfos {
		collection := loader.manager.Collection.Get(loadInfo.GetCollectionID())
		finalUsage, err := estimateLogicalResourceUsageOfSegment(collection.Schema(), loadInfo, finalFactor)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to estimate final resource usage of segment",
				mlog.Int64("collectionID", loadInfo.GetCollectionID()),
				mlog.Int64("segmentID", loadInfo.GetSegmentID()),
				mlog.Err(err))
			return 0, 0, err
		}

		mlog.Debug(context.TODO(), "segment logical resource for loading",
			mlog.Int64("segmentID", loadInfo.GetSegmentID()),
			mlog.Float64("memoryUsage(MB)", logutil.ToMB(float64(finalUsage.MemorySize))),
			mlog.Float64("diskUsage(MB)", logutil.ToMB(float64(finalUsage.DiskSize))),
		)
		predictLogicalDiskUsage += finalUsage.DiskSize
		predictLogicalMemUsage += finalUsage.MemorySize
	}

	mlog.Info(context.TODO(), "predict memory and disk logical usage after loaded (in MiB)",
		mlog.Float64("predictLogicalMemUsage(MB)", logutil.ToMB(float64(predictLogicalMemUsage))),
		mlog.Float64("predictLogicalDiskUsage(MB)", logutil.ToMB(float64(predictLogicalDiskUsage))),
	)

	logicalMemUsageLimit := uint64(float64(totalMem) * paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
	logicalDiskUsageLimit := uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64()) * paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat())

	if predictLogicalMemUsage > logicalMemUsageLimit {
		mlog.Warn(context.TODO(), "logical memory usage checking for segment loading failed",
			mlog.String("resourceType", "Memory"),
			mlog.Float64("predictLogicalMemUsageMB", logutil.ToMB(float64(predictLogicalMemUsage))),
			mlog.Float64("logicalMemUsageLimitMB", logutil.ToMB(float64(logicalMemUsageLimit))),
			mlog.Float64("evictableMemoryCacheRatio", paramtable.Get().QueryNodeCfg.TieredEvictableMemoryCacheRatio.GetAsFloat()),
		)
		return 0, 0, merr.WrapErrSegmentRequestResourceFailed("Memory")
	}

	if predictLogicalDiskUsage > logicalDiskUsageLimit {
		mlog.Warn(ctx, fmt.Sprintf("Logical disk usage checking for segment loading failed, predictLogicalDiskUsage = %v MB, LogicalDiskUsageLimit = %v MB, decrease the evictableDiskCacheRatio (current: %v) if you want to load more segments",
			logutil.ToMB(float64(predictLogicalDiskUsage)),
			logutil.ToMB(float64(logicalDiskUsageLimit)),
			paramtable.Get().QueryNodeCfg.TieredEvictableDiskCacheRatio.GetAsFloat(),
		))
		return 0, 0, merr.WrapErrSegmentRequestResourceFailed("Disk")
	}

	return predictLogicalMemUsage - logicalMemUsage, predictLogicalDiskUsage - logicalDiskUsage, nil
}

func (loader *segmentLoader) estimateSegmentLoadingResourceUsage(ctx context.Context, segmentLoadInfos ...*querypb.SegmentLoadInfo) (*ResourceUsage, uint64, error) {
	if len(segmentLoadInfos) == 0 {
		return &ResourceUsage{}, 0, nil
	}

	logger := mlog.With(
		mlog.Int64("collectionID", segmentLoadInfos[0].GetCollectionID()),
	)

	maxFactor := resourceEstimateFactor{
		memoryUsageFactor:           paramtable.Get().QueryNodeCfg.LoadMemoryUsageFactor.GetAsFloat(),
		memoryIndexUsageFactor:      paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(),
		EnableInterminSegmentIndex:  paramtable.Get().QueryNodeCfg.EnableInterminSegmentIndex.GetAsBool(),
		tempSegmentIndexFactor:      paramtable.Get().QueryNodeCfg.InterimIndexMemExpandRate.GetAsFloat(),
		deltaDataExpansionFactor:    paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
		jsonKeyStatsExpansionFactor: paramtable.Get().QueryNodeCfg.JSONKeyStatsExpansionFactor.GetAsFloat(),
		textIndexExpansionFactor:    paramtable.Get().QueryNodeCfg.TextIndexExpansionFactor.GetAsFloat(),
		TieredEvictionEnabled:       paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool(),
		externalRawDataFactor:       paramtable.Get().QueryNodeCfg.ExternalCollectionRawDataFactor.GetAsFloat(),
	}
	maxSegmentSize := uint64(0)
	predictMemUsage := uint64(0)
	predictDiskUsage := uint64(0)
	var predictGpuMemUsage []uint64
	mmapFieldCount := 0
	for _, loadInfo := range segmentLoadInfos {
		collection := loader.manager.Collection.Get(loadInfo.GetCollectionID())
		loadingUsage, err := estimateLoadingResourceUsageOfSegment(collection.Schema(), loadInfo, maxFactor)
		if err != nil {
			logger.Warn(ctx, "failed to estimate max resource usage of segment",
				mlog.Int64("collectionID", loadInfo.GetCollectionID()),
				mlog.Int64("segmentID", loadInfo.GetSegmentID()),
				mlog.Err(err))
			return nil, 0, err
		}

		logger.Debug(ctx, "segment resource for loading",
			mlog.Int64("segmentID", loadInfo.GetSegmentID()),
			mlog.Float64("loadingMemoryUsage(MB)", logutil.ToMB(float64(loadingUsage.MemorySize))),
			mlog.Float64("loadingDiskUsage(MB)", logutil.ToMB(float64(loadingUsage.DiskSize))),
			mlog.Float64("memoryLoadFactor", maxFactor.memoryUsageFactor),
		)
		mmapFieldCount += loadingUsage.MmapFieldCount
		predictDiskUsage += loadingUsage.DiskSize
		predictMemUsage += loadingUsage.MemorySize
		predictGpuMemUsage = append(predictGpuMemUsage, loadingUsage.FieldGpuMemorySize...)
		if loadingUsage.MemorySize > maxSegmentSize {
			maxSegmentSize = loadingUsage.MemorySize
		}
	}

	return &ResourceUsage{
		MemorySize:         predictMemUsage,
		DiskSize:           predictDiskUsage,
		MmapFieldCount:     mmapFieldCount,
		FieldGpuMemorySize: predictGpuMemUsage,
	}, maxSegmentSize, nil
}

// checkLoadingResource checks physical resource limits for an already-estimated loading usage.
// Callers that race with load resource commits must hold loader.mut.
// this function is used to estimate the logical resource usage of a segment, which should only be used when tiered eviction is enabled
// the result is the final resource usage of the segment inevictable part plus the final usage of evictable part with cache ratio applied
// TODO: the inevictable part is not correct, since we cannot know the final resource usage of interim index and default-value column before loading,
// current they are ignored, but we should consider them in the future
func estimateLogicalResourceUsageOfSegment(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, multiplyFactor resourceEstimateFactor) (usage *ResourceUsage, err error) {
	options := loadresource.DefaultSegmentFinalEstimateOptions()
	options.DeltaDataExpansionFactor = multiplyFactor.deltaDataExpansionFactor
	options.JSONKeyStatsExpansionFactor = multiplyFactor.jsonKeyStatsExpansionFactor
	options.TextIndexExpansionFactor = multiplyFactor.textIndexExpansionFactor
	options.TieredEvictionEnabled = multiplyFactor.TieredEvictionEnabled
	options.TieredEvictableMemoryCacheRatio = multiplyFactor.TieredEvictableMemoryCacheRatio
	options.TieredEvictableDiskCacheRatio = multiplyFactor.TieredEvictableDiskCacheRatio
	estimate, err := loadresource.EstimateSegmentFinalResource(context.Background(), schema, loadInfo, options, func(fn func() error) error {
		_, err := GetDynamicPool().Submit(func() (any, error) {
			return nil, fn()
		}).Await()
		return err
	})
	if err != nil {
		return nil, err
	}

	mlog.Debug(context.TODO(), "estimate logical resource usage result",
		mlog.Int64("segmentID", loadInfo.GetSegmentID()),
		mlog.Uint64("memorySize", estimate.MemoryBytes),
		mlog.Uint64("diskSize", estimate.DiskBytes),
	)

	return &ResourceUsage{
		MemorySize: estimate.MemoryBytes,
		DiskSize:   estimate.DiskBytes,
	}, nil
}

// estimateLoadingResourceUsageOfSegment estimates the resource usage of the segment when loading,
// it will return two different results, depending on the value of tiered eviction parameter:
//   - when tiered eviction is enabled, the result is the max resource usage of the segment that cannot be managed by caching layer,
//     which should be a subset of the segment inevictable part
//   - when tiered eviction is disabled, the result is the max resource usage of both the segment evictable and inevictable part
func estimateLoadingResourceUsageOfSegment(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, multiplyFactor resourceEstimateFactor) (usage *ResourceUsage, err error) {
	options := loadresource.DefaultSegmentLoadingEstimateOptions()
	options.DeltaDataExpansionFactor = multiplyFactor.deltaDataExpansionFactor
	options.JSONKeyStatsExpansionFactor = multiplyFactor.jsonKeyStatsExpansionFactor
	options.TextIndexExpansionFactor = multiplyFactor.textIndexExpansionFactor
	options.TieredEvictionEnabled = multiplyFactor.TieredEvictionEnabled
	options.EnableInterimSegmentIndex = multiplyFactor.EnableInterminSegmentIndex
	options.TempSegmentIndexFactor = multiplyFactor.tempSegmentIndexFactor
	options.ExternalRawDataFactor = multiplyFactor.externalRawDataFactor

	estimate, err := loadresource.EstimateSegmentLoadingResource(context.Background(), schema, loadInfo, options, func(fn func() error) error {
		_, err := GetDynamicPool().Submit(func() (any, error) {
			return nil, fn()
		}).Await()
		return err
	})
	if err != nil {
		return nil, err
	}

	return &ResourceUsage{
		MemorySize:         estimate.MemoryBytes,
		DiskSize:           estimate.DiskBytes,
		MmapFieldCount:     estimate.MmapFieldCount,
		FieldGpuMemorySize: estimate.FieldGPUMemoryBytes,
	}, nil
}

func (loader *segmentLoader) ReopenSegments(ctx context.Context,
	loadInfos []*querypb.SegmentLoadInfo,
) error {
	// Filter out LOADING segments only
	// use None to avoid loaded check
	infos := loader.prepare(ctx, commonpb.SegmentState_SegmentStateNone, loadInfos...)
	defer loader.unregister(infos...)

	// use full resource in case of whole segment reopen
	// TODO use calculated resource from segcore after supported
	requestResourceResult, err := loader.requestResource(ctx, infos...)
	if err != nil {
		mlog.Warn(context.TODO(), "reopen segment request resource failed", mlog.Err(err))
		return err
	}
	defer loader.freeRequestResource(requestResourceResult)

	for _, info := range infos {
		segment := loader.manager.Segment.GetSealed(info.GetSegmentID())
		if segment == nil {
			mlog.Warn(context.TODO(), "failed to reopen segment, segment not loaded", mlog.Int64("segmentID", info.GetSegmentID()))
			continue
		}
		collection := loader.manager.Collection.Get(info.GetCollectionID())
		if collection != nil {
			configureUseTakeForOutput(info, collection.Schema())
		}

		err := segment.Reopen(ctx, info)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to reopen segment", mlog.Int64("segmentID", info.GetSegmentID()), mlog.Err(err))
			return err
		}
	}

	return nil
}

func getBinlogDataDiskSize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.GetLogSize()
	}

	return fieldSize
}

func getBinlogDataMemorySize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.GetMemorySize()
	}

	return fieldSize
}

func gpuIndexRequiresGpu(indexParams []*commonpb.KeyValuePair) bool {
	indexParamMap := funcutil.KeyValuePair2Map(indexParams)
	indexType := indexParamMap[common.IndexTypeKey]

	switch indexType {
	case "GPU_CAGRA", "GPU_CUVS_CAGRA":
	case "GPU_BRUTE_FORCE", "GPU_CUVS_BRUTE_FORCE",
		"GPU_IVF_FLAT", "GPU_CUVS_IVF_FLAT",
		"GPU_IVF_PQ", "GPU_CUVS_IVF_PQ":
		return true
	default:
		return false
	}

	err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParamMap)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to append prepare load params for gpu index resource check",
			mlog.String("indexType", indexType),
			mlog.Err(err))
	}

	adaptForCPU, ok := indexParamMap["adapt_for_cpu"]
	if ok {
		enabled, err := strconv.ParseBool(adaptForCPU)
		if err == nil && enabled {
			return false
		}
	}
	return true
}

func checkSegmentGpuMemSize(fieldGpuMemSizeList []uint64, OverloadedMemoryThresholdPercentage float32) error {
	gpuInfos, err := hardware.GetAllGPUMemoryInfo()
	if err != nil {
		if len(fieldGpuMemSizeList) == 0 {
			return nil
		}
		return err
	}
	var usedGpuMem []uint64
	var maxGpuMemSize []uint64
	for _, gpuInfo := range gpuInfos {
		usedGpuMem = append(usedGpuMem, gpuInfo.TotalMemory-gpuInfo.FreeMemory)
		maxGpuMemSize = append(maxGpuMemSize, uint64(float32(gpuInfo.TotalMemory)*OverloadedMemoryThresholdPercentage))
	}
	currentGpuMem := usedGpuMem
	for _, fieldGpuMem := range fieldGpuMemSizeList {
		var minId int = -1
		var minGpuMem uint64 = math.MaxUint64
		for i := int(0); i < len(gpuInfos); i++ {
			GpuiMem := currentGpuMem[i] + fieldGpuMem
			if GpuiMem < maxGpuMemSize[i] && GpuiMem < minGpuMem {
				minId = i
				minGpuMem = GpuiMem
			}
		}
		if minId == -1 {
			mlog.Warn(context.TODO(), "load segment failed, GPU OOM if loaded",
				mlog.String("resourceType", "GPU"),
				mlog.Uint64("gpuMemUsageBytes", fieldGpuMem),
				mlog.Any("usedGpuMemBytes", usedGpuMem),
				mlog.Any("maxGpuMemBytes", maxGpuMemSize),
			)
			return merr.WrapErrSegmentRequestResourceFailed("GPU")
		}
		currentGpuMem[minId] = minGpuMem
	}
	return nil
}
