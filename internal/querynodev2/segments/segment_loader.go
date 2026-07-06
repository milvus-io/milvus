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
	"io"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
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
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
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

	// LoadIndex append index for segment and remove vector binlogs.
	LoadIndex(ctx context.Context,
		segment Segment,
		info *querypb.SegmentLoadInfo,
		version int64) error

	// ReopenSegments update segment data according to new load info.
	ReopenSegments(ctx context.Context,
		loadInfos []*querypb.SegmentLoadInfo,
	) error
}

type ResourceEstimate struct {
	MaxMemoryCost   uint64
	MaxDiskCost     uint64
	FinalMemoryCost uint64
	FinalDiskCost   uint64
	HasRawData      bool
}

func GetResourceEstimate(estimate *C.LoadResourceRequest) ResourceEstimate {
	return ResourceEstimate{
		MaxMemoryCost:   uint64(estimate.max_memory_cost),
		MaxDiskCost:     uint64(estimate.max_disk_cost),
		FinalMemoryCost: uint64(estimate.final_memory_cost),
		FinalDiskCost:   uint64(estimate.final_disk_cost),
		HasRawData:      bool(estimate.has_raw_data),
	}
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
	// segments. External tables always download, decompress and deserialize
	// row groups into Arrow buffers regardless of mmap / TieredEviction
	// settings, so peak transient memory = rawDataSize * factor. Defaults
	// to 2.0 via paramtable queryNode.externalCollection.rawDataFactor.
	externalRawDataFactor float64
}

func NewLoader(
	ctx context.Context,
	manager *Manager,
	cm storage.ChunkManager,
) *segmentLoader {
	duf := NewDiskUsageFetcher(ctx)
	go duf.Start()

	loader := &segmentLoader{
		manager:                   manager,
		cm:                        cm,
		loadingSegments:           typeutil.NewConcurrentMap[int64, *loadResult](),
		committedResourceNotifier: syncutil.NewVersionedNotifier(),
		duf:                       duf,
	}

	return loader
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

	mut                       sync.Mutex // guards committedResource
	committedResource         LoadResource
	committedLogicalResource  LoadResource
	committedResourceNotifier *syncutil.VersionedNotifier

	duf *diskUsageFetcher
}

var _ Loader = (*segmentLoader)(nil)

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

		for _, indexInfo := range loadInfo.IndexInfos {
			indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)

			// some build params also exist in indexParams, which are useless during loading process
			if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexParams["index_type"]) {
				if err := indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows()); err != nil {
					return nil, err
				}
			}

			// set whether enable offset cache for bitmap index
			if indexParams["index_type"] == indexparamcheck.IndexBitmap {
				indexparams.SetBitmapIndexLoadParams(paramtable.Get(), indexParams)
			}

			if err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams); err != nil {
				return nil, err
			}

			indexInfo.IndexParams = funcutil.Map2KeyValuePair(indexParams)
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

func configureUseTakeForOutput(loadInfo *querypb.SegmentLoadInfo, schema *schemapb.CollectionSchema) {
	if loadInfo == nil {
		return
	}
	if typeutil.IsExternalCollection(schema) {
		loadInfo.UseTakeForOutput = paramtable.Get().QueryNodeCfg.ExternalCollectionUseTakeForOutput.GetAsBool()
		return
	}
	loadInfo.UseTakeForOutput = paramtable.Get().QueryNodeCfg.InternalCollectionUseTakeForOutput.GetAsBool()
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

	loader.mut.Lock()
	defer loader.mut.Unlock()

	physicalMemoryUsage := hardware.GetUsedMemoryCount()
	totalMemory := hardware.GetMemoryCount()

	physicalDiskUsage, err := loader.duf.GetDiskUsage()
	if err != nil {
		return requestResourceResult{}, merr.Wrap(err, "get local used size failed")
	}
	diskCap := paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsUint64()

	result := requestResourceResult{
		CommittedResource: loader.committedResource,
	}

	if loader.committedResource.MemorySize+physicalMemoryUsage >= totalMemory {
		return result, merr.WrapErrServiceMemoryLimitExceeded(float32(loader.committedResource.MemorySize+physicalMemoryUsage), float32(totalMemory))
	} else if loader.committedResource.DiskSize+uint64(physicalDiskUsage) >= diskCap {
		return result, merr.WrapErrServiceDiskLimitExceeded(float32(loader.committedResource.DiskSize+uint64(physicalDiskUsage)), float32(diskCap))
	}

	result.ConcurrencyLevel = funcutil.Min(hardware.GetCPUNum(), len(infos))

	// TODO: disable logical resource checking for now
	// lmu, ldu, err := loader.checkLogicalSegmentSize(ctx, infos, totalMemory)
	// if err != nil {
	// 	mlog.Warn(context.TODO(), "no sufficient logical resource to load segments", mlog.Err(err))
	// 	return result, err
	// }

	if err := loader.checkLoadingResource(ctx, logger, loadingUsage, maxSegmentSize, totalMemory, physicalMemoryUsage, physicalDiskUsage); err != nil {
		return result, err
	}

	result.Resource.MemorySize = loadingUsage.MemorySize
	result.Resource.DiskSize = loadingUsage.DiskSize
	// result.LogicalResource.MemorySize = lmu
	// result.LogicalResource.DiskSize = ldu

	loader.committedResource.Add(result.Resource)
	// loader.committedLogicalResource.Add(result.LogicalResource)
	mlog.Info(context.TODO(), "request resource for loading segments (unit in MiB)",
		mlog.Float64("memory", logutil.ToMB(float64(result.Resource.MemorySize))),
		mlog.Float64("committedMemory", logutil.ToMB(float64(loader.committedResource.MemorySize))),
		mlog.Float64("disk", logutil.ToMB(float64(result.Resource.DiskSize))),
		mlog.Float64("committedDisk", logutil.ToMB(float64(loader.committedResource.DiskSize))),
	)

	return result, nil
}

// freeRequestResource returns request memory & storage usage request.
func (loader *segmentLoader) freeRequestResource(requestResourceResult requestResourceResult) {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	resource := requestResourceResult.Resource
	// logicalResource := requestResourceResult.LogicalResource

	if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		C.ReleaseLoadingResource(C.CResourceUsage{
			memory_bytes: C.int64_t(resource.MemorySize),
			disk_bytes:   C.int64_t(resource.DiskSize),
		})
	}

	loader.committedResource.Sub(resource)
	// loader.committedLogicalResource.Sub(logicalResource)
	loader.committedResourceNotifier.NotifyAll()
}

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

	mlog.Info(context.TODO(), "start loading remote...", mlog.Int("segmentNum", 1))

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
	mlog.Info(context.TODO(), "loading bloom filter for remote...")
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
	segmentNum := len(infos)
	if segmentNum == 0 {
		mlog.Info(context.TODO(), "no segment to load")
		return nil, nil
	}

	// Phase 1: always create metadata-only stubs (segmentID / partitionID / type).
	// This gives callers valid candidates even when BF data is not loaded,
	// so partition filtering and type-based delete-scope logic never need nil guards.
	bfSets := make([]*pkoracle.BloomFilterSet, segmentNum)
	for i, info := range infos {
		bfSets[i] = pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
	}

	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(collectionID)
		mlog.Warn(context.TODO(), "failed to get collection while loading segment", mlog.Err(err))
		return nil, err
	}

	schema := collection.Schema()
	isExternalCollection := typeutil.IsExternalCollection(schema)
	isMilvusTableRealPK := typeutil.NewStorageColumnResolver(schema).IsMilvusTable() &&
		HasExternalPrimaryKey(schema)

	// Phase 2: load BF stats into the stubs. Milvus-table real-PK correctness
	// depends on source bloom filters, so that path ignores the global BF
	// disable switch; other collections keep the historical metadata-only
	// behavior when BloomFilterEnabled=false.
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() && !isMilvusTableRealPK {
		mlog.Info(context.TODO(), "bloom filter disabled: returning metadata-only stubs")
		return bfSets, nil
	}

	// Virtual-PK external collections use ExternalSegmentCandidate and have no
	// reusable source-side PK stats.
	if isExternalCollection && !isMilvusTableRealPK {
		return bfSets, nil
	}

	pkField := GetPkField(schema)
	pkFieldID := pkField.GetFieldID()

	// Calculate total memory size needed for bloom filters (PK stats)
	var totalMemorySize int64
	for _, info := range infos {
		memSize, _ := packed.NewStatsResolverFromLoadInfo(info).BloomFilterMemorySize(pkFieldID)
		totalMemorySize += memSize
	}

	// Reserve memory resource if tiered eviction is enabled
	if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() && totalMemorySize > 0 {
		if ok := C.TryReserveLoadingResourceWithTimeout(C.CResourceUsage{
			// double loading memory size for bloom filters to avoid OOM during loading
			memory_bytes: C.int64_t(totalMemorySize * 2),
			disk_bytes:   C.int64_t(0),
		}, 1000); !ok {
			return nil, merr.WrapErrSegmentRequestResourceFailed("memory",
				fmt.Sprintf("failed to reserve loading resource for bloom filters, totalMemorySize = %v MB",
					logutil.ToMB(float64(totalMemorySize))))
		}
		mlog.Info(context.TODO(), "reserved loading resource for bloom filters", mlog.Float64("totalMemorySizeMB", logutil.ToMB(float64(totalMemorySize))))
	}

	defer func() {
		if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() && totalMemorySize > 0 {
			C.ReleaseLoadingResource(C.CResourceUsage{
				memory_bytes: C.int64_t(totalMemorySize * 2),
				disk_bytes:   C.int64_t(0),
			})
			mlog.Info(context.TODO(), "released loading resource for bloom filters", mlog.Float64("totalMemorySizeMB", logutil.ToMB(float64(totalMemorySize))))
		}
	}()

	mlog.Info(context.TODO(), "start loading remote...", mlog.Int("segmentNum", segmentNum))

	loadRemoteFunc := func(idx int) error {
		loadInfo := infos[idx]
		bfs := bfSets[idx]

		mlog.Info(context.TODO(), "loading bloom filter for remote...")
		pkStatsBinlogs, err := packed.NewStatsResolverFromLoadInfo(loadInfo).BloomFilterPaths(pkFieldID)
		if err != nil {
			return err
		}
		err = loader.loadBloomFilter(ctx, bfs.ID(), bfs, pkStatsBinlogs, loader.bloomFilterDownloader(collection, isMilvusTableRealPK))
		if err != nil {
			mlog.Warn(context.TODO(), "load remote segment bloom filter failed",
				mlog.Int64("partitionID", bfs.Partition()),
				mlog.Int64("segmentID", bfs.ID()),
				mlog.Err(err),
			)
			return err
		}
		if isMilvusTableRealPK && !bfs.PkCandidateExist() {
			return merr.WrapErrServiceInternalMsg("milvus-table real-PK segment missing bloom filter stats")
		}
		return nil
	}

	err := funcutil.ProcessFuncParallel(segmentNum, segmentNum, loadRemoteFunc, "loadRemoteFunc")
	if err != nil {
		// no partial success here
		mlog.Warn(context.TODO(), "failed to load remote segment", mlog.Err(err))
		return nil, err
	}

	// Charge loaded resource for bloom filters
	for _, bfs := range bfSets {
		bfs.Charge()
	}

	return bfSets, nil
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
	rootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
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

	collection := segment.GetCollection()
	indexedFieldInfos, _, textIndexes, unindexedTextFields, jsonKeyStats, _, _ := separateLoadInfoV2(loadInfo, collection.Schema())

	tr := timerecord.NewTimeRecorder("segmentLoader.loadSealedSegment")
	mlog.Info(context.TODO(), "Start loading fields...",
		mlog.Int("indexedFields count", len(indexedFieldInfos)),
		mlog.Int64s("indexed text fields", lo.Keys(textIndexes)),
		mlog.Int64s("unindexed text fields", lo.Keys(unindexedTextFields)),
		mlog.Int64s("indexed json key fields", lo.Keys(jsonKeyStats)),
	)
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
	mlog.Info(context.TODO(), "Finish loading segment",
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
	mlog.Info(context.TODO(), "start loading segment files",
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

func (loader *segmentLoader) loadFieldsIndex(ctx context.Context,
	schemaHelper *typeutil.SchemaHelper,
	segment *LocalSegment,
	numRows int64,
	indexedFieldInfos map[int64]*IndexedFieldInfo,
) error {
	for _, fieldInfo := range indexedFieldInfos {
		fieldID := fieldInfo.IndexInfo.FieldID
		indexInfo := fieldInfo.IndexInfo
		tr := timerecord.NewTimeRecorder("loadFieldIndex")
		err := loader.loadFieldIndex(ctx, segment, indexInfo)
		loadFieldIndexSpan := tr.RecordSpan()
		if err != nil {
			return err
		}

		mlog.Info(context.TODO(), "load field binlogs done for sealed segment with index",
			mlog.Int64("fieldID", fieldID),
			mlog.Any("binlog", fieldInfo.FieldBinlog.Binlogs),
			mlog.Int32("current_index_version", fieldInfo.IndexInfo.GetCurrentIndexVersion()),
			mlog.Duration("load_duration", loadFieldIndexSpan),
		)

		// set average row data size of variable field
		field, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			return err
		}
		if typeutil.IsVariableDataType(field.GetDataType()) {
			err = segment.UpdateFieldRawDataSize(ctx, numRows, fieldInfo.FieldBinlog)
			if err != nil {
				return err
			}
		}
	}

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

func (loader *segmentLoader) loadFieldIndex(ctx context.Context, segment *LocalSegment, indexInfo *querypb.FieldIndexInfo) error {
	filteredPaths := make([]string, 0, len(indexInfo.IndexFilePaths))

	for _, indexPath := range indexInfo.IndexFilePaths {
		if path.Base(indexPath) != storage.IndexParamsKey {
			filteredPaths = append(filteredPaths, indexPath)
		}
	}

	indexInfo.IndexFilePaths = filteredPaths
	fieldType, err := loader.getFieldType(segment.Collection(), indexInfo.FieldID)
	if err != nil {
		return err
	}

	collection := loader.manager.Collection.Get(segment.Collection())
	if collection == nil {
		return merr.WrapErrCollectionNotLoaded(segment.Collection(), "failed to load field index")
	}

	return segment.LoadIndex(ctx, indexInfo, fieldType)
}

func (loader *segmentLoader) loadBloomFilter(
	ctx context.Context,
	segmentID int64,
	bfs *pkoracle.BloomFilterSet,
	binlogPaths []string,
	downloader func(context.Context, []string) ([][]byte, error),
) error {
	return loader.loadBloomFilterWithDownloader(ctx, segmentID, bfs, binlogPaths, downloader)
}

// bloomFilterDownloader returns the byte downloader used for PK bloom-filter
// stats. Milvus-table real-PK stats live in the external source filesystem;
// ordinary internal stats stay on the local chunk manager.
func (loader *segmentLoader) bloomFilterDownloader(
	collection *Collection,
	useExternalSpec bool,
) func(context.Context, []string) ([][]byte, error) {
	if !useExternalSpec {
		return loader.cm.MultiRead
	}
	schema := collection.Schema()
	extfs := packed.ExternalSpecContext{
		CollectionID: collection.ID(),
		Source:       schema.GetExternalSource(),
		Spec:         schema.GetExternalSpec(),
	}
	return func(ctx context.Context, paths []string) ([][]byte, error) {
		return readExternalFiles(ctx, createStorageConfig(), extfs, paths)
	}
}

// loadBloomFilterWithDownloader merges one or more serialized PK bloom-filter
// stats into the segment BloomFilterSet.
func (loader *segmentLoader) loadBloomFilterWithDownloader(
	ctx context.Context,
	segmentID int64,
	bfs *pkoracle.BloomFilterSet,
	binlogPaths []string,
	downloader func(context.Context, []string) ([][]byte, error),
) error {
	if len(binlogPaths) == 0 {
		mlog.Info(context.TODO(), "there are no stats logs saved with segment")
		return nil
	}

	startTs := time.Now()
	values, err := downloader(ctx, binlogPaths)
	if err != nil {
		return err
	}
	blobs := make([]*storage.Blob, len(values))
	for i := range values {
		blobs[i] = &storage.Blob{Value: values[i]}
	}

	stats, err := storage.DeserializeBloomFilterStats(binlogPaths, blobs)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to deserialize bloom filter stats", mlog.Err(err))
		return err
	}

	var size uint
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		bfs.AddHistoricalStats(pkStat)
	}
	mlog.Info(context.TODO(), "Successfully load pk stats", mlog.Duration("time", time.Since(startTs)), mlog.Uint("size", size))
	return nil
}

// loadDeltalogs performs the internal actions of `LoadDeltaLogs`
// this function does not perform resource check and is meant be used among other load APIs.
func (loader *segmentLoader) loadDeltalogs(ctx context.Context, segment Segment, loadInfo *querypb.SegmentLoadInfo) error {
	deltaLogs := loadInfo.GetDeltalogs()
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadDeltalogs-%d", segment.ID()))
	defer sp.End()
	mlog.Info(context.TODO(), "loading delta...")

	var rowNums int64
	valid := func(binlog *datapb.Binlog, _ int) bool {
		// the segment has applied the delta logs, skip it
		if binlog.GetTimestampTo() > 0 && // this field may be missed in legacy versions
			binlog.GetTimestampTo() < segment.LastDeltaTimestamp() {
			return false
		}
		return true
	}
	for _, deltaLog := range deltaLogs {
		rowNums += lo.SumBy(lo.Filter(deltaLog.GetBinlogs(), valid), func(binlog *datapb.Binlog) int64 {
			return binlog.GetEntriesNum()
		})
	}

	collection := loader.manager.Collection.Get(segment.Collection())

	helper, _ := typeutil.CreateSchemaHelper(collection.Schema())
	pkField, _ := helper.GetPrimaryKeyField()
	deltaData, err := storage.NewDeltaDataWithPkType(rowNums, pkField.DataType)
	if err != nil {
		return err
	}

	readDeltaRecords := func(reader storage.RecordReader) error {
		defer reader.Close()
		for {
			dl, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			for i := 0; i < dl.Len(); i++ {
				var pk storage.PrimaryKey
				switch pkField.DataType {
				case schemapb.DataType_Int64:
					pk = storage.NewInt64PrimaryKey(dl.Column(0).(*array.Int64).Value(i))
				case schemapb.DataType_VarChar:
					pk = storage.NewVarCharPrimaryKey(dl.Column(0).(*array.String).Value(i))
				}
				ts := typeutil.Timestamp(dl.Column(1).(*array.Int64).Value(i))
				err = deltaData.Append(pk, ts)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	schema := collection.Schema()
	isExternalCollection := typeutil.IsExternalCollection(schema)
	resolver := typeutil.NewStorageColumnResolver(schema)
	if isExternalCollection && !resolver.IsMilvusTable() {
		mlog.Info(context.TODO(), "skip loading delta logs for non-milvus-table external collection")
		return nil
	}
	isMilvusTableRealPK := resolver.IsMilvusTable() && HasExternalPrimaryKey(schema)
	useExplicitDeltalogs := isMilvusTableRealPK && len(deltaLogs) > 0
	readPaths := func(paths []string, opts ...storage.RwOption) error {
		if len(paths) == 0 {
			return nil
		}
		reader, err := storage.NewDeltalogReader(pkField.DataType, paths, opts...)
		if err != nil {
			return err
		}
		return readDeltaRecords(reader)
	}

	if manifestPath := loadInfo.GetManifestPath(); manifestPath != "" && !useExplicitDeltalogs {
		if isMilvusTableRealPK {
			// Real-PK milvus-table manifests keep source deltalogs. Target-owned
			// deltalogs are only valid for virtual-PK translation.
			extfs := packed.ExternalSpecContext{
				CollectionID: collection.ID(),
				Source:       schema.GetExternalSource(),
				Spec:         schema.GetExternalSpec(),
			}
			sourceDeltalogs, err := packed.GetDeltaLogsFromManifestWithExtfs(
				manifestPath,
				createStorageConfig(),
				extfs,
			)
			if err != nil {
				return err
			}
			if err := validateMilvusTableRealPKDeltalogPaths(manifestPath, milvusTableDeltalogPaths(sourceDeltalogs)); err != nil {
				return err
			}
			if len(sourceDeltalogs) > 0 {
				storageV3Paths := make([]string, 0)
				legacyPaths := make([]string, 0)
				for _, deltalog := range sourceDeltalogs {
					for _, binlog := range lo.Filter(deltalog.GetBinlogs(), valid) {
						if packed.IsMilvusTableStorageV3DeltalogPath(binlog.GetLogPath()) {
							storageV3Paths = append(storageV3Paths, binlog.GetLogPath())
						} else {
							legacyPaths = append(legacyPaths, binlog.GetLogPath())
						}
					}
				}
				if len(storageV3Paths) > 0 {
					reader, err := storage.NewDeltalogReader(
						pkField.DataType,
						storageV3Paths,
						storage.WithVersion(storage.StorageV3),
						storage.WithStorageConfig(createStorageConfig()),
						storage.WithExternalReaderContext(extfs),
					)
					if err != nil {
						return err
					}
					if err := readDeltaRecords(reader); err != nil {
						return err
					}
				}
				if len(legacyPaths) > 0 {
					reader, err := storage.NewDeltalogReader(
						pkField.DataType,
						legacyPaths,
						storage.WithVersion(storage.StorageV1),
						storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
							return readExternalFiles(ctx, createStorageConfig(), extfs, paths)
						}),
					)
					if err != nil {
						return err
					}
					if err := readDeltaRecords(reader); err != nil {
						return err
					}
				}
			}
		} else {
			// V3: delta data lives in manifest.
			paths, err := packed.GetDeltaLogPathsFromManifest(manifestPath, createStorageConfig())
			if err != nil {
				return err
			}
			if err := readPaths(paths,
				storage.WithStorageConfig(createStorageConfig()),
				storage.WithVersion(storage.StorageV3),
			); err != nil {
				return err
			}
		}
	} else {
		// V1: delta data referenced by Deltalogs entries
		var paths []string
		for _, deltalog := range deltaLogs {
			for _, binlog := range lo.Filter(deltalog.Binlogs, valid) {
				if p := binlog.GetLogPath(); p != "" {
					paths = append(paths, p)
				}
			}
		}
		if err := readPaths(paths,
			storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
				return loader.cm.MultiRead(ctx, paths)
			}),
		); err != nil {
			return err
		}
	}

	err = segment.LoadDeltaData(ctx, deltaData)
	if err != nil {
		return err
	}

	mlog.Info(context.TODO(), "load delta logs done", mlog.Int64("deleteCount", deltaData.DeleteRowCount()))
	return nil
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
		}
	}
	return &indexpb.StorageConfig{
		Address:           params.MinioCfg.Address.GetValue(),
		AccessKeyID:       params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:   params.MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:            params.MinioCfg.UseSSL.GetAsBool(),
		SslCACert:         params.MinioCfg.SslCACert.GetValue(),
		BucketName:        params.MinioCfg.BucketName.GetValue(),
		RootPath:          params.MinioCfg.RootPath.GetValue(),
		UseIAM:            params.MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:       params.MinioCfg.IAMEndpoint.GetValue(),
		StorageType:       params.CommonCfg.StorageType.GetValue(),
		Region:            params.MinioCfg.Region.GetValue(),
		UseVirtualHost:    params.MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:     params.MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs:  params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
		GcpCredentialJSON: params.MinioCfg.GcpCredentialJSON.GetValue(),
		SslTlsMinVersion:  params.MinioCfg.SslTLSMinVersion.GetValue(),
		UseCrc32CChecksum: params.MinioCfg.UseCRC32C.GetAsBool(),
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
func (loader *segmentLoader) checkLoadingResource(
	ctx context.Context,
	logger *mlog.Logger,
	loadingUsage *ResourceUsage,
	maxSegmentSize uint64,
	totalMem uint64,
	memUsage uint64,
	localDiskUsage int64,
) error {
	memUsage += loader.committedResource.MemorySize
	if memUsage == 0 || totalMem == 0 {
		return merr.WrapErrServiceInternalMsg("get memory failed when checkLoadingResource")
	}

	diskUsage := uint64(localDiskUsage) + loader.committedResource.DiskSize
	predictMemUsage := memUsage + loadingUsage.MemorySize
	predictDiskUsage := diskUsage + loadingUsage.DiskSize

	logger.Info(ctx, "predict memory and disk usage while loading (in MiB)",
		mlog.Float64("maxSegmentSize(MB)", logutil.ToMB(float64(maxSegmentSize))),
		mlog.Float64("committedMemSize(MB)", logutil.ToMB(float64(loader.committedResource.MemorySize))),
		mlog.Float64("memLimit(MB)", logutil.ToMB(float64(totalMem))),
		mlog.Float64("memUsage(MB)", logutil.ToMB(float64(memUsage))),
		mlog.Float64("committedDiskSize(MB)", logutil.ToMB(float64(loader.committedResource.DiskSize))),
		mlog.Float64("diskUsage(MB)", logutil.ToMB(float64(diskUsage))),
		mlog.Float64("predictMemUsage(MB)", logutil.ToMB(float64(predictMemUsage))),
		mlog.Float64("predictDiskUsage(MB)", logutil.ToMB(float64(predictDiskUsage))),
		mlog.Int("mmapFieldCount", loadingUsage.MmapFieldCount),
	)

	var loadingResource C.CResourceUsage
	reservedLoadingResource := false
	if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		loadingResource = C.CResourceUsage{
			memory_bytes: C.int64_t(loadingUsage.MemorySize),
			disk_bytes:   C.int64_t(loadingUsage.DiskSize),
		}

		// try to reserve loading resource from caching layer
		if ok := C.TryReserveLoadingResourceWithTimeout(loadingResource, 1000); !ok {
			return merr.WrapErrSegmentRequestResourceFailed("memory/disk",
				fmt.Sprintf("failed to reserve loading resource from caching layer, predictMemUsage = %v MB, predictDiskUsage = %v MB, memUsage = %v MB, diskUsage = %v MB, memoryThresholdFactor = %f, diskThresholdFactor = %f",
					logutil.ToMB(float64(predictMemUsage)),
					logutil.ToMB(float64(predictDiskUsage)),
					logutil.ToMB(float64(memUsage)),
					logutil.ToMB(float64(diskUsage)),
					paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(),
					paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat(),
				))
		}
		reservedLoadingResource = true
	} else {
		// fallback to original segment loading logic
		if predictMemUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
			mlog.Warn(context.TODO(), "load segment failed, OOM if load",
				mlog.String("resourceType", "Memory"),
				mlog.Float64("maxSegmentSizeMB", logutil.ToMB(float64(maxSegmentSize))),
				mlog.Float64("memUsageMB", logutil.ToMB(float64(memUsage))),
				mlog.Float64("predictMemUsageMB", logutil.ToMB(float64(predictMemUsage))),
				mlog.Float64("totalMemMB", logutil.ToMB(float64(totalMem))),
				mlog.Float64("thresholdFactor", paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()),
			)
			return merr.WrapErrSegmentRequestResourceFailed("Memory")
		}

		if predictDiskUsage > uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())*paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()) {
			mlog.Warn(context.TODO(), "load segment failed, disk space is not enough",
				mlog.String("resourceType", "Disk"),
				mlog.Float64("diskUsageMB", logutil.ToMB(float64(diskUsage))),
				mlog.Float64("predictDiskUsageMB", logutil.ToMB(float64(predictDiskUsage))),
				mlog.Float64("totalDiskMB", logutil.ToMB(float64(uint64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())))),
				mlog.Float64("thresholdFactor", paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()),
			)
			return merr.WrapErrSegmentRequestResourceFailed("Disk")
		}
	}

	err := checkSegmentGpuMemSize(loadingUsage.FieldGpuMemorySize, float32(paramtable.Get().GpuConfig.OverloadedMemoryThresholdPercentage.GetAsFloat()))
	if err != nil {
		if reservedLoadingResource {
			C.ReleaseLoadingResource(loadingResource)
		}
		return err
	}

	return nil
}

// this function is used to estimate the logical resource usage of a segment, which should only be used when tiered eviction is enabled
// the result is the final resource usage of the segment inevictable part plus the final usage of evictable part with cache ratio applied
// TODO: the inevictable part is not correct, since we cannot know the final resource usage of interim index and default-value column before loading,
// current they are ignored, but we should consider them in the future
func estimateLogicalResourceUsageOfSegment(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, multiplyFactor resourceEstimateFactor) (usage *ResourceUsage, err error) {
	var segmentInevictableMemorySize, segmentInevictableDiskSize uint64
	var segmentEvictableMemorySize, segmentEvictableDiskSize uint64

	id2Binlogs := lo.SliceToMap(loadInfo.BinlogPaths, func(fieldBinlog *datapb.FieldBinlog) (int64, *datapb.FieldBinlog) {
		return fieldBinlog.GetFieldID(), fieldBinlog
	})

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create schema helper", mlog.String("name", schema.GetName()), mlog.Err(err))
		return nil, err
	}
	ctx := context.TODO()

	// PART 1: calculate logical resource usage of indexes
	for _, fieldIndexInfo := range loadInfo.IndexInfos {
		fieldID := fieldIndexInfo.GetFieldID()
		if len(fieldIndexInfo.GetIndexFilePaths()) > 0 {
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				return nil, err
			}
			isVectorType := typeutil.IsVectorType(fieldSchema.GetDataType())

			var estimateResult ResourceEstimate
			err = GetCLoadInfoWithFunc(ctx, fieldSchema, loadInfo, fieldIndexInfo, func(c *LoadIndexInfo) error {
				GetDynamicPool().Submit(func() (any, error) {
					loadResourceRequest := C.EstimateLoadIndexResource(c.cLoadIndexInfo)
					estimateResult = GetResourceEstimate(&loadResourceRequest)
					return nil, nil
				}).Await()
				return nil
			})
			if err != nil {
				return nil, merr.Wrapf(err, "failed to estimate logical resource usage of index, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}
			if isIndexEvictableEnable(fieldSchema, fieldIndexInfo) {
				segmentEvictableMemorySize += estimateResult.FinalMemoryCost
				segmentEvictableDiskSize += estimateResult.FinalDiskCost
			} else {
				segmentInevictableMemorySize += estimateResult.FinalMemoryCost
				segmentInevictableDiskSize += estimateResult.FinalDiskCost
			}

			// could skip binlog or
			// could be missing for new field or storage v2 group 0
			if estimateResult.HasRawData &&
				!paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool() {
				delete(id2Binlogs, fieldID)
				continue
			}

			// BM25 only checks vector datatype
			// scalar index does not have metrics type key
			if !isVectorType {
				continue
			}

			metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, fieldIndexInfo.IndexParams)
			if err != nil {
				return nil, merr.Wrapf(err, "failed to estimate logical resource usage of index, metric type not found, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}
			// skip raw data for BM25 index
			if metricType == metric.BM25 {
				delete(id2Binlogs, fieldID)
			}
		}
	}

	// PART 2: calculate logical resource usage of binlogs
	for fieldID, fieldBinlog := range id2Binlogs {
		fieldIDs := fieldBinlog.GetChildFields()
		// legacy default split
		if len(fieldIDs) == 0 {
			fieldIDs = []int64{fieldID}
		}
		binlogSize := uint64(getBinlogDataMemorySize(fieldBinlog))

		var supportInterimIndexDataType bool
		var containsTimestampField bool
		var doubleMemoryDataField bool
		var legacyNilSchema bool
		mmapEnabled := true
		evictableEnabled := true
		isVectorType := true

		for _, fieldID := range fieldIDs {
			// get field schema from fieldID
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				mlog.Warn(context.TODO(), "failed to get field schema", mlog.Int64("fieldID", fieldID), mlog.String("name", schema.GetName()), mlog.Err(err))
				return nil, err
			}

			// missing mapping, shall be "0" group for storage v2
			if fieldSchema == nil {
				legacyNilSchema = true
				break
			}

			supportInterimIndexDataType = supportInterimIndexDataType || SupportInterimIndexDataType(fieldSchema.GetDataType())
			isVectorType = isVectorType && typeutil.IsVectorType(fieldSchema.GetDataType())
			// constainSystemField = constainSystemField || common.IsSystemField(fieldSchema.GetFieldID())
			mmapEnabled = mmapEnabled && isDataMmapEnable(fieldSchema)
			evictableEnabled = evictableEnabled && isDataEvictableEnable(fieldSchema)
			containsTimestampField = containsTimestampField || DoubleMemorySystemField(fieldSchema.GetFieldID())
			doubleMemoryDataField = doubleMemoryDataField || DoubleMemoryDataType(fieldSchema.GetDataType())
		}

		// TODO: add default-value column's resource usage to inevictable part
		// TODO: add interim index's resource usage to inevictable part

		if legacyNilSchema {
			segmentEvictableMemorySize += binlogSize
			continue
		}

		// timestamp field double in InsertRecord & TimestampIndex
		if containsTimestampField {
			timestampSize := lo.SumBy(fieldBinlog.GetBinlogs(), func(binlog *datapb.Binlog) int64 {
				return binlog.GetEntriesNum() * 4
			})
			segmentInevictableMemorySize += 2 * uint64(timestampSize)
		}

		if isVectorType {
			mmapVectorField := paramtable.Get().QueryNodeCfg.MmapVectorField.GetAsBool()
			if mmapVectorField {
				if evictableEnabled {
					segmentEvictableDiskSize += binlogSize
				} else {
					segmentInevictableDiskSize += binlogSize
				}
			} else {
				if evictableEnabled {
					segmentEvictableMemorySize += binlogSize
				} else {
					segmentInevictableMemorySize += binlogSize
				}
			}
		} else if !mmapEnabled {
			if evictableEnabled {
				segmentEvictableMemorySize += binlogSize
			} else {
				segmentInevictableMemorySize += binlogSize
			}
			if doubleMemoryDataField {
				if evictableEnabled {
					segmentEvictableMemorySize += binlogSize
				} else {
					segmentInevictableMemorySize += binlogSize
				}
			}
		} else {
			if evictableEnabled {
				segmentEvictableDiskSize += binlogSize
			} else {
				segmentInevictableDiskSize += binlogSize
			}
		}
	}

	// PART 3: calculate logical resource usage of stats data
	for _, fieldBinlog := range loadInfo.Statslogs {
		segmentInevictableMemorySize += uint64(getBinlogDataMemorySize(fieldBinlog))
	}

	// PART 4: calculate logical resource usage of delete data
	for _, fieldBinlog := range loadInfo.Deltalogs {
		// MemorySize of filedBinlog is the actual size in memory, so the expansionFactor
		//   should be 1, in most cases.
		expansionFactor := float64(1)
		memSize := getBinlogDataMemorySize(fieldBinlog)

		// Note: If MemorySize == DiskSize, it means the segment comes from Milvus 2.3,
		//   MemorySize is actually compressed DiskSize of deltalog, so we'll fallback to use
		//   deltaExpansionFactor to compromise the compression ratio.
		if memSize == getBinlogDataDiskSize(fieldBinlog) {
			expansionFactor = multiplyFactor.deltaDataExpansionFactor
		}
		segmentInevictableMemorySize += uint64(float64(memSize) * expansionFactor)
	}

	// PART 5: calculate logical resource usage of text index stats data
	// Text match indexes are evictable (support_eviction=true in caching layer).
	for _, textStats := range loadInfo.GetTextStatsLogs() {
		fieldSchema, err := schemaHelper.GetFieldFromID(textStats.GetFieldID())
		if err != nil {
			return nil, err
		}
		textStatsSize := uint64(float64(textStats.GetMemorySize()) * multiplyFactor.textIndexExpansionFactor)
		if isDataMmapEnable(fieldSchema) {
			if isScalarStatsEvictableEnable(fieldSchema) {
				segmentEvictableDiskSize += textStatsSize
			} else {
				segmentInevictableDiskSize += textStatsSize
			}
		} else {
			if isScalarStatsEvictableEnable(fieldSchema) {
				segmentEvictableMemorySize += textStatsSize
			} else {
				segmentInevictableMemorySize += textStatsSize
			}
		}
	}

	mlog.Debug(context.TODO(), "estimate logical resoure usage result",
		mlog.Int64("segmentID", loadInfo.GetSegmentID()),
		mlog.Uint64("segmentInevictableMemorySize", segmentInevictableMemorySize),
		mlog.Uint64("segmentEvictableMemorySize", segmentEvictableMemorySize),
		mlog.Uint64("segmentInevictableDiskSize", segmentInevictableDiskSize),
		mlog.Uint64("segmentEvictableDiskSize", segmentEvictableDiskSize),
	)

	return &ResourceUsage{
		MemorySize: segmentInevictableMemorySize + uint64(float64(segmentEvictableMemorySize)*multiplyFactor.TieredEvictableMemoryCacheRatio),
		DiskSize:   segmentInevictableDiskSize + uint64(float64(segmentEvictableDiskSize)*multiplyFactor.TieredEvictableDiskCacheRatio),
	}, nil
}

// estimateLoadingResourceUsageOfSegment estimates the resource usage of the segment when loading,
// it will return two different results, depending on the value of tiered eviction parameter:
//   - when tiered eviction is enabled, the result is the max resource usage of the segment that cannot be managed by caching layer,
//     which should be a subset of the segment inevictable part
//   - when tiered eviction is disabled, the result is the max resource usage of both the segment evictable and inevictable part
func estimateLoadingResourceUsageOfSegment(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, multiplyFactor resourceEstimateFactor) (usage *ResourceUsage, err error) {
	var segMemoryLoadingSize, segDiskLoadingSize uint64
	var indexMemorySize uint64
	var mmapFieldCount int
	var fieldGpuMemorySize []uint64

	id2Binlogs := lo.SliceToMap(loadInfo.BinlogPaths, func(fieldBinlog *datapb.FieldBinlog) (int64, *datapb.FieldBinlog) {
		return fieldBinlog.GetFieldID(), fieldBinlog
	})

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create schema helper", mlog.String("name", schema.GetName()), mlog.Err(err))
		return nil, err
	}
	indexedFields := make(map[int64]struct{})
	ctx := context.Background()

	// PART 1: calculate size of indexes
	for _, fieldIndexInfo := range loadInfo.IndexInfos {
		fieldID := fieldIndexInfo.GetFieldID()
		if len(fieldIndexInfo.GetIndexFilePaths()) > 0 {
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				// field might have been dropped, skip its index
				mlog.Info(ctx, "skip index for dropped field", mlog.FieldFieldID(fieldID), mlog.String("name", schema.GetName()))
				continue
			}
			indexedFields[fieldID] = struct{}{}

			isVectorType := typeutil.IsVectorType(fieldSchema.GetDataType())

			var estimateResult ResourceEstimate
			err = GetCLoadInfoWithFunc(ctx, fieldSchema, loadInfo, fieldIndexInfo, func(c *LoadIndexInfo) error {
				GetDynamicPool().Submit(func() (any, error) {
					loadResourceRequest := C.EstimateLoadIndexResource(c.cLoadIndexInfo)
					estimateResult = GetResourceEstimate(&loadResourceRequest)
					return nil, nil
				}).Await()
				return nil
			})
			if err != nil {
				return nil, merr.Wrapf(err, "failed to estimate loading resource usage of index, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}

			if !multiplyFactor.TieredEvictionEnabled || !isIndexEvictableEnable(fieldSchema, fieldIndexInfo) {
				indexMemorySize += estimateResult.MaxMemoryCost
				segDiskLoadingSize += estimateResult.MaxDiskCost
			}

			if gpuIndexRequiresGpu(fieldIndexInfo.IndexParams) {
				fieldGpuMemorySize = append(fieldGpuMemorySize, estimateResult.MaxMemoryCost)
			}

			// could skip binlog or
			// could be missing for new field or storage v2 group 0
			if estimateResult.HasRawData &&
				!paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool() {
				delete(id2Binlogs, fieldID)
				continue
			}

			// BM25 only checks vector datatype
			// scalar index does not have metrics type key
			if !isVectorType {
				continue
			}

			metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, fieldIndexInfo.IndexParams)
			if err != nil {
				return nil, merr.Wrapf(err, "failed to estimate loading resource usage of index, metric type not found, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}
			// skip raw data for BM25 index
			if metricType == metric.BM25 {
				delete(id2Binlogs, fieldID)
			}
		}
	}

	// PART 2: calculate size of binlogs
	for fieldID, fieldBinlog := range id2Binlogs {
		fieldIDs := fieldBinlog.GetChildFields()
		// legacy default split
		if len(fieldIDs) == 0 {
			fieldIDs = []int64{fieldID}
		}
		binlogSize := uint64(getBinlogDataMemorySize(fieldBinlog))

		var supportInterimIndexDataType bool
		var containsTimestampField bool
		var doubleMomoryDataField bool
		var legacyNilSchema bool
		mmapEnabled := true
		evictableEnabled := true
		isVectorType := true
		hasIndex := true

		for _, fieldID := range fieldIDs {
			// get field schema from fieldID
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				// field might have been dropped, skip it and continue processing
				// other fields in the same column group
				mlog.Info(ctx, "skip binlog for dropped field", mlog.FieldFieldID(fieldID), mlog.String("name", schema.GetName()))
				continue
			}
			if _, ok := indexedFields[fieldID]; !ok {
				hasIndex = false
			}

			// missing mapping, shall be "0" group for storage v2
			if fieldSchema == nil {
				if !multiplyFactor.TieredEvictionEnabled {
					segMemoryLoadingSize += binlogSize
				}
				legacyNilSchema = true
				break
			}

			supportInterimIndexDataType = supportInterimIndexDataType || SupportInterimIndexDataType(fieldSchema.GetDataType())
			isVectorType = isVectorType && typeutil.IsVectorType(fieldSchema.GetDataType())
			mmapEnabled = mmapEnabled && isDataMmapEnable(fieldSchema)
			evictableEnabled = evictableEnabled && isDataEvictableEnable(fieldSchema)
			containsTimestampField = containsTimestampField || DoubleMemorySystemField(fieldSchema.GetFieldID())
			doubleMomoryDataField = doubleMomoryDataField || DoubleMemoryDataType(fieldSchema.GetDataType())
		}
		// legacy v2 segment without children
		if legacyNilSchema {
			continue
		}

		if !hasIndex {
			if !multiplyFactor.TieredEvictionEnabled {
				interimIndexEnable := multiplyFactor.EnableInterminSegmentIndex && !isGrowingMmapEnable() && supportInterimIndexDataType
				if interimIndexEnable {
					segMemoryLoadingSize += uint64(float64(binlogSize) * multiplyFactor.tempSegmentIndexFactor)
				}
			}
		}

		if isVectorType {
			mmapVectorField := paramtable.Get().QueryNodeCfg.MmapVectorField.GetAsBool()
			if mmapVectorField {
				if !multiplyFactor.TieredEvictionEnabled || !evictableEnabled {
					segDiskLoadingSize += binlogSize
				}
			} else {
				if !multiplyFactor.TieredEvictionEnabled || !evictableEnabled {
					segMemoryLoadingSize += binlogSize
				}
			}
			continue
		}

		// timestamp field double in InsertRecord & TimestampIndex
		if containsTimestampField {
			timestampSize := lo.SumBy(fieldBinlog.GetBinlogs(), func(binlog *datapb.Binlog) int64 {
				return binlog.GetEntriesNum() * 4
			})
			segMemoryLoadingSize += 2 * uint64(timestampSize)
		}

		if !mmapEnabled {
			if !multiplyFactor.TieredEvictionEnabled || !evictableEnabled {
				segMemoryLoadingSize += binlogSize
				if doubleMomoryDataField {
					segMemoryLoadingSize += binlogSize
				}
			}
		} else {
			if !multiplyFactor.TieredEvictionEnabled || !evictableEnabled {
				segDiskLoadingSize += uint64(getBinlogDataMemorySize(fieldBinlog))
			}
		}
	}

	// PART 2.5: external segment adjustments
	//
	// External segments carry pre-computed MemorySize in fake binlogs (from
	// DataNode Take sampling). Adjust the memory estimate for two external-
	// specific behaviors:
	//   1. Non-lazy path: apply externalRawDataFactor to cover the peak
	//      transient memory during download + decompress + Arrow deserialize
	//      (normal packed segments do not have this peak because their
	//      binlogs are already in Arrow IPC format).
	//   2. Full-lazy path (all external fields warmup=disable): no eager
	//      load, so subtract the raw data size that PART 2 added.
	// Also propagate EstimatedBytesPerRow to the C++ ManifestGroupTranslator
	// so the tiered-cache layer sizes chunks correctly.
	if typeutil.IsExternalCollection(schema) && loadInfo.GetNumOfRows() > 0 {
		var fakeBinlogMemSize int64
		for _, fb := range loadInfo.BinlogPaths {
			fakeBinlogMemSize += getBinlogDataMemorySize(fb)
		}
		loadInfo.EstimatedBytesPerRow = fakeBinlogMemSize / loadInfo.GetNumOfRows()

		if isExternalCollectionLazyLoad(schema) {
			// Full-lazy → zero eager load. Undo PART 2's rawSize addition.
			// Safety factor does not apply: no peak to cover.
			if segMemoryLoadingSize >= uint64(fakeBinlogMemSize) {
				segMemoryLoadingSize -= uint64(fakeBinlogMemSize)
			} else {
				segMemoryLoadingSize = 0
			}
		} else if factor := multiplyFactor.externalRawDataFactor; factor > 1.0 {
			// Non-lazy → add peak margin on top of rawSize that PART 2 added.
			segMemoryLoadingSize += uint64(float64(fakeBinlogMemSize) * (factor - 1.0))
		}
	}

	// PART 3: calculate size of stats data
	// stats data isn't managed by the caching layer, so its size should always be included,
	// regardless of the tiered eviction value
	for _, fieldBinlog := range loadInfo.Statslogs {
		segMemoryLoadingSize += uint64(getBinlogDataMemorySize(fieldBinlog))
	}

	// PART 4: calculate size of delete data
	// delete data isn't managed by the caching layer, so its size should always be included,
	// regardless of the tiered eviction value
	for _, fieldBinlog := range loadInfo.Deltalogs {
		// MemorySize of filedBinlog is the actual size in memory, but we should also consider
		// the memcpy from golang to cpp side, so the expansionFactor is set to 2.
		expansionFactor := float64(2)
		memSize := getBinlogDataMemorySize(fieldBinlog)

		// Note: If MemorySize == DiskSize, it means the segment comes from Milvus 2.3,
		//   MemorySize is actually compressed DiskSize of deltalog, so we'll fallback to use
		//   deltaExpansionFactor to compromise the compression ratio.
		if memSize == getBinlogDataDiskSize(fieldBinlog) {
			expansionFactor = multiplyFactor.deltaDataExpansionFactor
		}
		segMemoryLoadingSize += uint64(float64(memSize) * expansionFactor)
	}

	// PART 5: calculate size of json key stats data
	jsonStatsMmapEnable := paramtable.Get().QueryNodeCfg.MmapJSONStats.GetAsBool()
	for _, jsonKeyStats := range loadInfo.GetJsonKeyStatsLogs() {
		fieldSchema, err := schemaHelper.GetFieldFromID(jsonKeyStats.GetFieldID())
		if err != nil {
			return nil, err
		}
		jsonStatsSize := uint64(float64(jsonKeyStats.GetMemorySize()) * multiplyFactor.jsonKeyStatsExpansionFactor)
		if jsonStatsMmapEnable {
			if !multiplyFactor.TieredEvictionEnabled || !isScalarStatsEvictableEnable(fieldSchema) {
				segDiskLoadingSize += jsonStatsSize
			}
		} else {
			if !multiplyFactor.TieredEvictionEnabled || !isScalarStatsEvictableEnable(fieldSchema) {
				segMemoryLoadingSize += jsonStatsSize
			}
		}
	}

	// per struct memory size, used to keep mapping between row id and element id
	var structArrayOffsetsSize uint64
	// PART 6: calculate size of struct array offsets
	// The memory size is 4 * row_count + 4 * total_element_count
	// We cannot easily get the element count, so we estimate it by the row count * 10
	rowCount := uint64(loadInfo.GetNumOfRows())
	for range len(schema.GetStructArrayFields()) {
		structArrayOffsetsSize += 4*rowCount + 4*rowCount*10
	}

	// PART 7: calculate size of text index stats data
	// text index data is managed by the caching layer when tiered eviction is enabled,
	// so it only needs to be included when tiered eviction is disabled.
	// Text match index mmap is driven by scalar_field_enable_mmap (same as raw scalar data).
	// memory_size = sum of Tantivy index file sizes (same value as C++ ByteSize() after load),
	// so 1.0x is the baseline; textIndexExpansionFactor allows tuning if needed.
	textIndexMmapEnable := paramtable.Get().QueryNodeCfg.MmapScalarField.GetAsBool()
	for _, textStats := range loadInfo.GetTextStatsLogs() {
		fieldSchema, err := schemaHelper.GetFieldFromID(textStats.GetFieldID())
		if err != nil {
			return nil, err
		}
		textStatsSize := uint64(float64(textStats.GetMemorySize()) * multiplyFactor.textIndexExpansionFactor)
		if textIndexMmapEnable {
			if !multiplyFactor.TieredEvictionEnabled || !isScalarStatsEvictableEnable(fieldSchema) {
				segDiskLoadingSize += textStatsSize
			}
		} else {
			if !multiplyFactor.TieredEvictionEnabled || !isScalarStatsEvictableEnable(fieldSchema) {
				segMemoryLoadingSize += textStatsSize
			}
		}
	}

	return &ResourceUsage{
		MemorySize:         segMemoryLoadingSize + indexMemorySize + structArrayOffsetsSize,
		DiskSize:           segDiskLoadingSize,
		MmapFieldCount:     mmapFieldCount,
		FieldGpuMemorySize: fieldGpuMemorySize,
	}, nil
}

func DoubleMemoryDataType(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_String ||
		dataType == schemapb.DataType_VarChar ||
		dataType == schemapb.DataType_JSON
}

func DoubleMemorySystemField(fieldID int64) bool {
	return fieldID == common.TimeStampField
}

func SupportInterimIndexDataType(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_FloatVector ||
		dataType == schemapb.DataType_SparseFloatVector ||
		dataType == schemapb.DataType_Float16Vector ||
		dataType == schemapb.DataType_BFloat16Vector
}

func (loader *segmentLoader) getFieldType(collectionID, fieldID int64) (schemapb.DataType, error) {
	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		return 0, merr.WrapErrCollectionNotFound(collectionID)
	}

	for _, field := range collection.Schema().GetFields() {
		if field.GetFieldID() == fieldID {
			return field.GetDataType(), nil
		}
	}

	for _, structField := range collection.Schema().GetStructArrayFields() {
		if structField.GetFieldID() == fieldID {
			return schemapb.DataType_ArrayOfStruct, nil
		}
		for _, subField := range structField.GetFields() {
			if subField.GetFieldID() == fieldID {
				return subField.GetDataType(), nil
			}
		}
	}

	return 0, merr.WrapErrFieldNotFound(fieldID)
}

func (loader *segmentLoader) LoadIndex(ctx context.Context,
	seg Segment,
	loadInfo *querypb.SegmentLoadInfo,
	version int64,
) error {
	segment, ok := seg.(*LocalSegment)
	if !ok {
		return merr.WrapErrParameterInvalid("LocalSegment", fmt.Sprintf("%T", seg))
	}
	// Filter out LOADING segments only
	// use None to avoid loaded check
	infos := loader.prepare(ctx, commonpb.SegmentState_SegmentStateNone, loadInfo)
	defer loader.unregister(infos...)

	indexInfo := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *querypb.SegmentLoadInfo {
		info = typeutil.Clone(info)
		// remain binlog paths whose field id is in index infos to estimate resource usage correctly
		indexFields := typeutil.NewSet(lo.Map(info.GetIndexInfos(), func(indexInfo *querypb.FieldIndexInfo, _ int) int64 { return indexInfo.GetFieldID() })...)
		var binlogPaths []*datapb.FieldBinlog
		for _, binlog := range info.GetBinlogPaths() {
			if indexFields.Contain(binlog.GetFieldID()) {
				binlogPaths = append(binlogPaths, binlog)
			}
		}
		info.BinlogPaths = binlogPaths
		info.Deltalogs = nil
		info.Statslogs = nil
		return info
	})
	requestResourceResult, err := loader.requestResource(ctx, indexInfo...)
	if err != nil {
		return err
	}
	defer loader.freeRequestResource(requestResourceResult)

	mlog.Info(context.TODO(), "segment loader start to load index", mlog.Int("segmentNumAfterFilter", len(infos)))
	metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(paramtable.GetStringNodeID(), "LoadIndex").Inc()
	defer metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(paramtable.GetStringNodeID(), "LoadIndex").Dec()

	tr := timerecord.NewTimeRecorder("segmentLoader.LoadIndex")
	defer metrics.QueryNodeLoadIndexLatency.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(tr.ElapseSpan().Milliseconds()))
	for _, loadInfo := range infos {
		for _, info := range loadInfo.GetIndexInfos() {
			if len(info.GetIndexFilePaths()) == 0 {
				mlog.Warn(context.TODO(), "failed to add index for segment, index file list is empty, the segment may be too small")
				return merr.WrapErrIndexNotFound("index file list empty")
			}

			err := loader.loadFieldIndex(ctx, segment, info)
			if err != nil {
				mlog.Warn(context.TODO(), "failed to load index for segment", mlog.Err(err))
				return err
			}
		}
		loader.notifyLoadFinish(loadInfo)
	}

	return loader.waitSegmentLoadDone(ctx, commonpb.SegmentState_SegmentStateNone, []int64{loadInfo.GetSegmentID()}, version)
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
