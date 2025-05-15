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
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	UsedDiskMemoryRatio = 4
)

var errRetryTimerNotified = errors.New("retry timer notified")

type Loader interface {
	// Load loads binlogs, and spawn segments,
	// NOTE: make sure the ref count of the corresponding collection will never go down to 0 during this
	Load(ctx context.Context, collectionID int64, segmentType SegmentType, version int64, segments ...*querypb.SegmentLoadInfo) ([]Segment, error)

	// LoadDeltaLogs load deltalog and write delta data into provided segment.
	// it also executes resource protection logic in case of OOM.
	LoadDeltaLogs(ctx context.Context, segment Segment, deltaLogs []*datapb.FieldBinlog) error

	// LoadBloomFilterSet loads needed statslog for RemoteSegment.
	LoadBloomFilterSet(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error)

	// LoadBM25Stats loads BM25 statslog for RemoteSegment
	LoadBM25Stats(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) (*typeutil.ConcurrentMap[int64, map[int64]*storage.BM25Stats], error)

	// LoadIndex append index for segment and remove vector binlogs.
	LoadIndex(ctx context.Context,
		segment Segment,
		info *querypb.SegmentLoadInfo,
		version int64) error

	LoadLazySegment(ctx context.Context,
		segment Segment,
		loadInfo *querypb.SegmentLoadInfo,
	) error

	LoadJSONIndex(ctx context.Context,
		segment Segment,
		info *querypb.SegmentLoadInfo) error
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
		MaxMemoryCost:   uint64(float64(estimate.max_memory_cost) * util.GB),
		MaxDiskCost:     uint64(float64(estimate.max_disk_cost) * util.GB),
		FinalMemoryCost: uint64(float64(estimate.final_memory_cost) * util.GB),
		FinalDiskCost:   uint64(float64(estimate.final_disk_cost) * util.GB),
		HasRawData:      bool(estimate.has_raw_data),
	}
}

type requestResourceResult struct {
	Resource          LoadResource
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
	memoryUsageFactor        float64
	memoryIndexUsageFactor   float64
	enableTempSegmentIndex   bool
	tempSegmentIndexFactor   float64
	deltaDataExpansionFactor float64
}

func NewLoader(
	ctx context.Context,
	manager *Manager,
	cm storage.ChunkManager,
) *segmentLoader {
	cpuNum := hardware.GetCPUNum()
	ioPoolSize := cpuNum * 8
	// make sure small machines could load faster
	if ioPoolSize < 32 {
		ioPoolSize = 32
	}
	// limit the number of concurrency
	if ioPoolSize > 256 {
		ioPoolSize = 256
	}

	if configPoolSize := paramtable.Get().QueryNodeCfg.IoPoolSize.GetAsInt(); configPoolSize > 0 {
		ioPoolSize = configPoolSize
	}

	log.Info("SegmentLoader created", zap.Int("ioPoolSize", ioPoolSize))
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
	committedResourceNotifier *syncutil.VersionedNotifier

	duf *diskUsageFetcher
}

var _ Loader = (*segmentLoader)(nil)

func addBucketNameStorageV2(segmentInfo *querypb.SegmentLoadInfo) {
	if segmentInfo.GetStorageVersion() == 2 {
		bucketName := paramtable.Get().ServiceParam.MinioCfg.BucketName.GetValue()
		for _, fieldBinlog := range segmentInfo.GetBinlogPaths() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				binlog.LogPath = path.Join(bucketName, binlog.LogPath)
			}
		}
	}
}

func (loader *segmentLoader) Load(ctx context.Context,
	collectionID int64,
	segmentType SegmentType,
	version int64,
	segments ...*querypb.SegmentLoadInfo,
) ([]Segment, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.String("segmentType", segmentType.String()),
	)

	if len(segments) == 0 {
		log.Info("no segment to load")
		return nil, nil
	}
	for _, segmentInfo := range segments {
		addBucketNameStorageV2(segmentInfo)
	}

	coll := loader.manager.Collection.Get(collectionID)

	// Filter out loaded & loading segments
	infos := loader.prepare(ctx, segmentType, segments...)
	defer loader.unregister(infos...)

	log = log.With(
		zap.Int64s("requestSegments", lo.Map(segments, func(s *querypb.SegmentLoadInfo, _ int) int64 { return s.GetSegmentID() })),
		zap.Int64s("preparedSegments", lo.Map(infos, func(s *querypb.SegmentLoadInfo, _ int) int64 { return s.GetSegmentID() })),
	)

	// continue to wait other task done
	log.Info("start loading...", zap.Int("segmentNum", len(segments)), zap.Int("afterFilter", len(infos)))

	var err error
	var requestResourceResult requestResourceResult

	if !isLazyLoad(coll, segmentType) {
		// Check memory & storage limit
		// no need to check resource for lazy load here
		requestResourceResult, err = loader.requestResource(ctx, infos...)
		if err != nil {
			log.Warn("request resource failed", zap.Error(err))
			return nil, err
		}
		defer loader.freeRequest(requestResourceResult.Resource)
	}
	newSegments := typeutil.NewConcurrentMap[int64, Segment]()
	loaded := typeutil.NewConcurrentMap[int64, Segment]()
	defer func() {
		newSegments.Range(func(segmentID int64, s Segment) bool {
			log.Warn("release new segment created due to load failure",
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			s.Release(context.Background())
			return true
		})
		debug.FreeOSMemory()
	}()

	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(collectionID)
		log.Warn("failed to get collection", zap.Error(err))
		return nil, err
	}

	for _, info := range infos {
		loadInfo := info

		segment, err := NewSegment(
			ctx,
			collection,
			segmentType,
			version,
			loadInfo,
		)
		if err != nil {
			log.Warn("load segment failed when create new segment",
				zap.Int64("partitionID", loadInfo.GetPartitionID()),
				zap.Int64("segmentID", loadInfo.GetSegmentID()),
				zap.Error(err),
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

		logger := log.With(zap.Int64("partitionID", partitionID),
			zap.Int64("segmentID", segmentID),
			zap.String("segmentType", loadInfo.GetLevel().String()))
		metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), "LoadSegment").Inc()
		defer func() {
			metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), "LoadSegment").Dec()
			if err != nil {
				logger.Warn("load segment failed when load data into memory", zap.Error(err))
			}
			logger.Info("load segment done")
		}()
		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		logger.Info("load segment...")

		// L0 segment has no index or data to be load.
		if loadInfo.GetLevel() != datapb.SegmentLevel_L0 {
			s := segment.(*LocalSegment)
			// lazy load segment do not load segment at first time.
			if !s.IsLazyLoad() {
				if err = loader.LoadSegment(ctx, s, loadInfo); err != nil {
					return errors.Wrap(err, "At LoadSegment")
				}
			}
		}
		if err = loader.loadDeltalogs(ctx, segment, loadInfo.GetDeltalogs()); err != nil {
			return errors.Wrap(err, "At LoadDeltaLogs")
		}

		if err = segment.FinishLoad(); err != nil {
			return errors.Wrap(err, "At FinishLoad")
		}

		if segment.Level() != datapb.SegmentLevel_L0 {
			loader.manager.Segment.Put(ctx, segmentType, segment)
		}
		newSegments.GetAndRemove(segmentID)
		loaded.Insert(segmentID, segment)
		loader.notifyLoadFinish(loadInfo)

		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil
	}

	// Start to load,
	// Make sure we can always benefit from concurrency, and not spawn too many idle goroutines
	log.Info("start to load segments in parallel",
		zap.Int("segmentNum", len(infos)),
		zap.Int("concurrencyLevel", requestResourceResult.ConcurrencyLevel))

	err = funcutil.ProcessFuncParallel(len(infos),
		requestResourceResult.ConcurrencyLevel, loadSegmentFunc, "loadSegmentFunc")
	if err != nil {
		log.Warn("failed to load some segments", zap.Error(err))
		return nil, err
	}

	// Wait for all segments loaded
	segmentIDs := lo.Map(segments, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })
	if err := loader.waitSegmentLoadDone(ctx, segmentType, segmentIDs, version); err != nil {
		log.Warn("failed to wait the filtered out segments load done", zap.Error(err))
		return nil, err
	}

	log.Info("all segment load done")
	var result []Segment
	loaded.Range(func(_ int64, s Segment) bool {
		result = append(result, s)
		return true
	})
	return result, nil
}

func (loader *segmentLoader) prepare(ctx context.Context, segmentType SegmentType, segments ...*querypb.SegmentLoadInfo) []*querypb.SegmentLoadInfo {
	log := log.Ctx(ctx).With(
		zap.Stringer("segmentType", segmentType),
	)

	// filter out loaded & loading segments
	infos := make([]*querypb.SegmentLoadInfo, 0, len(segments))
	for _, segment := range segments {
		// Not loaded & loading & releasing.
		if !loader.manager.Segment.Exist(segment.GetSegmentID(), segmentType) &&
			!loader.loadingSegments.Contain(segment.GetSegmentID()) {
			infos = append(infos, segment)
			loader.loadingSegments.Insert(segment.GetSegmentID(), newLoadResult())
		} else {
			log.Info("skip loaded/loading segment",
				zap.Int64("segmentID", segment.GetSegmentID()),
				zap.Bool("isLoaded", len(loader.manager.Segment.GetBy(WithType(segmentType), WithID(segment.GetSegmentID()))) > 0),
				zap.Bool("isLoading", loader.loadingSegments.Contain(segment.GetSegmentID())),
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
	log := log.Ctx(ctx).With(
		zap.Int64s("segmentIDs", segmentIDs),
	)

	memoryUsage := hardware.GetUsedMemoryCount()
	totalMemory := hardware.GetMemoryCount()

	diskUsage, err := loader.duf.GetDiskUsage()
	if err != nil {
		return requestResourceResult{}, errors.Wrap(err, "get local used size failed")
	}
	diskCap := paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsUint64()

	loader.mut.Lock()
	defer loader.mut.Unlock()

	result := requestResourceResult{
		CommittedResource: loader.committedResource,
	}

	if loader.committedResource.MemorySize+memoryUsage >= totalMemory {
		return result, merr.WrapErrServiceMemoryLimitExceeded(float32(loader.committedResource.MemorySize+memoryUsage), float32(totalMemory))
	} else if loader.committedResource.DiskSize+uint64(diskUsage) >= diskCap {
		return result, merr.WrapErrServiceDiskLimitExceeded(float32(loader.committedResource.DiskSize+uint64(diskUsage)), float32(diskCap))
	}

	result.ConcurrencyLevel = funcutil.Min(hardware.GetCPUNum(), len(infos))
	mu, du, err := loader.checkSegmentSize(ctx, infos, memoryUsage, totalMemory, diskUsage)
	if err != nil {
		log.Warn("no sufficient resource to load segments", zap.Error(err))
		return result, err
	}

	result.Resource.MemorySize += mu
	result.Resource.DiskSize += du

	toMB := func(mem uint64) float64 {
		return float64(mem) / 1024 / 1024
	}
	loader.committedResource.Add(result.Resource)
	log.Info("request resource for loading segments (unit in MiB)",
		zap.Float64("memory", toMB(result.Resource.MemorySize)),
		zap.Float64("committedMemory", toMB(loader.committedResource.MemorySize)),
		zap.Float64("disk", toMB(result.Resource.DiskSize)),
		zap.Float64("committedDisk", toMB(loader.committedResource.DiskSize)),
	)

	return result, nil
}

// freeRequest returns request memory & storage usage request.
func (loader *segmentLoader) freeRequest(resource LoadResource) {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	loader.committedResource.Sub(resource)
	loader.committedResourceNotifier.NotifyAll()
}

func (loader *segmentLoader) waitSegmentLoadDone(ctx context.Context, segmentType SegmentType, segmentIDs []int64, version int64) error {
	log := log.Ctx(ctx).With(
		zap.String("segmentType", segmentType.String()),
		zap.Int64s("segmentIDs", segmentIDs),
	)
	for _, segmentID := range segmentIDs {
		if loader.manager.Segment.GetWithType(segmentID, segmentType) != nil {
			continue
		}

		result, ok := loader.loadingSegments.Get(segmentID)
		if !ok {
			log.Warn("segment was removed from the loading map early", zap.Int64("segmentID", segmentID))
			return errors.New("segment was removed from the loading map early")
		}

		log.Info("wait segment loaded...", zap.Int64("segmentID", segmentID))

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
			log.Warn("failed to wait segment loaded due to context done", zap.Int64("segmentID", segmentID))
			return ctx.Err()
		}

		if result.status.Load() == failure {
			log.Warn("failed to wait segment loaded", zap.Int64("segmentID", segmentID))
			return merr.WrapErrSegmentLack(segmentID, "failed to wait segment loaded")
		}

		// try to update segment version after wait segment loaded
		loader.manager.Segment.UpdateBy(IncreaseVersion(version), WithType(segmentType), WithID(segmentID))

		log.Info("segment loaded...", zap.Int64("segmentID", segmentID))
	}
	return nil
}

func (loader *segmentLoader) LoadBM25Stats(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) (*typeutil.ConcurrentMap[int64, map[int64]*storage.BM25Stats], error) {
	segmentNum := len(infos)
	if segmentNum == 0 {
		return nil, nil
	}

	segments := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
		return info.GetSegmentID()
	})
	log.Info("start loading bm25 stats for remote...", zap.Int64("collectionID", collectionID), zap.Int64s("segmentIDs", segments), zap.Int("segmentNum", segmentNum))

	loadedStats := typeutil.NewConcurrentMap[int64, map[int64]*storage.BM25Stats]()
	loadRemoteBM25Func := func(idx int) error {
		loadInfo := infos[idx]
		segmentID := loadInfo.SegmentID
		stats := make(map[int64]*storage.BM25Stats)

		log.Info("loading bm25 stats for remote...", zap.Int64("collectionID", collectionID), zap.Int64("segment", segmentID))
		logpaths := loader.filterBM25Stats(loadInfo.Bm25Logs)
		err := loader.loadBm25Stats(ctx, segmentID, stats, logpaths)
		if err != nil {
			log.Warn("load remote segment bm25 stats failed",
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return err
		}
		loadedStats.Insert(segmentID, stats)
		return nil
	}

	err := funcutil.ProcessFuncParallel(segmentNum, segmentNum, loadRemoteBM25Func, "loadRemoteBM25Func")
	if err != nil {
		// no partial success here
		log.Warn("failed to load bm25 stats for remote segment", zap.Int64("collectionID", collectionID), zap.Int64s("segmentIDs", segments), zap.Error(err))
		return nil, err
	}

	return loadedStats, nil
}

func (loader *segmentLoader) LoadBloomFilterSet(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64s("segmentIDs", lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
			return info.GetSegmentID()
		})),
	)

	segmentNum := len(infos)
	if segmentNum == 0 {
		log.Info("no segment to load")
		return nil, nil
	}

	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(collectionID)
		log.Warn("failed to get collection while loading segment", zap.Error(err))
		return nil, err
	}
	pkField := GetPkField(collection.Schema())

	log.Info("start loading remote...", zap.Int("segmentNum", segmentNum))

	loadedBfs := typeutil.NewConcurrentSet[*pkoracle.BloomFilterSet]()
	// TODO check memory for bf size
	loadRemoteFunc := func(idx int) error {
		loadInfo := infos[idx]
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		bfs := pkoracle.NewBloomFilterSet(segmentID, partitionID, commonpb.SegmentState_Sealed)

		log.Info("loading bloom filter for remote...")
		pkStatsBinlogs, logType := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segmentID, bfs, pkStatsBinlogs, logType)
		if err != nil {
			log.Warn("load remote segment bloom filter failed",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return err
		}
		loadedBfs.Insert(bfs)

		return nil
	}

	err := funcutil.ProcessFuncParallel(segmentNum, segmentNum, loadRemoteFunc, "loadRemoteFunc")
	if err != nil {
		// no partial success here
		log.Warn("failed to load remote segment", zap.Error(err))
		return nil, err
	}

	return loadedBfs.Collect(), nil
}

func separateIndexAndBinlog(loadInfo *querypb.SegmentLoadInfo) (map[int64]*IndexedFieldInfo, []*datapb.FieldBinlog) {
	fieldID2IndexInfo := make(map[int64][]*querypb.FieldIndexInfo)
	for _, indexInfo := range loadInfo.IndexInfos {
		if len(indexInfo.GetIndexFilePaths()) > 0 {
			fieldID := indexInfo.FieldID
			fieldID2IndexInfo[fieldID] = append(fieldID2IndexInfo[fieldID], indexInfo)
		}
	}

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
		} else {
			fieldBinlogs = append(fieldBinlogs, fieldBinlog)
		}
	}

	return indexedFieldInfos, fieldBinlogs
}

func separateLoadInfoV2(loadInfo *querypb.SegmentLoadInfo, schema *schemapb.CollectionSchema) (
	map[int64]*IndexedFieldInfo, // indexed info
	[]*datapb.FieldBinlog, // fields info
	map[int64]*datapb.TextIndexStats, // text indexed info
	map[int64]struct{}, // unindexed text fields
	map[int64]*datapb.JsonKeyStats, // json key stats info
) {
	fieldID2IndexInfo := make(map[int64][]*querypb.FieldIndexInfo)
	for _, indexInfo := range loadInfo.IndexInfos {
		if len(indexInfo.GetIndexFilePaths()) > 0 {
			fieldID := indexInfo.FieldID
			fieldID2IndexInfo[fieldID] = append(fieldID2IndexInfo[fieldID], indexInfo)
		}
	}

	indexedFieldInfos := make(map[int64]*IndexedFieldInfo)
	fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(loadInfo.BinlogPaths))

	for _, fieldBinlog := range loadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		// check num rows of data meta and index meta are consistent
		if infos, ok := fieldID2IndexInfo[fieldID]; ok {
			for _, indexInfo := range infos {
				fieldInfo := &IndexedFieldInfo{
					FieldBinlog: fieldBinlog,
					IndexInfo:   indexInfo,
				}
				indexedFieldInfos[indexInfo.IndexID] = fieldInfo
			}
		} else {
			fieldBinlogs = append(fieldBinlogs, fieldBinlog)
		}
	}

	textIndexedInfo := make(map[int64]*datapb.TextIndexStats, len(loadInfo.GetTextStatsLogs()))
	for _, fieldStatsLog := range loadInfo.GetTextStatsLogs() {
		textLog, ok := textIndexedInfo[fieldStatsLog.FieldID]
		if !ok {
			textIndexedInfo[fieldStatsLog.FieldID] = fieldStatsLog
		} else if fieldStatsLog.GetVersion() > textLog.GetVersion() {
			textIndexedInfo[fieldStatsLog.FieldID] = fieldStatsLog
		}
	}

	jsonKeyIndexInfo := make(map[int64]*datapb.JsonKeyStats, len(loadInfo.GetJsonKeyStatsLogs()))
	for _, fieldStatsLog := range loadInfo.GetJsonKeyStatsLogs() {
		jsonKeyLog, ok := jsonKeyIndexInfo[fieldStatsLog.FieldID]
		if !ok {
			jsonKeyIndexInfo[fieldStatsLog.FieldID] = fieldStatsLog
		} else if fieldStatsLog.GetVersion() > jsonKeyLog.GetVersion() {
			jsonKeyIndexInfo[fieldStatsLog.FieldID] = fieldStatsLog
		}
	}

	unindexedTextFields := make(map[int64]struct{})
	for _, field := range schema.GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		_, textIndexExist := textIndexedInfo[field.GetFieldID()]
		if h.EnableMatch() && !textIndexExist {
			unindexedTextFields[field.GetFieldID()] = struct{}{}
		}
	}

	return indexedFieldInfos, fieldBinlogs, textIndexedInfo, unindexedTextFields, jsonKeyIndexInfo
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
	schemaHelper, _ := typeutil.CreateSchemaHelper(collection.Schema())
	indexedFieldInfos, fieldBinlogs, textIndexes, unindexedTextFields, jsonKeyStats := separateLoadInfoV2(loadInfo, collection.Schema())
	if err := segment.AddFieldDataInfo(ctx, loadInfo.GetNumOfRows(), loadInfo.GetBinlogPaths()); err != nil {
		return err
	}

	log := log.Ctx(ctx).With(zap.Int64("segmentID", segment.ID()))
	tr := timerecord.NewTimeRecorder("segmentLoader.loadSealedSegment")
	log.Info("Start loading fields...",
		// zap.Int64s("indexedFields", lo.Keys(indexedFieldInfos)),
		zap.Int64s("indexed text fields", lo.Keys(textIndexes)),
		zap.Int64s("unindexed text fields", lo.Keys(unindexedTextFields)),
		zap.Int64s("indexed json key fields", lo.Keys(jsonKeyStats)),
	)
	if err := loader.loadFieldsIndex(ctx, schemaHelper, segment, loadInfo.GetNumOfRows(), indexedFieldInfos); err != nil {
		return err
	}
	loadFieldsIndexSpan := tr.RecordSpan()
	metrics.QueryNodeLoadIndexLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(loadFieldsIndexSpan.Milliseconds()))

	// 2. complement raw data for the scalar fields without raw data
	for _, info := range indexedFieldInfos {
		fieldID := info.IndexInfo.FieldID
		field, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			return err
		}
		if !segment.HasRawData(fieldID) || field.GetIsPrimaryKey() {
			log.Info("field index doesn't include raw data, load binlog...",
				zap.Int64("fieldID", fieldID),
				zap.String("index", info.IndexInfo.GetIndexName()),
			)
			// for scalar index's raw data, only load to mmap not memory
			if err = segment.LoadFieldData(ctx, fieldID, loadInfo.GetNumOfRows(), info.FieldBinlog); err != nil {
				log.Warn("load raw data failed", zap.Int64("fieldID", fieldID), zap.Error(err))
				return err
			}
		}
	}
	complementScalarDataSpan := tr.RecordSpan()
	if err := loadSealedSegmentFields(ctx, collection, segment, fieldBinlogs, loadInfo.GetNumOfRows()); err != nil {
		return err
	}
	loadRawDataSpan := tr.RecordSpan()

	// load text indexes.
	for _, info := range textIndexes {
		if err := segment.LoadTextIndex(ctx, info, schemaHelper); err != nil {
			return err
		}
	}
	loadTextIndexesSpan := tr.RecordSpan()

	// create index for unindexed text fields.
	for fieldID := range unindexedTextFields {
		if err := segment.CreateTextIndex(ctx, fieldID); err != nil {
			return err
		}
	}

	for _, info := range jsonKeyStats {
		if err := segment.LoadJSONKeyIndex(ctx, info, schemaHelper); err != nil {
			return err
		}
	}
	loadJSONKeyIndexesSpan := tr.RecordSpan()

	// 4. rectify entries number for binlog in very rare cases
	// https://github.com/milvus-io/milvus/23654
	// legacy entry num = 0
	if err := loader.patchEntryNumber(ctx, segment, loadInfo); err != nil {
		return err
	}
	patchEntryNumberSpan := tr.RecordSpan()
	log.Info("Finish loading segment",
		zap.Duration("loadFieldsIndexSpan", loadFieldsIndexSpan),
		zap.Duration("complementScalarDataSpan", complementScalarDataSpan),
		zap.Duration("loadRawDataSpan", loadRawDataSpan),
		zap.Duration("patchEntryNumberSpan", patchEntryNumberSpan),
		zap.Duration("loadTextIndexesSpan", loadTextIndexesSpan),
		zap.Duration("loadJsonKeyIndexSpan", loadJSONKeyIndexesSpan),
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
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.Collection()),
		zap.Int64("partitionID", segment.Partition()),
		zap.String("shard", segment.Shard().VirtualName()),
		zap.Int64("segmentID", segment.ID()),
	)

	log.Info("start loading segment files",
		zap.Int64("rowNum", loadInfo.GetNumOfRows()),
		zap.String("segmentType", segment.Type().String()))

	collection := loader.manager.Collection.Get(segment.Collection())
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(segment.Collection())
		log.Warn("failed to get collection while loading segment", zap.Error(err))
		return err
	}
	pkField := GetPkField(collection.Schema())

	// TODO(xige-16): Optimize the data loading process and reduce data copying
	// for now, there will be multiple copies in the process of data loading into segCore
	defer debug.FreeOSMemory()

	if segment.Type() == SegmentTypeSealed {
		if err := loader.loadSealedSegment(ctx, loadInfo, segment); err != nil {
			return err
		}
	} else {
		if err := segment.LoadMultiFieldData(ctx); err != nil {
			return err
		}
	}

	// load statslog if it's growing segment
	if segment.segmentType == SegmentTypeGrowing {
		log.Info("loading statslog...")
		pkStatsBinlogs, logType := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segment.ID(), segment.bloomFilterSet, pkStatsBinlogs, logType)
		if err != nil {
			return err
		}

		if len(loadInfo.Bm25Logs) > 0 {
			log.Info("loading bm25 stats...")
			bm25StatsLogs := loader.filterBM25Stats(loadInfo.Bm25Logs)

			err = loader.loadBm25Stats(ctx, segment.ID(), segment.bm25Stats, bm25StatsLogs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (loader *segmentLoader) LoadLazySegment(ctx context.Context,
	segment Segment,
	loadInfo *querypb.SegmentLoadInfo,
) (err error) {
	resource, err := loader.requestResourceWithTimeout(ctx, loadInfo)
	if err != nil {
		log.Ctx(ctx).Warn("request resource failed", zap.Error(err))
		return err
	}
	defer loader.freeRequest(resource)

	return loader.LoadSegment(ctx, segment, loadInfo)
}

// requestResourceWithTimeout requests memory & storage to load segments with a timeout and retry.
func (loader *segmentLoader) requestResourceWithTimeout(ctx context.Context, infos ...*querypb.SegmentLoadInfo) (LoadResource, error) {
	retryInterval := paramtable.Get().QueryNodeCfg.LazyLoadRequestResourceRetryInterval.GetAsDuration(time.Millisecond)
	timeoutStarted := false
	for {
		listener := loader.committedResourceNotifier.Listen(syncutil.VersionedListenAtLatest)

		result, err := loader.requestResource(ctx, infos...)
		if err == nil {
			return result.Resource, nil
		}

		// start timeout if there's no committed resource in loading.
		if !timeoutStarted && result.CommittedResource.IsZero() {
			timeout := paramtable.Get().QueryNodeCfg.LazyLoadRequestResourceTimeout.GetAsDuration(time.Millisecond)
			var cancel context.CancelFunc
			// TODO: use context.WithTimeoutCause instead of contextutil.WithTimeoutCause in go1.21
			ctx, cancel = contextutil.WithTimeoutCause(ctx, timeout, merr.ErrServiceResourceInsufficient)
			defer cancel()
			timeoutStarted = true
		}

		// TODO: use context.WithTimeoutCause instead of contextutil.WithTimeoutCause in go1.21
		ctxWithRetryTimeout, cancelWithRetryTimeout := contextutil.WithTimeoutCause(ctx, retryInterval, errRetryTimerNotified)
		err = listener.Wait(ctxWithRetryTimeout)
		// if error is not caused by retry timeout, return it directly.
		if err != nil && !errors.Is(err, errRetryTimerNotified) {
			cancelWithRetryTimeout()
			return LoadResource{}, err
		}
		cancelWithRetryTimeout()
	}
}

func (loader *segmentLoader) filterPKStatsBinlogs(fieldBinlogs []*datapb.FieldBinlog, pkFieldID int64) ([]string, storage.StatsLogType) {
	result := make([]string, 0)
	for _, fieldBinlog := range fieldBinlogs {
		if fieldBinlog.FieldID == pkFieldID {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				_, logidx := path.Split(binlog.GetLogPath())
				// if special status log exist
				// only load one file
				switch logidx {
				case storage.CompoundStatsType.LogIdx():
					return []string{binlog.GetLogPath()}, storage.CompoundStatsType
				default:
					result = append(result, binlog.GetLogPath())
				}
			}
		}
	}
	return result, storage.DefaultStatsType
}

func (loader *segmentLoader) filterBM25Stats(fieldBinlogs []*datapb.FieldBinlog) map[int64][]string {
	result := make(map[int64][]string, 0)
	for _, fieldBinlog := range fieldBinlogs {
		logpaths := []string{}
		for _, binlog := range fieldBinlog.GetBinlogs() {
			_, logidx := path.Split(binlog.GetLogPath())
			// if special status log exist
			// only load one file
			if logidx == storage.CompoundStatsType.LogIdx() {
				logpaths = []string{binlog.GetLogPath()}
				break
			} else {
				logpaths = append(logpaths, binlog.GetLogPath())
			}
		}
		result[fieldBinlog.FieldID] = logpaths
	}
	return result
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

	log.Ctx(ctx).Info("load field binlogs done for sealed segment",
		zap.Int64("collection", segment.Collection()),
		zap.Int64("segment", segment.ID()),
		zap.Int("len(field)", len(fields)),
		zap.String("segmentType", segment.Type().String()))

	return nil
}

func (loader *segmentLoader) loadFieldsIndex(ctx context.Context,
	schemaHelper *typeutil.SchemaHelper,
	segment *LocalSegment,
	numRows int64,
	indexedFieldInfos map[int64]*IndexedFieldInfo,
) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.Collection()),
		zap.Int64("partitionID", segment.Partition()),
		zap.Int64("segmentID", segment.ID()),
		zap.Int64("rowCount", numRows),
	)

	for _, fieldInfo := range indexedFieldInfos {
		fieldID := fieldInfo.IndexInfo.FieldID
		indexInfo := fieldInfo.IndexInfo
		tr := timerecord.NewTimeRecorder("loadFieldIndex")
		err := loader.loadFieldIndex(ctx, segment, indexInfo)
		loadFieldIndexSpan := tr.RecordSpan()
		if err != nil {
			return err
		}

		log.Info("load field binlogs done for sealed segment with index",
			zap.Int64("fieldID", fieldID),
			zap.Any("binlog", fieldInfo.FieldBinlog.Binlogs),
			zap.Int32("current_index_version", fieldInfo.IndexInfo.GetCurrentIndexVersion()),
			zap.Duration("load_duration", loadFieldIndexSpan),
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

func (loader *segmentLoader) loadBm25Stats(ctx context.Context, segmentID int64, stats map[int64]*storage.BM25Stats, binlogPaths map[int64][]string) error {
	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", segmentID),
	)
	if len(binlogPaths) == 0 {
		log.Info("there are no bm25 stats logs saved with segment")
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
		log.Info("Successfully load bm25 stats", zap.Duration("time", time.Since(startTs)), zap.Int64("numRow", newStats.NumRow()), zap.Int64("fieldID", fieldID))
	}

	return nil
}

func (loader *segmentLoader) loadBloomFilter(ctx context.Context, segmentID int64, bfs *pkoracle.BloomFilterSet,
	binlogPaths []string, logType storage.StatsLogType,
) error {
	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", segmentID),
	)
	if len(binlogPaths) == 0 {
		log.Info("there are no stats logs saved with segment")
		return nil
	}

	startTs := time.Now()
	values, err := loader.cm.MultiRead(ctx, binlogPaths)
	if err != nil {
		return err
	}
	blobs := []*storage.Blob{}
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: values[i]})
	}

	var stats []*storage.PrimaryKeyStats
	if logType == storage.CompoundStatsType {
		stats, err = storage.DeserializeStatsList(blobs[0])
		if err != nil {
			log.Warn("failed to deserialize stats list", zap.Error(err))
			return err
		}
	} else {
		stats, err = storage.DeserializeStats(blobs)
		if err != nil {
			log.Warn("failed to deserialize stats", zap.Error(err))
			return err
		}
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
	log.Info("Successfully load pk stats", zap.Duration("time", time.Since(startTs)), zap.Uint("size", size))
	return nil
}

// loadDeltalogs performs the internal actions of `LoadDeltaLogs`
// this function does not perform resource check and is meant be used among other load APIs.
func (loader *segmentLoader) loadDeltalogs(ctx context.Context, segment Segment, deltaLogs []*datapb.FieldBinlog) error {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadDeltalogs-%d", segment.ID()))
	defer sp.End()
	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", segment.ID()),
		zap.Int("deltaNum", len(deltaLogs)),
	)
	log.Info("loading delta...")

	var blobs []*storage.Blob
	var futures []*conc.Future[any]
	for _, deltaLog := range deltaLogs {
		for _, bLog := range deltaLog.GetBinlogs() {
			bLog := bLog
			// the segment has applied the delta logs, skip it
			if bLog.GetTimestampTo() > 0 && // this field may be missed in legacy versions
				bLog.GetTimestampTo() < segment.LastDeltaTimestamp() {
				continue
			}
			future := GetLoadPool().Submit(func() (any, error) {
				value, err := loader.cm.Read(ctx, bLog.GetLogPath())
				if err != nil {
					return nil, err
				}
				blob := &storage.Blob{
					Key:    bLog.GetLogPath(),
					Value:  value,
					RowNum: bLog.EntriesNum,
				}
				return blob, nil
			})
			futures = append(futures, future)
		}
	}
	for _, future := range futures {
		blob, err := future.Await()
		if err != nil {
			return err
		}
		blobs = append(blobs, blob.(*storage.Blob))
	}
	if len(blobs) == 0 {
		log.Info("there are no delta logs saved with segment, skip loading delete record")
		return nil
	}

	rowNums := lo.SumBy(blobs, func(blob *storage.Blob) int64 {
		return blob.RowNum
	})

	collection := loader.manager.Collection.Get(segment.Collection())

	helper, _ := typeutil.CreateSchemaHelper(collection.Schema())
	pkField, _ := helper.GetPrimaryKeyField()
	deltaData, err := storage.NewDeltaDataWithPkType(rowNums, pkField.DataType)
	if err != nil {
		return err
	}

	reader, err := storage.CreateDeltalogReader(blobs)
	if err != nil {
		return err
	}
	defer reader.Close()
	for {
		dl, err := reader.NextValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = deltaData.Append((*dl).Pk, (*dl).Ts)
		if err != nil {
			return err
		}
	}

	err = segment.LoadDeltaData(ctx, deltaData)
	if err != nil {
		return err
	}

	log.Info("load delta logs done", zap.Int64("deleteCount", deltaData.DeleteRowCount()))
	return nil
}

// LoadDeltaLogs load deltalog and write delta data into provided segment.
// it also executes resource protection logic in case of OOM.
func (loader *segmentLoader) LoadDeltaLogs(ctx context.Context, segment Segment, deltaLogs []*datapb.FieldBinlog) error {
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    segment.ID(),
		CollectionID: segment.Collection(),
		Deltalogs:    deltaLogs,
	}
	// Check memory & storage limit
	requestResourceResult, err := loader.requestResource(ctx, loadInfo)
	if err != nil {
		log.Warn("request resource failed", zap.Error(err))
		return err
	}
	defer loader.freeRequest(requestResourceResult.Resource)
	return loader.loadDeltalogs(ctx, segment, deltaLogs)
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

	log.Warn("legacy segment binlog found, start to patch entry num", zap.Int64("segmentID", segment.ID()))
	rowIDField := lo.FindOrElse(loadInfo.BinlogPaths, nil, func(binlog *datapb.FieldBinlog) bool {
		return binlog.GetFieldID() == common.RowIDField
	})

	if rowIDField == nil {
		return errors.New("rowID field binlog not found")
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
			err = errors.New("rowID & index binlog number not matched")
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

// checkSegmentSize checks whether the memory & disk is sufficient to load the segments
// returns the memory & disk usage while loading if possible to load,
// otherwise, returns error
func (loader *segmentLoader) checkSegmentSize(ctx context.Context, segmentLoadInfos []*querypb.SegmentLoadInfo, memUsage, totalMem uint64, localDiskUsage int64) (uint64, uint64, error) {
	if len(segmentLoadInfos) == 0 {
		return 0, 0, nil
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segmentLoadInfos[0].GetCollectionID()),
	)

	toMB := func(mem uint64) float64 {
		return float64(mem) / 1024 / 1024
	}

	memUsage = memUsage + loader.committedResource.MemorySize
	if memUsage == 0 || totalMem == 0 {
		return 0, 0, errors.New("get memory failed when checkSegmentSize")
	}

	diskUsage := uint64(localDiskUsage) + loader.committedResource.DiskSize

	factor := resourceEstimateFactor{
		memoryUsageFactor:        paramtable.Get().QueryNodeCfg.LoadMemoryUsageFactor.GetAsFloat(),
		memoryIndexUsageFactor:   paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(),
		enableTempSegmentIndex:   paramtable.Get().QueryNodeCfg.EnableTempSegmentIndex.GetAsBool(),
		tempSegmentIndexFactor:   paramtable.Get().QueryNodeCfg.InterimIndexMemExpandRate.GetAsFloat(),
		deltaDataExpansionFactor: paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
	}
	maxSegmentSize := uint64(0)
	predictMemUsage := memUsage
	predictDiskUsage := diskUsage
	var predictGpuMemUsage []uint64
	mmapFieldCount := 0
	for _, loadInfo := range segmentLoadInfos {
		collection := loader.manager.Collection.Get(loadInfo.GetCollectionID())
		usage, err := getResourceUsageEstimateOfSegment(collection.Schema(), loadInfo, factor)
		if err != nil {
			log.Warn(
				"failed to estimate resource usage of segment",
				zap.Int64("collectionID", loadInfo.GetCollectionID()),
				zap.Int64("segmentID", loadInfo.GetSegmentID()),
				zap.Error(err))
			return 0, 0, err
		}

		log.Debug("segment resource for loading",
			zap.Int64("segmentID", loadInfo.GetSegmentID()),
			zap.Float64("memoryUsage(MB)", toMB(usage.MemorySize)),
			zap.Float64("diskUsage(MB)", toMB(usage.DiskSize)),
			zap.Float64("memoryLoadFactor", factor.memoryUsageFactor),
		)
		mmapFieldCount += usage.MmapFieldCount
		predictDiskUsage += usage.DiskSize
		predictMemUsage += usage.MemorySize
		predictGpuMemUsage = usage.FieldGpuMemorySize
		if usage.MemorySize > maxSegmentSize {
			maxSegmentSize = usage.MemorySize
		}
	}

	log.Info("predict memory and disk usage while loading (in MiB)",
		zap.Float64("maxSegmentSize(MB)", toMB(maxSegmentSize)),
		zap.Float64("committedMemSize(MB)", toMB(loader.committedResource.MemorySize)),
		zap.Float64("memLimit(MB)", toMB(totalMem)),
		zap.Float64("memUsage(MB)", toMB(memUsage)),
		zap.Float64("committedDiskSize(MB)", toMB(loader.committedResource.DiskSize)),
		zap.Float64("diskUsage(MB)", toMB(diskUsage)),
		zap.Float64("predictMemUsage(MB)", toMB(predictMemUsage)),
		zap.Float64("predictDiskUsage(MB)", toMB(predictDiskUsage)),
		zap.Int("mmapFieldCount", mmapFieldCount),
	)

	if predictMemUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
		return 0, 0, fmt.Errorf("load segment failed, OOM if load, maxSegmentSize = %v MB,  memUsage = %v MB, predictMemUsage = %v MB, totalMem = %v MB thresholdFactor = %f",
			toMB(maxSegmentSize),
			toMB(memUsage),
			toMB(predictMemUsage),
			toMB(totalMem),
			paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
	}

	if predictDiskUsage > uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())*paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()) {
		return 0, 0, merr.WrapErrServiceDiskLimitExceeded(float32(predictDiskUsage), float32(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64()), fmt.Sprintf("load segment failed, disk space is not enough, diskUsage = %v MB, predictDiskUsage = %v MB, totalDisk = %v MB, thresholdFactor = %f",
			toMB(diskUsage),
			toMB(predictDiskUsage),
			toMB(uint64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())),
			paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()))
	}

	err := checkSegmentGpuMemSize(predictGpuMemUsage, float32(paramtable.Get().GpuConfig.OverloadedMemoryThresholdPercentage.GetAsFloat()))
	if err != nil {
		return 0, 0, err
	}
	return predictMemUsage - memUsage, predictDiskUsage - diskUsage, nil
}

// getResourceUsageEstimateOfSegment estimates the resource usage of the segment
func getResourceUsageEstimateOfSegment(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, multiplyFactor resourceEstimateFactor) (usage *ResourceUsage, err error) {
	var segmentMemorySize, segmentDiskSize uint64
	var indexMemorySize uint64
	var mmapFieldCount int
	var fieldGpuMemorySize []uint64

	fieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
	for _, fieldIndexInfo := range loadInfo.IndexInfos {
		fieldID := fieldIndexInfo.FieldID
		fieldID2IndexInfo[fieldID] = fieldIndexInfo
	}

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		log.Warn("failed to create schema helper", zap.String("name", schema.GetName()), zap.Error(err))
		return nil, err
	}
	calculateDataSizeCount := 0
	ctx := context.Background()

	for _, fieldBinlog := range loadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		var mmapEnabled bool
		// TODO retrieve_enable should be considered
		fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			log.Warn("failed to get field schema", zap.Int64("fieldID", fieldID), zap.String("name", schema.GetName()), zap.Error(err))
			return nil, err
		}
		binlogSize := uint64(getBinlogDataMemorySize(fieldBinlog))
		isVectorType := typeutil.IsVectorType(fieldSchema.DataType)
		shouldCalculateDataSize := false

		if fieldIndexInfo, ok := fieldID2IndexInfo[fieldID]; ok && len(fieldIndexInfo.GetIndexFilePaths()) > 0 {
			var estimateResult ResourceEstimate
			err := GetCLoadInfoWithFunc(ctx, fieldSchema, loadInfo, fieldIndexInfo, func(c *LoadIndexInfo) error {
				GetDynamicPool().Submit(func() (any, error) {
					loadResourceRequest := C.EstimateLoadIndexResource(c.cLoadIndexInfo)
					estimateResult = GetResourceEstimate(&loadResourceRequest)
					return nil, nil
				}).Await()
				return nil
			})
			if err != nil {
				return nil, errors.Wrapf(err, "failed to estimate resource usage of index, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}
			indexMemorySize += estimateResult.MaxMemoryCost
			segmentDiskSize += estimateResult.MaxDiskCost
			if vecindexmgr.GetVecIndexMgrInstance().IsGPUVecIndex(common.GetIndexType(fieldIndexInfo.IndexParams)) {
				fieldGpuMemorySize = append(fieldGpuMemorySize, estimateResult.MaxMemoryCost)
			}
			if !estimateResult.HasRawData && !isVectorType {
				shouldCalculateDataSize = true
			}

			if !estimateResult.HasRawData && isVectorType {
				metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, fieldIndexInfo.IndexParams)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to estimate resource usage of index, metric type nout found, collection %d, segment %d, indexBuildID %d",
						loadInfo.GetCollectionID(),
						loadInfo.GetSegmentID(),
						fieldIndexInfo.GetBuildID())
				}
				if metricType != metric.BM25 {
					mmapVectorField := paramtable.Get().QueryNodeCfg.MmapVectorField.GetAsBool()
					if mmapVectorField {
						segmentDiskSize += binlogSize
					} else {
						segmentMemorySize += binlogSize
					}
				}
			}
		} else {
			shouldCalculateDataSize = true
			// querynode will generate a (memory type) intermin index for vector type
			interimIndexEnable := multiplyFactor.enableTempSegmentIndex && !isGrowingMmapEnable() && SupportInterimIndexDataType(fieldSchema.GetDataType())
			if interimIndexEnable {
				segmentMemorySize += uint64(float64(binlogSize) * multiplyFactor.tempSegmentIndexFactor)
			}
		}

		if shouldCalculateDataSize {
			calculateDataSizeCount += 1
			mmapEnabled = isDataMmapEnable(fieldSchema)

			if !mmapEnabled || common.IsSystemField(fieldSchema.GetFieldID()) {
				segmentMemorySize += binlogSize
				if DoubleMemorySystemField(fieldSchema.GetFieldID()) || DoubleMemoryDataType(fieldSchema.GetDataType()) {
					segmentMemorySize += binlogSize
				}
			} else {
				segmentDiskSize += uint64(getBinlogDataDiskSize(fieldBinlog))
			}
		}

		if mmapEnabled {
			mmapFieldCount++
		}
	}

	// get size of stats data
	for _, fieldBinlog := range loadInfo.Statslogs {
		segmentMemorySize += uint64(getBinlogDataMemorySize(fieldBinlog))
	}

	// get size of delete data
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
		segmentMemorySize += uint64(float64(memSize) * expansionFactor)
	}
	return &ResourceUsage{
		MemorySize:         segmentMemorySize + indexMemorySize,
		DiskSize:           segmentDiskSize,
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
		dataType == schemapb.DataType_SparseFloatVector
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
	log := log.Ctx(ctx).With(
		zap.Int64("collection", segment.Collection()),
		zap.Int64("segment", segment.ID()),
	)

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
	defer loader.freeRequest(requestResourceResult.Resource)

	log.Info("segment loader start to load index", zap.Int("segmentNumAfterFilter", len(infos)))
	metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), "LoadIndex").Inc()
	defer metrics.QueryNodeLoadSegmentConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), "LoadIndex").Dec()

	tr := timerecord.NewTimeRecorder("segmentLoader.LoadIndex")
	defer metrics.QueryNodeLoadIndexLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))
	for _, loadInfo := range infos {
		fieldIDs := typeutil.NewSet(lo.Map(loadInfo.GetIndexInfos(), func(info *querypb.FieldIndexInfo, _ int) int64 { return info.GetFieldID() })...)
		fieldInfos := lo.SliceToMap(lo.Filter(loadInfo.GetBinlogPaths(), func(info *datapb.FieldBinlog, _ int) bool { return fieldIDs.Contain(info.GetFieldID()) }),
			func(info *datapb.FieldBinlog) (int64, *datapb.FieldBinlog) { return info.GetFieldID(), info })

		for _, info := range loadInfo.GetIndexInfos() {
			if len(info.GetIndexFilePaths()) == 0 {
				log.Warn("failed to add index for segment, index file list is empty, the segment may be too small")
				return merr.WrapErrIndexNotFound("index file list empty")
			}

			fieldInfo, ok := fieldInfos[info.GetFieldID()]
			if !ok {
				return merr.WrapErrParameterInvalid("index info with corresponding field info", "missing field info", strconv.FormatInt(fieldInfo.GetFieldID(), 10))
			}
			err := loader.loadFieldIndex(ctx, segment, info)
			if err != nil {
				log.Warn("failed to load index for segment", zap.Error(err))
				return err
			}
		}
		loader.notifyLoadFinish(loadInfo)
	}

	return loader.waitSegmentLoadDone(ctx, commonpb.SegmentState_SegmentStateNone, []int64{loadInfo.GetSegmentID()}, version)
}

func (loader *segmentLoader) LoadJSONIndex(ctx context.Context,
	seg Segment,
	loadInfo *querypb.SegmentLoadInfo,
) error {
	segment, ok := seg.(*LocalSegment)
	if !ok {
		return merr.WrapErrParameterInvalid("LocalSegment", fmt.Sprintf("%T", seg))
	}

	collection := segment.GetCollection()
	schemaHelper, _ := typeutil.CreateSchemaHelper(collection.Schema())

	jsonKeyIndexInfo := make(map[int64]*datapb.JsonKeyStats, len(loadInfo.GetJsonKeyStatsLogs()))
	for _, fieldStatsLog := range loadInfo.GetJsonKeyStatsLogs() {
		jsonKeyLog, ok := jsonKeyIndexInfo[fieldStatsLog.FieldID]
		if !ok {
			jsonKeyIndexInfo[fieldStatsLog.FieldID] = fieldStatsLog
		} else if fieldStatsLog.GetVersion() > jsonKeyLog.GetVersion() {
			jsonKeyIndexInfo[fieldStatsLog.FieldID] = fieldStatsLog
		}
	}
	for _, info := range jsonKeyIndexInfo {
		if err := segment.LoadJSONKeyIndex(ctx, info, schemaHelper); err != nil {
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
			return fmt.Errorf("load segment failed, GPU OOM if loaded, GpuMemUsage(bytes) = %v, usedGpuMem(bytes) = %v, maxGPUMem(bytes) = %v",
				fieldGpuMem,
				usedGpuMem,
				maxGpuMemSize)
		}
		currentGpuMem[minId] += minGpuMem
	}
	return nil
}
