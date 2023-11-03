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
#cgo pkg-config: milvus_segcore milvus_common

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"
#include "common/init_c.h"

*/
import "C"

import (
	"context"
	"fmt"
	"path"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	UsedDiskMemoryRatio = 4
)

var ErrReadDeltaMsgFailed = errors.New("ReadDeltaMsgFailed")

type Loader interface {
	// Load loads binlogs, and spawn segments,
	// NOTE: make sure the ref count of the corresponding collection will never go down to 0 during this
	Load(ctx context.Context, collectionID int64, segmentType SegmentType, version int64, segments ...*querypb.SegmentLoadInfo) ([]Segment, error)

	LoadDeltaLogs(ctx context.Context, segment *LocalSegment, deltaLogs []*datapb.FieldBinlog) error

	// LoadBloomFilterSet loads needed statslog for RemoteSegment.
	LoadBloomFilterSet(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error)

	// LoadIndex append index for segment and remove vector binlogs.
	LoadIndex(ctx context.Context, segment *LocalSegment, info *querypb.SegmentLoadInfo, version int64) error
}

type LoadResource struct {
	MemorySize uint64
	DiskSize   uint64
	WorkNum    int
}

func (r *LoadResource) Add(resource LoadResource) {
	r.MemorySize += resource.MemorySize
	r.DiskSize += resource.DiskSize
	r.WorkNum += resource.WorkNum
}

func (r *LoadResource) Sub(resource LoadResource) {
	r.MemorySize -= resource.MemorySize
	r.DiskSize -= resource.DiskSize
	r.WorkNum -= resource.WorkNum
}

func NewLoader(
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

	loader := &segmentLoader{
		manager:         manager,
		cm:              cm,
		loadingSegments: typeutil.NewConcurrentMap[int64, *loadResult](),
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

	mut sync.Mutex
	// The channel will be closed as the segment loaded
	loadingSegments   *typeutil.ConcurrentMap[int64, *loadResult]
	committedResource LoadResource
}

var _ Loader = (*segmentLoader)(nil)

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
	// Filter out loaded & loading segments
	infos := loader.prepare(segmentType, version, segments...)
	defer loader.unregister(infos...)

	log.With(
		zap.Int64s("requestSegments", lo.Map(segments, func(s *querypb.SegmentLoadInfo, _ int) int64 { return s.GetSegmentID() })),
		zap.Int64s("preparedSegments", lo.Map(infos, func(s *querypb.SegmentLoadInfo, _ int) int64 { return s.GetSegmentID() })),
	)

	// continue to wait other task done
	log.Info("start loading...", zap.Int("segmentNum", len(segments)), zap.Int("afterFilter", len(infos)))

	// Check memory & storage limit
	resource, concurrencyLevel, err := loader.requestResource(ctx, infos...)
	if err != nil {
		log.Warn("request resource failed", zap.Error(err))
		return nil, err
	}
	defer loader.freeRequest(resource)

	newSegments := typeutil.NewConcurrentMap[int64, *LocalSegment]()
	loaded := typeutil.NewConcurrentMap[int64, *LocalSegment]()
	defer func() {
		newSegments.Range(func(_ int64, s *LocalSegment) bool {
			s.Release()
			return true
		})
		debug.FreeOSMemory()
	}()

	for _, info := range infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID
		shard := info.InsertChannel

		collection := loader.manager.Collection.Get(collectionID)
		if collection == nil {
			err := merr.WrapErrCollectionNotFound(collectionID)
			log.Warn("failed to get collection", zap.Error(err))
			return nil, err
		}
		segment, err := NewSegment(collection, segmentID, partitionID, collectionID, shard, segmentType, version, info.GetStartPosition(), info.GetDeltaPosition())
		if err != nil {
			log.Warn("load segment failed when create new segment",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return nil, err
		}

		newSegments.Insert(segmentID, segment)
	}

	loadSegmentFunc := func(idx int) error {
		loadInfo := infos[idx]
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		segment, _ := newSegments.Get(segmentID)

		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		err := loader.loadSegment(ctx, segment, loadInfo)
		if err != nil {
			log.Warn("load segment failed when load data into memory",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return err
		}
		loader.manager.Segment.Put(segmentType, segment)
		newSegments.GetAndRemove(segmentID)
		loaded.Insert(segmentID, segment)
		log.Info("load segment done", zap.Int64("segmentID", segmentID))
		loader.notifyLoadFinish(loadInfo)

		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(tr.ElapseSpan().Seconds())
		return nil
	}

	// Start to load,
	// Make sure we can always benefit from concurrency, and not spawn too many idle goroutines
	log.Info("start to load segments in parallel",
		zap.Int("segmentNum", len(infos)),
		zap.Int("concurrencyLevel", concurrencyLevel))
	err = funcutil.ProcessFuncParallel(len(infos),
		concurrencyLevel, loadSegmentFunc, "loadSegmentFunc")
	if err != nil {
		log.Warn("failed to load some segments", zap.Error(err))
		return nil, err
	}

	// Wait for all segments loaded
	if err := loader.waitSegmentLoadDone(ctx, segmentType, lo.Map(segments, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })...); err != nil {
		log.Warn("failed to wait the filtered out segments load done", zap.Error(err))
		return nil, err
	}

	log.Info("all segment load done")
	var result []Segment
	loaded.Range(func(_ int64, s *LocalSegment) bool {
		result = append(result, s)
		return true
	})
	return result, nil
}

func (loader *segmentLoader) prepare(segmentType SegmentType, version int64, segments ...*querypb.SegmentLoadInfo) []*querypb.SegmentLoadInfo {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	// filter out loaded & loading segments
	infos := make([]*querypb.SegmentLoadInfo, 0, len(segments))
	for _, segment := range segments {
		// Not loaded & loading
		if len(loader.manager.Segment.GetBy(WithType(segmentType), WithID(segment.GetSegmentID()))) == 0 &&
			!loader.loadingSegments.Contain(segment.GetSegmentID()) {
			infos = append(infos, segment)
			loader.loadingSegments.Insert(segment.GetSegmentID(), newLoadResult())
		} else {
			// try to update segment version before skip load operation
			loader.manager.Segment.UpdateSegmentBy(IncreaseVersion(version),
				WithType(segmentType), WithID(segment.SegmentID))
			log.Info("skip loaded/loading segment", zap.Int64("segmentID", segment.GetSegmentID()),
				zap.Bool("isLoaded", len(loader.manager.Segment.GetBy(WithType(segmentType), WithID(segment.GetSegmentID()))) > 0),
				zap.Bool("isLoading", loader.loadingSegments.Contain(segment.GetSegmentID())),
			)
		}
	}

	return infos
}

func (loader *segmentLoader) unregister(segments ...*querypb.SegmentLoadInfo) {
	loader.mut.Lock()
	defer loader.mut.Unlock()
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
func (loader *segmentLoader) requestResource(ctx context.Context, infos ...*querypb.SegmentLoadInfo) (LoadResource, int, error) {
	resource := LoadResource{}
	// we need to deal with empty infos case separately,
	// because the following judgement for requested resources are based on current status and static config
	// which may block empty-load operations by accident
	if len(infos) == 0 {
		return resource, 0, nil
	}

	segmentIDs := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
		return info.GetSegmentID()
	})
	log := log.Ctx(ctx).With(
		zap.Int64s("segmentIDs", segmentIDs),
	)

	loader.mut.Lock()
	defer loader.mut.Unlock()

	memoryUsage := hardware.GetUsedMemoryCount()
	totalMemory := hardware.GetMemoryCount()

	diskUsage, err := GetLocalUsedSize(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		return resource, 0, errors.Wrap(err, "get local used size failed")
	}
	diskCap := paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsUint64()

	poolCap := hardware.GetCPUNum() * paramtable.Get().CommonCfg.HighPriorityThreadCoreCoefficient.GetAsInt()
	if poolCap > 256 {
		poolCap = 256
	}
	if loader.committedResource.WorkNum >= poolCap {
		return resource, 0, merr.WrapErrServiceRequestLimitExceeded(int32(poolCap))
	} else if loader.committedResource.MemorySize+memoryUsage >= totalMemory {
		return resource, 0, merr.WrapErrServiceMemoryLimitExceeded(float32(loader.committedResource.MemorySize+memoryUsage), float32(totalMemory))
	} else if loader.committedResource.DiskSize+uint64(diskUsage) >= diskCap {
		return resource, 0, merr.WrapErrServiceDiskLimitExceeded(float32(loader.committedResource.DiskSize+uint64(diskUsage)), float32(diskCap))
	}

	concurrencyLevel := funcutil.Min(hardware.GetCPUNum(), len(infos))

	for _, info := range infos {
		for _, field := range info.GetBinlogPaths() {
			resource.WorkNum += len(field.GetBinlogs())
		}
		for _, index := range info.GetIndexInfos() {
			resource.WorkNum += len(index.IndexFilePaths)
		}
	}

	for ; concurrencyLevel > 1; concurrencyLevel /= 2 {
		_, _, err := loader.checkSegmentSize(ctx, infos, concurrencyLevel)
		if err == nil {
			break
		}
	}

	mu, du, err := loader.checkSegmentSize(ctx, infos, concurrencyLevel)
	if err != nil {
		log.Warn("no sufficient resource to load segments", zap.Error(err))
		return resource, 0, err
	}

	resource.MemorySize += mu
	resource.DiskSize += du

	toMB := func(mem uint64) float64 {
		return float64(mem) / 1024 / 1024
	}
	loader.committedResource.Add(resource)
	log.Info("request resource for loading segments (unit in MiB)",
		zap.Int("workerNum", resource.WorkNum),
		zap.Int("committedWorkerNum", loader.committedResource.WorkNum),
		zap.Float64("memory", toMB(resource.MemorySize)),
		zap.Float64("committedMemory", toMB(loader.committedResource.MemorySize)),
		zap.Float64("disk", toMB(resource.DiskSize)),
		zap.Float64("committedDisk", toMB(loader.committedResource.DiskSize)),
	)

	return resource, concurrencyLevel, nil
}

// freeRequest returns request memory & storage usage request.
func (loader *segmentLoader) freeRequest(resource LoadResource) {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	loader.committedResource.Sub(resource)
}

func (loader *segmentLoader) waitSegmentLoadDone(ctx context.Context, segmentType SegmentType, segmentIDs ...int64) error {
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

		log.Info("segment loaded...", zap.Int64("segmentID", segmentID))
	}
	return nil
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

func (loader *segmentLoader) loadSegment(ctx context.Context,
	segment *LocalSegment,
	loadInfo *querypb.SegmentLoadInfo,
) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.Collection()),
		zap.Int64("partitionID", segment.Partition()),
		zap.String("shard", segment.Shard()),
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
		fieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
		for _, indexInfo := range loadInfo.IndexInfos {
			if len(indexInfo.GetIndexFilePaths()) > 0 {
				fieldID := indexInfo.FieldID
				fieldID2IndexInfo[fieldID] = indexInfo
			}
		}

		indexedFieldInfos := make(map[int64]*IndexedFieldInfo)
		fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(loadInfo.BinlogPaths))

		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID
			// check num rows of data meta and index meta are consistent
			if indexInfo, ok := fieldID2IndexInfo[fieldID]; ok {
				fieldInfo := &IndexedFieldInfo{
					FieldBinlog: fieldBinlog,
					IndexInfo:   indexInfo,
				}
				indexedFieldInfos[fieldID] = fieldInfo
			} else {
				fieldBinlogs = append(fieldBinlogs, fieldBinlog)
			}
		}

		log.Info("load fields...",
			zap.Int64s("indexedFields", lo.Keys(indexedFieldInfos)),
		)
		if err := loader.loadFieldsIndex(ctx, collection.Schema(), segment, loadInfo.GetNumOfRows(), indexedFieldInfos); err != nil {
			return err
		}
		if err := loader.loadSealedSegmentFields(ctx, segment, fieldBinlogs, loadInfo.GetNumOfRows()); err != nil {
			return err
		}
		if err := segment.AddFieldDataInfo(loadInfo.GetNumOfRows(), loadInfo.GetBinlogPaths()); err != nil {
			return err
		}
		// https://github.com/milvus-io/milvus/23654
		// legacy entry num = 0
		if err := loader.patchEntryNumber(ctx, segment, loadInfo); err != nil {
			return err
		}
	} else {
		if err := segment.LoadMultiFieldData(loadInfo.GetNumOfRows(), loadInfo.BinlogPaths); err != nil {
			return err
		}
	}

	// load statslog if it's growing segment
	if segment.typ == SegmentTypeGrowing {
		log.Info("loading statslog...")
		pkStatsBinlogs, logType := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segment.segmentID, segment.bloomFilterSet, pkStatsBinlogs, logType)
		if err != nil {
			return err
		}
	}

	log.Info("loading delta...")
	return loader.LoadDeltaLogs(ctx, segment, loadInfo.Deltalogs)
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

func (loader *segmentLoader) loadSealedSegmentFields(ctx context.Context, segment *LocalSegment, fields []*datapb.FieldBinlog, rowCount int64) error {
	runningGroup, _ := errgroup.WithContext(ctx)
	for _, field := range fields {
		fieldBinLog := field
		fieldID := field.FieldID
		runningGroup.Go(func() error {
			return segment.LoadFieldData(fieldID, rowCount, fieldBinLog)
		})
	}
	err := runningGroup.Wait()
	if err != nil {
		return err
	}

	log.Ctx(ctx).Info("load field binlogs done for sealed segment",
		zap.Int64("collection", segment.collectionID),
		zap.Int64("segment", segment.segmentID),
		zap.Int("len(field)", len(fields)),
		zap.String("segmentType", segment.Type().String()))

	return nil
}

func (loader *segmentLoader) loadFieldsIndex(ctx context.Context,
	schema *schemapb.CollectionSchema,
	segment *LocalSegment,
	numRows int64,
	vecFieldInfos map[int64]*IndexedFieldInfo,
) error {
	schemaHelper, _ := typeutil.CreateSchemaHelper(schema)

	for fieldID, fieldInfo := range vecFieldInfos {
		indexInfo := fieldInfo.IndexInfo
		err := loader.loadFieldIndex(ctx, segment, indexInfo)
		if err != nil {
			return err
		}

		log.Info("load field binlogs done for sealed segment with index",
			zap.Int64("collection", segment.collectionID),
			zap.Int64("segment", segment.segmentID),
			zap.Int64("fieldID", fieldID),
			zap.Any("binlog", fieldInfo.FieldBinlog.Binlogs),
			zap.Int32("current_index_version", fieldInfo.IndexInfo.GetCurrentIndexVersion()),
		)

		segment.AddIndex(fieldID, fieldInfo)

		// set average row data size of variable field
		field, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			return err
		}
		if typeutil.IsVariableDataType(field.GetDataType()) {
			err = segment.UpdateFieldRawDataSize(numRows, fieldInfo.FieldBinlog)
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

	// 2. use index path to update segment
	indexInfo.IndexFilePaths = filteredPaths
	fieldType, err := loader.getFieldType(segment.Collection(), indexInfo.FieldID)
	if err != nil {
		return err
	}

	return segment.LoadIndex(indexInfo, fieldType)
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

func (loader *segmentLoader) LoadDeltaLogs(ctx context.Context, segment *LocalSegment, deltaLogs []*datapb.FieldBinlog) error {
	dCodec := storage.DeleteCodec{}
	var blobs []*storage.Blob
	for _, deltaLog := range deltaLogs {
		for _, bLog := range deltaLog.GetBinlogs() {
			// the segment has applied the delta logs, skip it
			if bLog.GetTimestampTo() > 0 && // this field may be missed in legacy versions
				bLog.GetTimestampTo() < segment.LastDeltaTimestamp() {
				continue
			}
			value, err := loader.cm.Read(ctx, bLog.GetLogPath())
			if err != nil {
				return err
			}
			blob := &storage.Blob{
				Key:   bLog.GetLogPath(),
				Value: value,
			}
			blobs = append(blobs, blob)
		}
	}
	if len(blobs) == 0 {
		log.Info("there are no delta logs saved with segment, skip loading delete record", zap.Any("segmentID", segment.segmentID))
		return nil
	}
	_, _, deltaData, err := dCodec.Deserialize(blobs)
	if err != nil {
		return err
	}

	err = segment.LoadDeltaData(deltaData)
	if err != nil {
		return err
	}
	return nil
}

func (loader *segmentLoader) patchEntryNumber(ctx context.Context, segment *LocalSegment, loadInfo *querypb.SegmentLoadInfo) error {
	var needReset bool

	segment.fieldIndexes.Range(func(fieldID int64, info *IndexedFieldInfo) bool {
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

	log.Warn("legacy segment binlog found, start to patch entry num", zap.Int64("segmentID", segment.segmentID))
	rowIDField := lo.FindOrElse(loadInfo.BinlogPaths, nil, func(binlog *datapb.FieldBinlog) bool {
		return binlog.GetFieldID() == common.RowIDField
	})

	if rowIDField == nil {
		return errors.New("rowID field binlog not found")
	}

	counts := make([]int64, 0, len(rowIDField.GetBinlogs()))
	for _, binlog := range rowIDField.GetBinlogs() {
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

		rowIDs, err := er.GetInt64FromPayload()
		if err != nil {
			return err
		}
		counts = append(counts, int64(len(rowIDs)))
	}

	var err error
	segment.fieldIndexes.Range(func(fieldID int64, info *IndexedFieldInfo) bool {
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

func GetIndexResourceUsage(indexInfo *querypb.FieldIndexInfo) (uint64, uint64, error) {
	indexType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.IndexTypeKey, indexInfo.IndexParams)
	if err != nil {
		return 0, 0, fmt.Errorf("index type not exist in index params")
	}
	if indexType == indexparamcheck.IndexDISKANN {
		neededMemSize := indexInfo.IndexSize / UsedDiskMemoryRatio
		neededDiskSize := indexInfo.IndexSize - neededMemSize
		return uint64(neededMemSize), uint64(neededDiskSize), nil
	}

	return uint64(indexInfo.IndexSize), 0, nil
}

// checkSegmentSize checks whether the memory & disk is sufficient to load the segments with given concurrency,
// returns the memory & disk usage while loading if possible to load,
// otherwise, returns error
func (loader *segmentLoader) checkSegmentSize(ctx context.Context, segmentLoadInfos []*querypb.SegmentLoadInfo, concurrency int) (uint64, uint64, error) {
	if len(segmentLoadInfos) == 0 || concurrency == 0 {
		return 0, 0, nil
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segmentLoadInfos[0].GetCollectionID()),
	)

	toMB := func(mem uint64) float64 {
		return float64(mem) / 1024 / 1024
	}

	memUsage := hardware.GetUsedMemoryCount() + loader.committedResource.MemorySize
	totalMem := hardware.GetMemoryCount()
	if memUsage == 0 || totalMem == 0 {
		return 0, 0, errors.New("get memory failed when checkSegmentSize")
	}

	localDiskUsage, err := GetLocalUsedSize(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		return 0, 0, errors.Wrap(err, "get local used size failed")
	}

	metrics.QueryNodeDiskUsedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(toMB(uint64(localDiskUsage)))
	diskUsage := uint64(localDiskUsage) + loader.committedResource.DiskSize

	mmapEnabled := len(paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()) > 0
	maxSegmentSize := uint64(0)
	predictMemUsage := memUsage
	predictDiskUsage := diskUsage
	for _, loadInfo := range segmentLoadInfos {
		oldUsedMem := predictMemUsage
		vecFieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
		for _, fieldIndexInfo := range loadInfo.IndexInfos {
			if fieldIndexInfo.EnableIndex {
				fieldID := fieldIndexInfo.FieldID
				vecFieldID2IndexInfo[fieldID] = fieldIndexInfo
			}
		}

		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID
			if fieldIndexInfo, ok := vecFieldID2IndexInfo[fieldID]; ok {
				neededMemSize, neededDiskSize, err := GetIndexResourceUsage(fieldIndexInfo)
				if err != nil {
					log.Warn("failed to get index size",
						zap.Int64("collectionID", loadInfo.CollectionID),
						zap.Int64("segmentID", loadInfo.SegmentID),
						zap.Int64("indexBuildID", fieldIndexInfo.BuildID),
						zap.Error(err),
					)
					return 0, 0, err
				}
				if mmapEnabled {
					predictDiskUsage += neededMemSize + neededDiskSize
				} else {
					predictMemUsage += neededMemSize
					predictDiskUsage += neededDiskSize
				}
			} else {
				if mmapEnabled {
					predictDiskUsage += uint64(getBinlogDataSize(fieldBinlog))
				} else {
					predictMemUsage += uint64(getBinlogDataSize(fieldBinlog))
				}
			}
		}

		// get size of stats data
		for _, fieldBinlog := range loadInfo.Statslogs {
			predictMemUsage += uint64(getBinlogDataSize(fieldBinlog))
		}

		// get size of delete data
		for _, fieldBinlog := range loadInfo.Deltalogs {
			predictMemUsage += uint64(getBinlogDataSize(fieldBinlog))
		}

		if predictMemUsage-oldUsedMem > maxSegmentSize {
			maxSegmentSize = predictMemUsage - oldUsedMem
		}
	}

	log.Info("predict memory and disk usage while loading (in MiB)",
		zap.Float64("maxSegmentSize", toMB(maxSegmentSize)),
		zap.Int("concurrency", concurrency),
		zap.Float64("committedMemSize", toMB(loader.committedResource.MemorySize)),
		zap.Float64("memUsage", toMB(memUsage)),
		zap.Float64("committedDiskSize", toMB(loader.committedResource.DiskSize)),
		zap.Float64("diskUsage", toMB(diskUsage)),
		zap.Float64("predictMemUsage", toMB(predictMemUsage)),
		zap.Float64("predictDiskUsage", toMB(predictDiskUsage)),
		zap.Bool("mmapEnabled", mmapEnabled),
	)

	if !mmapEnabled && predictMemUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
		return 0, 0, fmt.Errorf("load segment failed, OOM if load, maxSegmentSize = %v MB, concurrency = %d, memUsage = %v MB, predictMemUsage = %v MB, totalMem = %v MB thresholdFactor = %f",
			toMB(maxSegmentSize),
			concurrency,
			toMB(memUsage),
			toMB(predictMemUsage),
			toMB(totalMem),
			paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
	}

	if mmapEnabled && memUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
		return 0, 0, fmt.Errorf("load segment failed, OOM if load, maxSegmentSize = %v MB, concurrency = %d, memUsage = %v MB, predictMemUsage = %v MB, totalMem = %v MB thresholdFactor = %f",
			toMB(maxSegmentSize),
			concurrency,
			toMB(memUsage),
			toMB(predictMemUsage),
			toMB(totalMem),
			paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
	}

	if predictDiskUsage > uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())*paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()) {
		return 0, 0, fmt.Errorf("load segment failed, disk space is not enough, diskUsage = %v MB, predictDiskUsage = %v MB, totalDisk = %v MB, thresholdFactor = %f",
			toMB(diskUsage),
			toMB(predictDiskUsage),
			toMB(uint64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())),
			paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat())
	}

	return predictMemUsage - memUsage, predictDiskUsage - diskUsage, nil
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

func (loader *segmentLoader) LoadIndex(ctx context.Context, segment *LocalSegment, loadInfo *querypb.SegmentLoadInfo, version int64) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collection", segment.Collection()),
		zap.Int64("segment", segment.ID()),
	)

	// Filter out LOADING segments only
	// use None to avoid loaded check
	infos := loader.prepare(commonpb.SegmentState_SegmentStateNone, version, loadInfo)
	defer loader.unregister(infos...)

	indexInfo := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *querypb.SegmentLoadInfo {
		info = typeutil.Clone(info)
		info.BinlogPaths = nil
		info.Deltalogs = nil
		info.Statslogs = nil
		return info
	})
	resource, _, err := loader.requestResource(ctx, indexInfo...)
	if err != nil {
		return err
	}
	defer loader.freeRequest(resource)

	log.Info("segment loader start to load index", zap.Int("segmentNumAfterFilter", len(infos)))

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
				return merr.WrapErrParameterInvalid("index info with corresponding  field info", "missing field info", strconv.FormatInt(fieldInfo.GetFieldID(), 10))
			}
			err := loader.loadFieldIndex(ctx, segment, info)
			if err != nil {
				log.Warn("failed to load index for segment", zap.Error(err))
				return err
			}
			segment.AddIndex(info.FieldID, &IndexedFieldInfo{
				IndexInfo:   info,
				FieldBinlog: fieldInfo,
			})
		}
		loader.notifyLoadFinish(loadInfo)
	}

	return loader.waitSegmentLoadDone(ctx, commonpb.SegmentState_SegmentStateNone, loadInfo.GetSegmentID())
}

func getBinlogDataSize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.LogSize
	}

	return fieldSize
}

func getIndexEngineVersion() (minimal, current int32) {
	cMinimal, cCurrent := C.GetMinimalIndexVersion(), C.GetCurrentIndexVersion()
	return int32(cMinimal), int32(cCurrent)
}
