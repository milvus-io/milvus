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

#include "futures/future_c.h"
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SegmentType = commonpb.SegmentState

const (
	SegmentTypeGrowing = commonpb.SegmentState_Growing
	SegmentTypeSealed  = commonpb.SegmentState_Sealed
)

var ErrSegmentUnhealthy = errors.New("segment unhealthy")

// IndexedFieldInfo contains binlog info of vector field
type IndexedFieldInfo struct {
	FieldBinlog *datapb.FieldBinlog
	IndexInfo   *querypb.FieldIndexInfo
	IsLoaded    bool
}

type baseSegment struct {
	collection *Collection
	version    *atomic.Int64

	segmentType    SegmentType
	bloomFilterSet *pkoracle.BloomFilterSet
	loadInfo       *atomic.Pointer[querypb.SegmentLoadInfo]
	isLazyLoad     bool
	skipGrowingBF  bool // Skip generating or maintaining BF for growing segments; deletion checks will be handled in segcore.
	channel        metautil.Channel

	bm25Stats map[int64]*storage.BM25Stats

	resourceUsageCache *atomic.Pointer[ResourceUsage]

	needUpdatedVersion *atomic.Int64 // only for lazy load mode update index
}

func newBaseSegment(collection *Collection, segmentType SegmentType, version int64, loadInfo *querypb.SegmentLoadInfo) (baseSegment, error) {
	channel, err := metautil.ParseChannel(loadInfo.GetInsertChannel(), channelMapper)
	if err != nil {
		return baseSegment{}, err
	}
	bs := baseSegment{
		collection:     collection,
		loadInfo:       atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo),
		version:        atomic.NewInt64(version),
		segmentType:    segmentType,
		bloomFilterSet: pkoracle.NewBloomFilterSet(loadInfo.GetSegmentID(), loadInfo.GetPartitionID(), segmentType),
		bm25Stats:      make(map[int64]*storage.BM25Stats),
		channel:        channel,
		isLazyLoad:     isLazyLoad(collection, segmentType),
		skipGrowingBF:  segmentType == SegmentTypeGrowing && paramtable.Get().QueryNodeCfg.SkipGrowingSegmentBF.GetAsBool(),

		resourceUsageCache: atomic.NewPointer[ResourceUsage](nil),
		needUpdatedVersion: atomic.NewInt64(0),
	}
	return bs, nil
}

// isLazyLoad checks if the segment is lazy load
func isLazyLoad(collection *Collection, segmentType SegmentType) bool {
	return segmentType == SegmentTypeSealed && // only sealed segment enable lazy load
		(common.IsCollectionLazyLoadEnabled(collection.Schema().Properties...) || // collection level lazy load
			(!common.HasLazyload(collection.Schema().Properties) &&
				params.Params.QueryNodeCfg.LazyLoadEnabled.GetAsBool())) // global level lazy load
}

// ID returns the identity number.
func (s *baseSegment) ID() int64 {
	return s.loadInfo.Load().GetSegmentID()
}

func (s *baseSegment) Collection() int64 {
	return s.loadInfo.Load().GetCollectionID()
}

func (s *baseSegment) GetCollection() *Collection {
	return s.collection
}

func (s *baseSegment) Partition() int64 {
	return s.loadInfo.Load().GetPartitionID()
}

func (s *baseSegment) DatabaseName() string {
	return s.collection.GetDBName()
}

func (s *baseSegment) ResourceGroup() string {
	return s.collection.GetResourceGroup()
}

func (s *baseSegment) Shard() metautil.Channel {
	return s.channel
}

func (s *baseSegment) Type() SegmentType {
	return s.segmentType
}

func (s *baseSegment) Level() datapb.SegmentLevel {
	return s.loadInfo.Load().GetLevel()
}

func (s *baseSegment) IsSorted() bool {
	return s.loadInfo.Load().GetIsSorted()
}

func (s *baseSegment) StartPosition() *msgpb.MsgPosition {
	return s.loadInfo.Load().GetStartPosition()
}

func (s *baseSegment) Version() int64 {
	return s.version.Load()
}

func (s *baseSegment) CASVersion(old, newVersion int64) bool {
	return s.version.CompareAndSwap(old, newVersion)
}

func (s *baseSegment) LoadInfo() *querypb.SegmentLoadInfo {
	return s.loadInfo.Load()
}

func (s *baseSegment) UpdateBloomFilter(pks []storage.PrimaryKey) {
	if s.skipGrowingBF {
		return
	}
	s.bloomFilterSet.UpdateBloomFilter(pks)
}

func (s *baseSegment) UpdateBM25Stats(stats map[int64]*storage.BM25Stats) {
	for fieldID, new := range stats {
		if current, ok := s.bm25Stats[fieldID]; ok {
			current.Merge(new)
		} else {
			s.bm25Stats[fieldID] = new
		}
	}
}

func (s *baseSegment) GetBM25Stats() map[int64]*storage.BM25Stats {
	return s.bm25Stats
}

// MayPkExist returns true if the given PK exists in the PK range and being positive through the bloom filter,
// false otherwise,
// may returns true even the PK doesn't exist actually
func (s *baseSegment) MayPkExist(pk *storage.LocationsCache) bool {
	if s.skipGrowingBF {
		return true
	}
	return s.bloomFilterSet.MayPkExist(pk)
}

func (s *baseSegment) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	if s.skipGrowingBF {
		allPositive := make([]bool, lc.Size())
		for i := 0; i < lc.Size(); i++ {
			allPositive[i] = true
		}
		return allPositive
	}
	return s.bloomFilterSet.BatchPkExist(lc)
}

// ResourceUsageEstimate returns the estimated resource usage of the segment.
func (s *baseSegment) ResourceUsageEstimate() ResourceUsage {
	if s.segmentType == SegmentTypeGrowing {
		// Growing segment cannot do resource usage estimate.
		return ResourceUsage{}
	}
	cache := s.resourceUsageCache.Load()
	if cache != nil {
		return *cache
	}

	usage, err := getResourceUsageEstimateOfSegment(s.collection.Schema(), s.LoadInfo(), resourceEstimateFactor{
		memoryUsageFactor:        1.0,
		memoryIndexUsageFactor:   1.0,
		enableTempSegmentIndex:   false,
		deltaDataExpansionFactor: paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
	})
	if err != nil {
		// Should never failure, if failed, segment should never be loaded.
		log.Warn("unreachable: failed to get resource usage estimate of segment", zap.Error(err), zap.Int64("collectionID", s.Collection()), zap.Int64("segmentID", s.ID()))
		return ResourceUsage{}
	}
	s.resourceUsageCache.Store(usage)
	return *usage
}

func (s *baseSegment) IsLazyLoad() bool {
	return s.isLazyLoad
}

func (s *baseSegment) NeedUpdatedVersion() int64 {
	return s.needUpdatedVersion.Load()
}

func (s *baseSegment) SetLoadInfo(loadInfo *querypb.SegmentLoadInfo) {
	s.loadInfo.Store(loadInfo)
}

func (s *baseSegment) SetNeedUpdatedVersion(version int64) {
	s.needUpdatedVersion.Store(version)
}

type FieldInfo struct {
	*datapb.FieldBinlog
	RowCount int64
}

var _ Segment = (*LocalSegment)(nil)

// Segment is a wrapper of the underlying C-structure segment.
type LocalSegment struct {
	baseSegment
	ptrLock *state.LoadStateLock
	ptr     C.CSegmentInterface // TODO: Remove in future, after move load index into segcore package.
	// always keep same with csegment.RawPtr(), for eaiser to access,
	csegment segcore.CSegment

	// cached results, to avoid too many CGO calls
	memSize     *atomic.Int64
	rowNum      *atomic.Int64
	insertCount *atomic.Int64

	lastDeltaTimestamp *atomic.Uint64
	fields             *typeutil.ConcurrentMap[int64, *FieldInfo]
	fieldIndexes       *typeutil.ConcurrentMap[int64, *IndexedFieldInfo]
	warmupDispatcher   *AsyncWarmupDispatcher
}

func NewSegment(ctx context.Context,
	collection *Collection,
	segmentType SegmentType,
	version int64,
	loadInfo *querypb.SegmentLoadInfo,
	warmupDispatcher *AsyncWarmupDispatcher,
) (Segment, error) {
	log := log.Ctx(ctx)
	/*
		CStatus
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type, CSegmentInterface* newSegment);
	*/
	if loadInfo.GetLevel() == datapb.SegmentLevel_L0 {
		return NewL0Segment(collection, segmentType, version, loadInfo)
	}

	base, err := newBaseSegment(collection, segmentType, version, loadInfo)
	if err != nil {
		return nil, err
	}

	var locker *state.LoadStateLock
	switch segmentType {
	case SegmentTypeSealed:
		locker = state.NewLoadStateLock(state.LoadStateOnlyMeta)
	case SegmentTypeGrowing:
		locker = state.NewLoadStateLock(state.LoadStateDataLoaded)
	default:
		return nil, fmt.Errorf("illegal segment type %d when create segment %d", segmentType, loadInfo.GetSegmentID())
	}

	logger := log.With(
		zap.Int64("collectionID", loadInfo.GetCollectionID()),
		zap.Int64("partitionID", loadInfo.GetPartitionID()),
		zap.Int64("segmentID", loadInfo.GetSegmentID()),
		zap.String("segmentType", segmentType.String()),
		zap.String("level", loadInfo.GetLevel().String()),
	)

	var csegment segcore.CSegment
	if _, err := GetDynamicPool().Submit(func() (any, error) {
		var err error
		csegment, err = segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
			Collection:    collection.ccollection,
			SegmentID:     loadInfo.GetSegmentID(),
			SegmentType:   segmentType,
			IsSorted:      loadInfo.GetIsSorted(),
			EnableChunked: paramtable.Get().QueryNodeCfg.MultipleChunkedEnable.GetAsBool(),
		})
		return nil, err
	}).Await(); err != nil {
		logger.Warn("create segment failed", zap.Error(err))
		return nil, err
	}
	log.Info("create segment done")

	segment := &LocalSegment{
		baseSegment:        base,
		ptrLock:            locker,
		ptr:                C.CSegmentInterface(csegment.RawPointer()),
		csegment:           csegment,
		lastDeltaTimestamp: atomic.NewUint64(0),
		fields:             typeutil.NewConcurrentMap[int64, *FieldInfo](),
		fieldIndexes:       typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),

		memSize:          atomic.NewInt64(-1),
		rowNum:           atomic.NewInt64(-1),
		insertCount:      atomic.NewInt64(0),
		warmupDispatcher: warmupDispatcher,
	}

	if err := segment.initializeSegment(); err != nil {
		csegment.Release()
		return nil, err
	}
	return segment, nil
}

func (s *LocalSegment) initializeSegment() error {
	loadInfo := s.loadInfo.Load()
	indexedFieldInfos, fieldBinlogs := separateIndexAndBinlog(loadInfo)
	schemaHelper, _ := typeutil.CreateSchemaHelper(s.collection.Schema())

	for fieldID, info := range indexedFieldInfos {
		field, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			return err
		}
		indexInfo := info.IndexInfo
		s.fieldIndexes.Insert(indexInfo.GetFieldID(), &IndexedFieldInfo{
			FieldBinlog: &datapb.FieldBinlog{
				FieldID: indexInfo.GetFieldID(),
			},
			IndexInfo: indexInfo,
			IsLoaded:  false,
		})
		if !typeutil.IsVectorType(field.GetDataType()) && !s.HasRawData(fieldID) {
			s.fields.Insert(fieldID, &FieldInfo{
				FieldBinlog: info.FieldBinlog,
				RowCount:    loadInfo.GetNumOfRows(),
			})
		}
	}

	for _, binlogs := range fieldBinlogs {
		s.fields.Insert(binlogs.FieldID, &FieldInfo{
			FieldBinlog: binlogs,
			RowCount:    loadInfo.GetNumOfRows(),
		})
	}

	// Update the insert count when initialize the segment and update the metrics.
	s.insertCount.Store(loadInfo.GetNumOfRows())
	return nil
}

// PinIfNotReleased acquires the `ptrLock` and returns true if the pointer is valid
// Provide ONLY the read lock operations,
// don't make `ptrLock` public to avoid abusing of the mutex.
func (s *LocalSegment) PinIfNotReleased() error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	return nil
}

func (s *LocalSegment) Unpin() {
	s.ptrLock.Unpin()
}

func (s *LocalSegment) InsertCount() int64 {
	return s.insertCount.Load()
}

func (s *LocalSegment) RowNum() int64 {
	// if segment is not loaded, return 0 (maybe not loaded or release by lru)
	if !s.ptrLock.PinIf(state.IsDataLoaded) {
		return 0
	}
	defer s.ptrLock.Unpin()

	rowNum := s.rowNum.Load()
	if rowNum < 0 {
		GetDynamicPool().Submit(func() (any, error) {
			rowNum = s.csegment.RowNum()
			s.rowNum.Store(rowNum)
			return nil, nil
		}).Await()
	}
	return rowNum
}

func (s *LocalSegment) MemSize() int64 {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return 0
	}
	defer s.ptrLock.Unpin()

	memSize := s.memSize.Load()
	if memSize < 0 {
		GetDynamicPool().Submit(func() (any, error) {
			memSize = s.csegment.MemSize()
			s.memSize.Store(memSize)
			return nil, nil
		}).Await()
	}
	return memSize
}

func (s *LocalSegment) LastDeltaTimestamp() uint64 {
	return s.lastDeltaTimestamp.Load()
}

func (s *LocalSegment) GetIndex(fieldID int64) *IndexedFieldInfo {
	info, _ := s.fieldIndexes.Get(fieldID)
	return info
}

func (s *LocalSegment) ExistIndex(fieldID int64) bool {
	fieldInfo, ok := s.fieldIndexes.Get(fieldID)
	if !ok {
		return false
	}
	return fieldInfo.IndexInfo != nil
}

func (s *LocalSegment) HasRawData(fieldID int64) bool {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return false
	}
	defer s.ptrLock.Unpin()

	return s.csegment.HasRawData(fieldID)
}

func (s *LocalSegment) Indexes() []*IndexedFieldInfo {
	var result []*IndexedFieldInfo
	s.fieldIndexes.Range(func(key int64, value *IndexedFieldInfo) bool {
		result = append(result, value)
		return true
	})
	return result
}

func (s *LocalSegment) ResetIndexesLazyLoad(lazyState bool) {
	for _, indexInfo := range s.Indexes() {
		indexInfo.IsLoaded = lazyState
	}
}

func (s *LocalSegment) Search(ctx context.Context, searchReq *segcore.SearchRequest) (*segcore.SearchResult, error) {
	log := log.Ctx(ctx).WithLazy(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("segmentID", s.ID()),
		zap.String("segmentType", s.segmentType.String()),
	)

	if !s.ptrLock.PinIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	hasIndex := s.ExistIndex(searchReq.SearchFieldID())
	log = log.With(zap.Bool("withIndex", hasIndex))
	log.Debug("search segment...")

	tr := timerecord.NewTimeRecorder("cgoSearch")
	result, err := s.csegment.Search(ctx, searchReq)
	if err != nil {
		log.Warn("Search failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Debug("search segment done")
	return result, nil
}

func (s *LocalSegment) retrieve(ctx context.Context, plan *segcore.RetrievePlan, log *zap.Logger) (*segcore.RetrieveResult, error) {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log.Debug("begin to retrieve")

	tr := timerecord.NewTimeRecorder("cgoRetrieve")
	result, err := s.csegment.Retrieve(ctx, plan)
	if err != nil {
		log.Warn("Retrieve failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return result, nil
}

func (s *LocalSegment) Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	log := log.Ctx(ctx).WithLazy(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("msgID", plan.MsgID()),
		zap.String("segmentType", s.segmentType.String()),
	)

	result, err := s.retrieve(ctx, plan, log)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "partial-segcore-results-deserialization")
	defer span.End()

	retrieveResult, err := result.GetResult()
	if err != nil {
		log.Warn("unmarshal retrieve result failed", zap.Error(err))
		return nil, err
	}
	log.Debug("retrieve segment done", zap.Int("resultNum", len(retrieveResult.Offset)))
	return retrieveResult, nil
}

func (s *LocalSegment) retrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets, log *zap.Logger) (*segcore.RetrieveResult, error) {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log.Debug("begin to retrieve by offsets")
	tr := timerecord.NewTimeRecorder("cgoRetrieveByOffsets")
	result, err := s.csegment.RetrieveByOffsets(ctx, plan)
	if err != nil {
		log.Warn("RetrieveByOffsets failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return result, nil
}

func (s *LocalSegment) RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
	log := log.Ctx(ctx).WithLazy(zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("msgID", plan.MsgID()),
		zap.String("segmentType", s.segmentType.String()),
		zap.Int("resultNum", len(plan.Offsets)),
	)

	result, err := s.retrieveByOffsets(ctx, plan, log)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "reduced-segcore-results-deserialization")
	defer span.End()

	retrieveResult, err := result.GetResult()
	if err != nil {
		log.Warn("unmarshal retrieve by offsets result failed", zap.Error(err))
		return nil, err
	}
	log.Debug("retrieve by segment offsets done", zap.Int("resultNum", len(retrieveResult.Offset)))
	return retrieveResult, nil
}

func (s *LocalSegment) GetFieldDataPath(index *IndexedFieldInfo, offset int64) (dataPath string, offsetInBinlog int64) {
	offsetInBinlog = offset
	for _, binlog := range index.FieldBinlog.Binlogs {
		if offsetInBinlog < binlog.EntriesNum {
			dataPath = binlog.GetLogPath()
			break
		} else {
			offsetInBinlog -= binlog.EntriesNum
		}
	}
	return dataPath, offsetInBinlog
}

func (s *LocalSegment) Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error {
	if s.Type() != SegmentTypeGrowing {
		return fmt.Errorf("unexpected segmentType when segmentInsert, segmentType = %s", s.segmentType.String())
	}
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	var result *segcore.InsertResult
	var err error
	GetDynamicPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				"Insert",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()

		result, err = s.csegment.Insert(ctx, &segcore.InsertRequest{
			RowIDs:     rowIDs,
			Timestamps: timestamps,
			Record:     record,
		})
		return nil, nil
	}).Await()

	if err != nil {
		return err
	}
	s.insertCount.Add(result.InsertedRows)
	s.rowNum.Store(-1)
	s.memSize.Store(-1)
	return nil
}

func (s *LocalSegment) Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	/*
		CStatus
		Delete(CSegmentInterface c_segment,
		           long int reserved_offset,
		           long size,
		           const long* primary_keys,
		           const unsigned long* timestamps);
	*/

	if primaryKeys.Len() == 0 {
		return nil
	}
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	var err error
	GetDynamicPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				"Delete",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		_, err = s.csegment.Delete(ctx, &segcore.DeleteRequest{
			PrimaryKeys: primaryKeys,
			Timestamps:  timestamps,
		})
		return nil, nil
	}).Await()

	if err != nil {
		return err
	}

	s.rowNum.Store(-1)
	s.lastDeltaTimestamp.Store(timestamps[len(timestamps)-1])
	return nil
}

// -------------------------------------------------------------------------------------- interfaces for sealed segment
func (s *LocalSegment) LoadMultiFieldData(ctx context.Context) error {
	loadInfo := s.loadInfo.Load()
	rowCount := loadInfo.GetNumOfRows()
	fields := loadInfo.GetBinlogPaths()

	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	req := &segcore.LoadFieldDataRequest{
		MMapDir:  paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue(),
		RowCount: rowCount,
	}
	for _, field := range fields {
		req.Fields = append(req.Fields, segcore.LoadFieldDataInfo{
			Field: field,
		})
	}

	var err error
	GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				"LoadFieldData",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		_, err = s.csegment.LoadFieldData(ctx, req)
		return nil, nil
	}).Await()
	if err != nil {
		log.Warn("LoadMultiFieldData failed", zap.Error(err))
		return err
	}

	log.Info("load mutil field done", zap.Int64("row count", rowCount), zap.Int64("segmentID", s.ID()))
	return nil
}

func (s *LocalSegment) LoadFieldData(ctx context.Context, fieldID int64, rowCount int64, field *datapb.FieldBinlog) error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadFieldData-%d-%d", s.ID(), fieldID))
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", fieldID),
		zap.Int64("rowCount", rowCount),
	)
	log.Info("start loading field data for field")

	// TODO retrieve_enable should be considered
	collection := s.collection
	fieldSchema, err := getFieldSchema(collection.Schema(), fieldID)
	if err != nil {
		return err
	}
	mmapEnabled := isDataMmapEnable(fieldSchema)
	req := &segcore.LoadFieldDataRequest{
		MMapDir: paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue(),
		Fields: []segcore.LoadFieldDataInfo{{
			Field:      field,
			EnableMMap: mmapEnabled,
		}},
		RowCount: rowCount,
	}

	GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				"LoadFieldData",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		_, err = s.csegment.LoadFieldData(ctx, req)
		log.Info("submitted loadFieldData task to load pool")
		return nil, nil
	}).Await()

	if err != nil {
		log.Warn("LoadFieldData failed", zap.Error(err))
		return err
	}
	log.Info("load field done")
	return nil
}

func (s *LocalSegment) AddFieldDataInfo(ctx context.Context, rowCount int64, fields []*datapb.FieldBinlog) error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log := log.Ctx(ctx).WithLazy(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("row count", rowCount),
	)

	req := &segcore.AddFieldDataInfoRequest{
		Fields:   make([]segcore.LoadFieldDataInfo, 0, len(fields)),
		RowCount: rowCount,
	}
	for _, field := range fields {
		req.Fields = append(req.Fields, segcore.LoadFieldDataInfo{
			Field: field,
		})
	}

	var err error
	GetLoadPool().Submit(func() (any, error) {
		_, err = s.csegment.AddFieldDataInfo(ctx, req)
		return nil, nil
	}).Await()

	if err != nil {
		log.Warn("AddFieldDataInfo failed", zap.Error(err))
		return err
	}
	log.Info("add field data info done")
	return nil
}

func (s *LocalSegment) LoadDeltaData(ctx context.Context, deltaData *storage.DeltaData) error {
	pks, tss := deltaData.DeletePks(), deltaData.DeleteTimestamps()
	rowNum := deltaData.DeleteRowCount()

	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	ids, err := storage.ParsePrimaryKeysBatch2IDs(pks)
	if err != nil {
		return err
	}

	idsBlob, err := proto.Marshal(ids)
	if err != nil {
		return err
	}

	loadInfo := C.CLoadDeletedRecordInfo{
		timestamps:        unsafe.Pointer(&tss[0]),
		primary_keys:      (*C.uint8_t)(unsafe.Pointer(&idsBlob[0])),
		primary_keys_size: C.uint64_t(len(idsBlob)),
		row_count:         C.int64_t(rowNum),
	}
	/*
		CStatus
		LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info)
	*/
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				"LoadDeletedRecord",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		status = C.LoadDeletedRecord(s.ptr, loadInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "LoadDeletedRecord failed",
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID())); err != nil {
		return err
	}

	s.rowNum.Store(-1)
	s.lastDeltaTimestamp.Store(tss[len(tss)-1])

	log.Info("load deleted record done",
		zap.Int64("rowNum", rowNum),
		zap.String("segmentType", s.Type().String()))
	return nil
}

func GetCLoadInfoWithFunc(ctx context.Context,
	fieldSchema *schemapb.FieldSchema,
	s *querypb.SegmentLoadInfo,
	indexInfo *querypb.FieldIndexInfo,
	f func(c *LoadIndexInfo) error,
) error {
	// 1.
	loadIndexInfo, err := newLoadIndexInfo(ctx)
	if err != nil {
		return err
	}
	defer deleteLoadIndexInfo(loadIndexInfo)

	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	// as Knowhere reports error if encounter an unknown param, we need to delete it
	delete(indexParams, common.MmapEnabledKey)

	// some build params also exist in indexParams, which are useless during loading process
	if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexParams["index_type"]) {
		if err := indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows()); err != nil {
			return err
		}
	}

	// set whether enable offset cache for bitmap index
	if indexParams["index_type"] == indexparamcheck.IndexBitmap {
		indexparams.SetBitmapIndexLoadParams(paramtable.Get(), indexParams)
	}

	if err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams); err != nil {
		return err
	}

	enableMmap := isIndexMmapEnable(fieldSchema, indexInfo)

	indexInfoProto := &cgopb.LoadIndexInfo{
		CollectionID:       s.GetCollectionID(),
		PartitionID:        s.GetPartitionID(),
		SegmentID:          s.GetSegmentID(),
		Field:              fieldSchema,
		EnableMmap:         enableMmap,
		MmapDirPath:        paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue(),
		IndexID:            indexInfo.GetIndexID(),
		IndexBuildID:       indexInfo.GetBuildID(),
		IndexVersion:       indexInfo.GetIndexVersion(),
		IndexParams:        indexParams,
		IndexFiles:         indexInfo.GetIndexFilePaths(),
		IndexEngineVersion: indexInfo.GetCurrentIndexVersion(),
		IndexStoreVersion:  indexInfo.GetIndexStoreVersion(),
		IndexFileSize:      indexInfo.GetIndexSize(),
	}

	// 2.
	if err := loadIndexInfo.appendLoadIndexInfo(ctx, indexInfoProto); err != nil {
		log.Warn("fail to append load index info", zap.Error(err))
		return err
	}
	return f(loadIndexInfo)
}

func (s *LocalSegment) LoadIndex(ctx context.Context, indexInfo *querypb.FieldIndexInfo, fieldType schemapb.DataType) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", indexInfo.GetFieldID()),
		zap.Int64("indexID", indexInfo.GetIndexID()),
	)

	old := s.GetIndex(indexInfo.GetFieldID())
	// the index loaded
	if old != nil && old.IndexInfo.GetIndexID() == indexInfo.GetIndexID() && old.IsLoaded {
		log.Warn("index already loaded")
		return nil
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadIndex-%d-%d", s.ID(), indexInfo.GetFieldID()))
	defer sp.End()

	tr := timerecord.NewTimeRecorder("loadIndex")

	schemaHelper, err := typeutil.CreateSchemaHelper(s.GetCollection().Schema())
	if err != nil {
		return err
	}
	fieldSchema, err := schemaHelper.GetFieldFromID(indexInfo.GetFieldID())
	if err != nil {
		return err
	}

	// // if segment is pk sorted, user created indexes bring no performance gain but extra memory usage
	if s.IsSorted() && fieldSchema.GetIsPrimaryKey() {
		log.Info("skip loading index for pk field in sorted segment")
		// set field index, preventing repeated loading index task
		s.fieldIndexes.Insert(indexInfo.GetFieldID(), &IndexedFieldInfo{
			FieldBinlog: &datapb.FieldBinlog{
				FieldID: indexInfo.GetFieldID(),
			},
			IndexInfo: indexInfo,
			IsLoaded:  true,
		})
	}

	return s.innerLoadIndex(ctx, fieldSchema, indexInfo, tr, fieldType)
}

func (s *LocalSegment) innerLoadIndex(ctx context.Context,
	fieldSchema *schemapb.FieldSchema,
	indexInfo *querypb.FieldIndexInfo,
	tr *timerecord.TimeRecorder,
	fieldType schemapb.DataType,
) error {
	err := GetCLoadInfoWithFunc(ctx, fieldSchema,
		s.LoadInfo(), indexInfo, func(loadIndexInfo *LoadIndexInfo) error {
			newLoadIndexInfoSpan := tr.RecordSpan()

			if err := loadIndexInfo.loadIndex(ctx); err != nil {
				if loadIndexInfo.cleanLocalData(ctx) != nil {
					log.Warn("failed to clean cached data on disk after append index failed",
						zap.Int64("buildID", indexInfo.BuildID),
						zap.Int64("index version", indexInfo.IndexVersion))
				}
				return err
			}
			if s.Type() != SegmentTypeSealed {
				errMsg := fmt.Sprintln("updateSegmentIndex failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
				return errors.New(errMsg)
			}
			appendLoadIndexInfoSpan := tr.RecordSpan()

			// 3.
			err := s.UpdateIndexInfo(ctx, indexInfo, loadIndexInfo)
			if err != nil {
				return err
			}
			updateIndexInfoSpan := tr.RecordSpan()
			if !typeutil.IsVectorType(fieldType) || s.HasRawData(indexInfo.GetFieldID()) {
				return nil
			}

			// 4.
			mmapChunkCache := paramtable.Get().QueryNodeCfg.MmapChunkCache.GetAsBool()
			s.WarmupChunkCache(ctx, indexInfo.GetFieldID(), mmapChunkCache)
			warmupChunkCacheSpan := tr.RecordSpan()
			log.Info("Finish loading index",
				zap.Duration("newLoadIndexInfoSpan", newLoadIndexInfoSpan),
				zap.Duration("appendLoadIndexInfoSpan", appendLoadIndexInfoSpan),
				zap.Duration("updateIndexInfoSpan", updateIndexInfoSpan),
				zap.Duration("warmupChunkCacheSpan", warmupChunkCacheSpan),
			)
			return nil
		})
	if err != nil {
		log.Warn("load index failed", zap.Error(err))
	}
	return err
}

func (s *LocalSegment) LoadTextIndex(ctx context.Context, textLogs *datapb.TextIndexStats, schemaHelper *typeutil.SchemaHelper) error {
	log.Ctx(ctx).Info("load text index", zap.Int64("field id", textLogs.GetFieldID()), zap.Any("text logs", textLogs))

	f, err := schemaHelper.GetFieldFromID(textLogs.GetFieldID())
	if err != nil {
		return err
	}

	cgoProto := &indexcgopb.LoadTextIndexInfo{
		FieldID:      textLogs.GetFieldID(),
		Version:      textLogs.GetVersion(),
		BuildID:      textLogs.GetBuildID(),
		Files:        textLogs.GetFiles(),
		Schema:       f,
		CollectionID: s.Collection(),
		PartitionID:  s.Partition(),
	}

	marshaled, err := proto.Marshal(cgoProto)
	if err != nil {
		return err
	}

	var status C.CStatus
	_, _ = GetLoadPool().Submit(func() (any, error) {
		status = C.LoadTextIndex(s.ptr, (*C.uint8_t)(unsafe.Pointer(&marshaled[0])), (C.uint64_t)(len(marshaled)))
		return nil, nil
	}).Await()

	return HandleCStatus(ctx, &status, "LoadTextIndex failed")
}

func (s *LocalSegment) UpdateIndexInfo(ctx context.Context, indexInfo *querypb.FieldIndexInfo, info *LoadIndexInfo) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", indexInfo.FieldID),
	)
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.UpdateSealedSegmentIndex(s.ptr, info.cLoadIndexInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "UpdateSealedSegmentIndex failed",
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", indexInfo.FieldID)); err != nil {
		return err
	}

	s.fieldIndexes.Insert(indexInfo.GetFieldID(), &IndexedFieldInfo{
		FieldBinlog: &datapb.FieldBinlog{
			FieldID: indexInfo.GetFieldID(),
		},
		IndexInfo: indexInfo,
		IsLoaded:  true,
	})
	log.Info("updateSegmentIndex done")
	return nil
}

func (s *LocalSegment) WarmupChunkCache(ctx context.Context, fieldID int64, mmapEnabled bool) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", fieldID),
		zap.Bool("mmapEnabled", mmapEnabled),
	)
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return
	}
	defer s.ptrLock.Unpin()

	var status C.CStatus

	warmingUp := strings.ToLower(paramtable.Get().QueryNodeCfg.ChunkCacheWarmingUp.GetValue())
	switch warmingUp {
	case "sync":
		GetWarmupPool().Submit(func() (any, error) {
			cFieldID := C.int64_t(fieldID)
			cMmapEnabled := C.bool(mmapEnabled)
			status = C.WarmupChunkCache(s.ptr, cFieldID, cMmapEnabled)
			if err := HandleCStatus(ctx, &status, "warming up chunk cache failed"); err != nil {
				log.Warn("warming up chunk cache synchronously failed", zap.Error(err))
				return nil, err
			}
			log.Info("warming up chunk cache synchronously done")
			return nil, nil
		}).Await()
	case "async":
		task := func() (any, error) {
			// failed to wait for state update, return directly
			if !s.ptrLock.BlockUntilDataLoadedOrReleased() {
				return nil, nil
			}
			if s.PinIfNotReleased() != nil {
				return nil, nil
			}
			defer s.Unpin()

			cFieldID := C.int64_t(fieldID)
			cMmapEnabled := C.bool(mmapEnabled)
			status = C.WarmupChunkCache(s.ptr, cFieldID, cMmapEnabled)
			if err := HandleCStatus(ctx, &status, ""); err != nil {
				log.Warn("warming up chunk cache asynchronously failed", zap.Error(err))
				return nil, err
			}
			log.Info("warming up chunk cache asynchronously done")
			return nil, nil
		}
		s.warmupDispatcher.AddTask(task)
	default:
		// no warming up
	}
}

func (s *LocalSegment) UpdateFieldRawDataSize(ctx context.Context, numRows int64, fieldBinlog *datapb.FieldBinlog) error {
	var status C.CStatus
	fieldID := fieldBinlog.FieldID
	fieldDataSize := int64(0)
	for _, binlog := range fieldBinlog.GetBinlogs() {
		fieldDataSize += binlog.GetMemorySize()
	}
	GetDynamicPool().Submit(func() (any, error) {
		status = C.UpdateFieldRawDataSize(s.ptr, C.int64_t(fieldID), C.int64_t(numRows), C.int64_t(fieldDataSize))
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "updateFieldRawDataSize failed"); err != nil {
		return err
	}

	log.Ctx(ctx).Info("updateFieldRawDataSize done", zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *LocalSegment) CreateTextIndex(ctx context.Context, fieldID int64) error {
	var status C.CStatus
	log.Ctx(ctx).Info("create text index for segment", zap.Int64("segmentID", s.ID()), zap.Int64("fieldID", fieldID))

	GetLoadPool().Submit(func() (any, error) {
		status = C.CreateTextIndex(s.ptr, C.int64_t(fieldID))
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "CreateTextIndex failed"); err != nil {
		return err
	}

	log.Ctx(ctx).Info("create text index for segment done", zap.Int64("segmentID", s.ID()), zap.Int64("fieldID", fieldID))

	return nil
}

type ReleaseScope int

const (
	ReleaseScopeAll ReleaseScope = iota
	ReleaseScopeData
)

type releaseOptions struct {
	Scope ReleaseScope
}

func newReleaseOptions() *releaseOptions {
	return &releaseOptions{
		Scope: ReleaseScopeAll,
	}
}

type releaseOption func(*releaseOptions)

func WithReleaseScope(scope ReleaseScope) releaseOption {
	return func(options *releaseOptions) {
		options.Scope = scope
	}
}

func (s *LocalSegment) Release(ctx context.Context, opts ...releaseOption) {
	options := newReleaseOptions()
	for _, opt := range opts {
		opt(options)
	}
	stateLockGuard := s.startRelease(options.Scope)
	if stateLockGuard == nil { // release is already done.
		return
	}
	// release will never fail
	defer stateLockGuard.Done(nil)

	log := log.Ctx(ctx).With(zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.String("segmentType", s.segmentType.String()),
		zap.Int64("insertCount", s.InsertCount()),
	)

	// wait all read ops finished
	ptr := s.ptr
	if options.Scope == ReleaseScopeData {
		s.ReleaseSegmentData()
		log.Info("release segment data done and the field indexes info has been set lazy load=true")
		return
	}

	GetDynamicPool().Submit(func() (any, error) {
		C.DeleteSegment(ptr)
		return nil, nil
	}).Await()

	log.Info("delete segment from memory")
}

// ReleaseSegmentData releases the segment data.
func (s *LocalSegment) ReleaseSegmentData() {
	GetDynamicPool().Submit(func() (any, error) {
		C.ClearSegmentData(s.ptr)
		return nil, nil
	}).Await()
	for _, indexInfo := range s.Indexes() {
		indexInfo.IsLoaded = false
	}
}

// StartLoadData starts the loading process of the segment.
func (s *LocalSegment) StartLoadData() (state.LoadStateLockGuard, error) {
	return s.ptrLock.StartLoadData()
}

// startRelease starts the releasing process of the segment.
func (s *LocalSegment) startRelease(scope ReleaseScope) state.LoadStateLockGuard {
	switch scope {
	case ReleaseScopeData:
		return s.ptrLock.StartReleaseData()
	case ReleaseScopeAll:
		return s.ptrLock.StartReleaseAll()
	default:
		panic(fmt.Sprintf("unexpected release scope %d", scope))
	}
}

func (s *LocalSegment) RemoveFieldFile(fieldId int64) {
	GetDynamicPool().Submit(func() (any, error) {
		C.RemoveFieldFile(s.ptr, C.int64_t(fieldId))
		return nil, nil
	}).Await()
}

func (s *LocalSegment) RemoveUnusedFieldFiles() error {
	schema := s.collection.Schema()
	indexInfos, _ := separateIndexAndBinlog(s.LoadInfo())
	for _, indexInfo := range indexInfos {
		need, err := s.indexNeedLoadRawData(schema, indexInfo)
		if err != nil {
			return err
		}
		if !need {
			s.RemoveFieldFile(indexInfo.IndexInfo.FieldID)
		}
	}
	return nil
}

func (s *LocalSegment) indexNeedLoadRawData(schema *schemapb.CollectionSchema, indexInfo *IndexedFieldInfo) (bool, error) {
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return false, err
	}
	fieldSchema, err := schemaHelper.GetFieldFromID(indexInfo.IndexInfo.FieldID)
	if err != nil {
		return false, err
	}
	return !typeutil.IsVectorType(fieldSchema.DataType) && s.HasRawData(indexInfo.IndexInfo.FieldID), nil
}

type (
	WarmupTask            = func() (any, error)
	AsyncWarmupDispatcher struct {
		mu     sync.RWMutex
		tasks  []WarmupTask
		notify chan struct{}
	}
)

func NewWarmupDispatcher() *AsyncWarmupDispatcher {
	return &AsyncWarmupDispatcher{
		notify: make(chan struct{}, 1),
	}
}

func (d *AsyncWarmupDispatcher) AddTask(task func() (any, error)) {
	d.mu.Lock()
	d.tasks = append(d.tasks, task)
	d.mu.Unlock()
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

func (d *AsyncWarmupDispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.notify:
			d.mu.RLock()
			tasks := make([]WarmupTask, len(d.tasks))
			copy(tasks, d.tasks)
			d.mu.RUnlock()

			for _, task := range tasks {
				select {
				case <-ctx.Done():
					return
				default:
					GetWarmupPool().Submit(task)
				}
			}

			d.mu.Lock()
			d.tasks = d.tasks[len(tasks):]
			d.mu.Unlock()
		}
	}
}
