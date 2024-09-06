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
	"runtime"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/cgopb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
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
	channel        metautil.Channel

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
		channel:        channel,
		isLazyLoad:     isLazyLoad(collection, segmentType),

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
	s.bloomFilterSet.UpdateBloomFilter(pks)
}

// MayPkExist returns true if the given PK exists in the PK range and being positive through the bloom filter,
// false otherwise,
// may returns true even the PK doesn't exist actually
func (s *baseSegment) MayPkExist(pk *storage.LocationsCache) bool {
	return s.bloomFilterSet.MayPkExist(pk)
}

func (s *baseSegment) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
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
	ptr     C.CSegmentInterface

	// cached results, to avoid too many CGO calls
	memSize     *atomic.Int64
	rowNum      *atomic.Int64
	insertCount *atomic.Int64

	lastDeltaTimestamp *atomic.Uint64
	fields             *typeutil.ConcurrentMap[int64, *FieldInfo]
	fieldIndexes       *typeutil.ConcurrentMap[int64, *IndexedFieldInfo]
}

func NewSegment(ctx context.Context,
	collection *Collection,
	segmentType SegmentType,
	version int64,
	loadInfo *querypb.SegmentLoadInfo,
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

	var cSegType C.SegmentType
	var locker *state.LoadStateLock
	switch segmentType {
	case SegmentTypeSealed:
		cSegType = C.Sealed
		locker = state.NewLoadStateLock(state.LoadStateOnlyMeta)
	case SegmentTypeGrowing:
		locker = state.NewLoadStateLock(state.LoadStateDataLoaded)
		cSegType = C.Growing
	default:
		return nil, fmt.Errorf("illegal segment type %d when create segment %d", segmentType, loadInfo.GetSegmentID())
	}

	var newPtr C.CSegmentInterface
	_, err = GetDynamicPool().Submit(func() (any, error) {
		status := C.NewSegment(collection.collectionPtr, cSegType, C.int64_t(loadInfo.GetSegmentID()), &newPtr, C.bool(loadInfo.GetIsSorted()))
		err := HandleCStatus(ctx, &status, "NewSegmentFailed",
			zap.Int64("collectionID", loadInfo.GetCollectionID()),
			zap.Int64("partitionID", loadInfo.GetPartitionID()),
			zap.Int64("segmentID", loadInfo.GetSegmentID()),
			zap.String("segmentType", segmentType.String()))
		return nil, err
	}).Await()
	if err != nil {
		return nil, err
	}

	log.Info("create segment",
		zap.Int64("collectionID", loadInfo.GetCollectionID()),
		zap.Int64("partitionID", loadInfo.GetPartitionID()),
		zap.Int64("segmentID", loadInfo.GetSegmentID()),
		zap.String("segmentType", segmentType.String()),
		zap.String("level", loadInfo.GetLevel().String()),
	)

	segment := &LocalSegment{
		baseSegment:        base,
		ptrLock:            locker,
		ptr:                newPtr,
		lastDeltaTimestamp: atomic.NewUint64(0),
		fields:             typeutil.NewConcurrentMap[int64, *FieldInfo](),
		fieldIndexes:       typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),

		memSize:     atomic.NewInt64(-1),
		rowNum:      atomic.NewInt64(-1),
		insertCount: atomic.NewInt64(0),
	}

	if err := segment.initializeSegment(); err != nil {
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
	if !s.ptrLock.PinIfNotReleased() {
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
	if !s.ptrLock.RLockIf(state.IsDataLoaded) {
		return 0
	}
	defer s.ptrLock.RUnlock()

	rowNum := s.rowNum.Load()
	if rowNum < 0 {
		var rowCount C.int64_t
		GetDynamicPool().Submit(func() (any, error) {
			rowCount = C.GetRealCount(s.ptr)
			s.rowNum.Store(int64(rowCount))
			return nil, nil
		}).Await()
		rowNum = int64(rowCount)
	}

	return rowNum
}

func (s *LocalSegment) MemSize() int64 {
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return 0
	}
	defer s.ptrLock.RUnlock()

	memSize := s.memSize.Load()
	if memSize < 0 {
		var cMemSize C.int64_t
		GetDynamicPool().Submit(func() (any, error) {
			cMemSize = C.GetMemoryUsageInBytes(s.ptr)
			s.memSize.Store(int64(cMemSize))
			return nil, nil
		}).Await()

		memSize = int64(cMemSize)
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
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return false
	}
	defer s.ptrLock.RUnlock()

	ret := C.HasRawData(s.ptr, C.int64_t(fieldID))
	return bool(ret)
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

func (s *LocalSegment) Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error) {
	/*
		CStatus
		Search(void* plan,
			void* placeholder_groups,
			uint64_t* timestamps,
			int num_groups,
			long int* result_ids,
			float* result_distances);
	*/
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("segmentID", s.ID()),
		zap.String("segmentType", s.segmentType.String()),
	)
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(searchReq)

	hasIndex := s.ExistIndex(searchReq.searchFieldID)
	log = log.With(zap.Bool("withIndex", hasIndex))
	log.Debug("search segment...")

	tr := timerecord.NewTimeRecorder("cgoSearch")

	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncSearch(
				traceCtx.ctx,
				s.ptr,
				searchReq.plan.cSearchPlan,
				searchReq.cPlaceholderGroup,
				C.uint64_t(searchReq.mvccTimestamp),
			))
		},
		cgo.WithName("search"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		log.Warn("Search failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Debug("search segment done")
	return &SearchResult{
		cSearchResult: (C.CSearchResult)(result),
	}, nil
}

func (s *LocalSegment) Retrieve(ctx context.Context, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("msgID", plan.msgID),
		zap.String("segmentType", s.segmentType.String()),
	)
	log.Debug("begin to retrieve")

	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)

	maxLimitSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	tr := timerecord.NewTimeRecorder("cgoRetrieve")

	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncRetrieve(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				C.uint64_t(plan.Timestamp),
				C.int64_t(maxLimitSize),
				C.bool(plan.ignoreNonPk),
			))
		},
		cgo.WithName("retrieve"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		log.Warn("Retrieve failed")
		return nil, err
	}
	defer C.DeleteRetrieveResult((*C.CRetrieveResult)(result))

	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))

	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "partial-segcore-results-deserialization")
	defer span.End()

	retrieveResult := new(segcorepb.RetrieveResults)
	if err := UnmarshalCProto((*C.CRetrieveResult)(result), retrieveResult); err != nil {
		log.Warn("unmarshal retrieve result failed", zap.Error(err))
		return nil, err
	}

	log.Debug("retrieve segment done",
		zap.Int("resultNum", len(retrieveResult.Offset)),
	)
	// Sort was done by the segcore.
	// sort.Sort(&byPK{result})
	return retrieveResult, nil
}

func (s *LocalSegment) RetrieveByOffsets(ctx context.Context, plan *RetrievePlan, offsets []int64) (*segcorepb.RetrieveResults, error) {
	if len(offsets) == 0 {
		return nil, merr.WrapErrParameterInvalid("segment offsets", "empty offsets")
	}

	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	fields := []zap.Field{
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("msgID", plan.msgID),
		zap.String("segmentType", s.segmentType.String()),
		zap.Int("resultNum", len(offsets)),
	}

	log := log.Ctx(ctx).With(fields...)
	log.Debug("begin to retrieve by offsets")
	tr := timerecord.NewTimeRecorder("cgoRetrieveByOffsets")
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)
	defer runtime.KeepAlive(offsets)

	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncRetrieveByOffsets(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				(*C.int64_t)(unsafe.Pointer(&offsets[0])),
				C.int64_t(len(offsets)),
			))
		},
		cgo.WithName("retrieve-by-offsets"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		log.Warn("RetrieveByOffsets failed")
		return nil, err
	}
	defer C.DeleteRetrieveResult((*C.CRetrieveResult)(result))

	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))

	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "reduced-segcore-results-deserialization")
	defer span.End()

	retrieveResult := new(segcorepb.RetrieveResults)
	if err := UnmarshalCProto((*C.CRetrieveResult)(result), retrieveResult); err != nil {
		log.Warn("unmarshal retrieve by offsets result failed", zap.Error(err))
		return nil, err
	}

	log.Debug("retrieve by segment offsets done",
		zap.Int("resultNum", len(retrieveResult.Offset)),
	)
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

// -------------------------------------------------------------------------------------- interfaces for growing segment
func (s *LocalSegment) preInsert(ctx context.Context, numOfRecords int) (int64, error) {
	/*
		long int
		PreInsert(CSegmentInterface c_segment, long int size);
	*/
	var offset int64
	cOffset := (*C.int64_t)(&offset)

	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.PreInsert(s.ptr, C.int64_t(int64(numOfRecords)), cOffset)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "PreInsert failed"); err != nil {
		return 0, err
	}
	return offset, nil
}

func (s *LocalSegment) Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error {
	if s.Type() != SegmentTypeGrowing {
		return fmt.Errorf("unexpected segmentType when segmentInsert, segmentType = %s", s.segmentType.String())
	}
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	offset, err := s.preInsert(ctx, len(rowIDs))
	if err != nil {
		return err
	}

	insertRecordBlob, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal insert record: %s", err)
	}

	numOfRow := len(rowIDs)
	cOffset := C.int64_t(offset)
	cNumOfRows := C.int64_t(numOfRow)
	cEntityIDsPtr := (*C.int64_t)(&(rowIDs)[0])
	cTimestampsPtr := (*C.uint64_t)(&(timestamps)[0])

	var status C.CStatus

	GetDynamicPool().Submit(func() (any, error) {
		status = C.Insert(s.ptr,
			cOffset,
			cNumOfRows,
			cEntityIDsPtr,
			cTimestampsPtr,
			(*C.uint8_t)(unsafe.Pointer(&insertRecordBlob[0])),
			(C.uint64_t)(len(insertRecordBlob)),
		)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "Insert failed"); err != nil {
		return err
	}

	s.insertCount.Add(int64(numOfRow))
	s.rowNum.Store(-1)
	s.memSize.Store(-1)
	return nil
}

func (s *LocalSegment) Delete(ctx context.Context, primaryKeys []storage.PrimaryKey, timestamps []typeutil.Timestamp) error {
	/*
		CStatus
		Delete(CSegmentInterface c_segment,
		           long int reserved_offset,
		           long size,
		           const long* primary_keys,
		           const unsigned long* timestamps);
	*/

	if len(primaryKeys) == 0 {
		return nil
	}
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	cOffset := C.int64_t(0) // depre
	cSize := C.int64_t(len(primaryKeys))
	cTimestampsPtr := (*C.uint64_t)(&(timestamps)[0])

	ids := &schemapb.IDs{}
	pkType := primaryKeys[0].Type()
	switch pkType {
	case schemapb.DataType_Int64:
		int64Pks := make([]int64, len(primaryKeys))
		for index, pk := range primaryKeys {
			int64Pks[index] = pk.(*storage.Int64PrimaryKey).Value
		}
		ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Pks,
			},
		}
	case schemapb.DataType_VarChar:
		varCharPks := make([]string, len(primaryKeys))
		for index, entity := range primaryKeys {
			varCharPks[index] = entity.(*storage.VarCharPrimaryKey).Value
		}
		ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: varCharPks,
			},
		}
	default:
		return fmt.Errorf("invalid data type of primary keys")
	}

	dataBlob, err := proto.Marshal(ids)
	if err != nil {
		return fmt.Errorf("failed to marshal ids: %s", err)
	}
	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.Delete(s.ptr,
			cOffset,
			cSize,
			(*C.uint8_t)(unsafe.Pointer(&dataBlob[0])),
			(C.uint64_t)(len(dataBlob)),
			cTimestampsPtr,
		)
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "Delete failed"); err != nil {
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

	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	loadFieldDataInfo, err := newLoadFieldDataInfo(ctx)
	defer deleteFieldDataInfo(loadFieldDataInfo)
	if err != nil {
		return err
	}

	for _, field := range fields {
		fieldID := field.FieldID
		err = loadFieldDataInfo.appendLoadFieldInfo(ctx, fieldID, rowCount)
		if err != nil {
			return err
		}

		for _, binlog := range field.Binlogs {
			err = loadFieldDataInfo.appendLoadFieldDataPath(ctx, fieldID, binlog)
			if err != nil {
				return err
			}
		}

		loadFieldDataInfo.appendMMapDirPath(paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue())
	}

	var status C.CStatus
	GetLoadPool().Submit(func() (any, error) {
		status = C.LoadFieldData(s.ptr, loadFieldDataInfo.cLoadFieldDataInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "LoadMultiFieldData failed",
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID())); err != nil {
		return err
	}

	log.Info("load mutil field done",
		zap.Int64("row count", rowCount),
		zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *LocalSegment) LoadFieldData(ctx context.Context, fieldID int64, rowCount int64, field *datapb.FieldBinlog) error {
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

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

	loadFieldDataInfo, err := newLoadFieldDataInfo(ctx)
	defer deleteFieldDataInfo(loadFieldDataInfo)
	if err != nil {
		return err
	}

	err = loadFieldDataInfo.appendLoadFieldInfo(ctx, fieldID, rowCount)
	if err != nil {
		return err
	}

	if field != nil {
		for _, binlog := range field.Binlogs {
			err = loadFieldDataInfo.appendLoadFieldDataPath(ctx, fieldID, binlog)
			if err != nil {
				return err
			}
		}
	}

	// TODO retrieve_enable should be considered
	collection := s.collection
	fieldSchema, err := getFieldSchema(collection.Schema(), fieldID)
	if err != nil {
		return err
	}
	mmapEnabled := isDataMmapEnable(fieldSchema)
	loadFieldDataInfo.appendMMapDirPath(paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue())
	loadFieldDataInfo.enableMmap(fieldID, mmapEnabled)

	var status C.CStatus
	GetLoadPool().Submit(func() (any, error) {
		log.Info("submitted loadFieldData task to load pool")
		status = C.LoadFieldData(s.ptr, loadFieldDataInfo.cLoadFieldDataInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "LoadFieldData failed",
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", fieldID)); err != nil {
		return err
	}

	log.Info("load field done")

	return nil
}

func (s *LocalSegment) AddFieldDataInfo(ctx context.Context, rowCount int64, fields []*datapb.FieldBinlog) error {
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("row count", rowCount),
	)

	loadFieldDataInfo, err := newLoadFieldDataInfo(ctx)
	if err != nil {
		return err
	}
	defer deleteFieldDataInfo(loadFieldDataInfo)

	for _, field := range fields {
		fieldID := field.FieldID
		err = loadFieldDataInfo.appendLoadFieldInfo(ctx, fieldID, rowCount)
		if err != nil {
			return err
		}

		for _, binlog := range field.Binlogs {
			err = loadFieldDataInfo.appendLoadFieldDataPath(ctx, fieldID, binlog)
			if err != nil {
				return err
			}
		}
	}

	var status C.CStatus
	GetLoadPool().Submit(func() (any, error) {
		status = C.AddFieldDataInfoForSealed(s.ptr, loadFieldDataInfo.cLoadFieldDataInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(ctx, &status, "AddFieldDataInfo failed",
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID())); err != nil {
		return err
	}

	log.Info("add field data info done")
	return nil
}

func (s *LocalSegment) LoadDeltaData(ctx context.Context, deltaData *storage.DeleteData) error {
	pks, tss := deltaData.Pks, deltaData.Tss
	rowNum := deltaData.RowCount

	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	pkType := pks[0].Type()
	ids := &schemapb.IDs{}
	switch pkType {
	case schemapb.DataType_Int64:
		int64Pks := make([]int64, len(pks))
		for index, pk := range pks {
			int64Pks[index] = pk.(*storage.Int64PrimaryKey).Value
		}
		ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Pks,
			},
		}
	case schemapb.DataType_VarChar:
		varCharPks := make([]string, len(pks))
		for index, pk := range pks {
			varCharPks[index] = pk.(*storage.VarCharPrimaryKey).Value
		}
		ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: varCharPks,
			},
		}
	default:
		return fmt.Errorf("invalid data type of primary keys")
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
	// 1.
	loadIndexInfo, err := newLoadIndexInfo(ctx)
	if err != nil {
		return err
	}
	defer deleteLoadIndexInfo(loadIndexInfo)

	schema, err := typeutil.CreateSchemaHelper(s.GetCollection().Schema())
	if err != nil {
		return err
	}
	fieldSchema, err := schema.GetFieldFromID(indexInfo.GetFieldID())
	if err != nil {
		return err
	}

	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	// as Knowhere reports error if encounter an unknown param, we need to delete it
	delete(indexParams, common.MmapEnabledKey)

	// some build params also exist in indexParams, which are useless during loading process
	if indexParams["index_type"] == indexparamcheck.IndexDISKANN {
		if err := indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows()); err != nil {
			return err
		}
	}

	if err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams); err != nil {
		return err
	}

	enableMmap := isIndexMmapEnable(fieldSchema, indexInfo)

	indexInfoProto := &cgopb.LoadIndexInfo{
		CollectionID:       s.Collection(),
		PartitionID:        s.Partition(),
		SegmentID:          s.ID(),
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
	}

	newLoadIndexInfoSpan := tr.RecordSpan()

	// 2.
	if err := loadIndexInfo.finish(ctx, indexInfoProto); err != nil {
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
	err = s.UpdateIndexInfo(ctx, indexInfo, loadIndexInfo)
	if err != nil {
		return err
	}
	updateIndexInfoSpan := tr.RecordSpan()
	if !typeutil.IsVectorType(fieldType) || s.HasRawData(indexInfo.GetFieldID()) {
		return nil
	}

	// 4.
	s.WarmupChunkCache(ctx, indexInfo.GetFieldID(), isDataMmapEnable(fieldSchema))
	warmupChunkCacheSpan := tr.RecordSpan()
	log.Info("Finish loading index",
		zap.Duration("newLoadIndexInfoSpan", newLoadIndexInfoSpan),
		zap.Duration("appendLoadIndexInfoSpan", appendLoadIndexInfoSpan),
		zap.Duration("updateIndexInfoSpan", updateIndexInfoSpan),
		zap.Duration("warmupChunkCacheSpan", warmupChunkCacheSpan),
	)
	return nil
}

func (s *LocalSegment) UpdateIndexInfo(ctx context.Context, indexInfo *querypb.FieldIndexInfo, info *LoadIndexInfo) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("fieldID", indexInfo.FieldID),
	)
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.RUnlock()

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
	if !s.ptrLock.RLockIf(state.IsNotReleased) {
		return
	}
	defer s.ptrLock.RUnlock()

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
		GetWarmupPool().Submit(func() (any, error) {
			// bad implemtation, warmup is async at another goroutine and hold the rlock.
			// the state transition of segment in segment loader will blocked.
			// add a waiter to avoid it.
			s.ptrLock.BlockUntilDataLoadedOrReleased()
			if !s.ptrLock.RLockIf(state.IsNotReleased) {
				return nil, nil
			}
			defer s.ptrLock.RUnlock()

			cFieldID := C.int64_t(fieldID)
			cMmapEnabled := C.bool(mmapEnabled)
			status = C.WarmupChunkCache(s.ptr, cFieldID, cMmapEnabled)
			if err := HandleCStatus(ctx, &status, ""); err != nil {
				log.Warn("warming up chunk cache asynchronously failed", zap.Error(err))
				return nil, err
			}
			log.Info("warming up chunk cache asynchronously done")
			return nil, nil
		})
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

	C.DeleteSegment(ptr)

	localDiskUsage, err := GetLocalUsedSize(context.Background(), paramtable.Get().LocalStorageCfg.Path.GetValue())
	// ignore error here, shall not block releasing
	if err == nil {
		metrics.QueryNodeDiskUsedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(localDiskUsage) / 1024 / 1024) // in MB
	}

	log.Info("delete segment from memory")
}

// ReleaseSegmentData releases the segment data.
func (s *LocalSegment) ReleaseSegmentData() {
	C.ClearSegmentData(s.ptr)
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
	C.RemoveFieldFile(s.ptr, C.int64_t(fieldId))
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
