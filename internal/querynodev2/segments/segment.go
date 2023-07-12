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
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	pkoracle "github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SegmentType = commonpb.SegmentState

const (
	SegmentTypeGrowing = commonpb.SegmentState_Growing
	SegmentTypeSealed  = commonpb.SegmentState_Sealed
)

var (
	ErrSegmentUnhealthy = errors.New("segment unhealthy")
)

// IndexedFieldInfo contains binlog info of vector field
type IndexedFieldInfo struct {
	FieldBinlog *datapb.FieldBinlog
	IndexInfo   *querypb.FieldIndexInfo
}

type Segment interface {
	// Properties
	ID() int64
	Collection() int64
	Partition() int64
	Shard() string
	Version() int64
	StartPosition() *msgpb.MsgPosition
	Type() SegmentType

	// Stats related
	// InsertCount returns the number of inserted rows, not effected by deletion
	InsertCount() int64
	// RowNum returns the number of rows, it's slow, so DO NOT call it in a loop
	RowNum() int64
	MemSize() int64

	// Index related
	AddIndex(fieldID int64, index *IndexedFieldInfo)
	GetIndex(fieldID int64) *IndexedFieldInfo
	ExistIndex(fieldID int64) bool
	Indexes() []*IndexedFieldInfo

	// Modification related
	Insert(rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error
	Delete(primaryKeys []storage.PrimaryKey, timestamps []typeutil.Timestamp) error
	LastDeltaTimestamp() uint64

	// Bloom filter related
	UpdateBloomFilter(pks []storage.PrimaryKey)
	MayPkExist(pk storage.PrimaryKey) bool
}

type baseSegment struct {
	segmentID      int64
	partitionID    int64
	shard          string
	collectionID   int64
	typ            SegmentType
	version        int64
	startPosition  *msgpb.MsgPosition // for growing segment release
	bloomFilterSet *pkoracle.BloomFilterSet
}

func newBaseSegment(id, partitionID, collectionID int64, shard string, typ SegmentType, version int64, startPosition *msgpb.MsgPosition) baseSegment {
	return baseSegment{
		segmentID:      id,
		partitionID:    partitionID,
		collectionID:   collectionID,
		shard:          shard,
		typ:            typ,
		version:        version,
		startPosition:  startPosition,
		bloomFilterSet: pkoracle.NewBloomFilterSet(id, partitionID, typ),
	}
}

// ID returns the identity number.
func (s *baseSegment) ID() int64 {
	return s.segmentID
}

func (s *baseSegment) Collection() int64 {
	return s.collectionID
}

func (s *baseSegment) Partition() int64 {
	return s.partitionID
}

func (s *baseSegment) Shard() string {
	return s.shard
}

func (s *baseSegment) Type() SegmentType {
	return s.typ
}

func (s *baseSegment) StartPosition() *msgpb.MsgPosition {
	return s.startPosition
}

func (s *baseSegment) Version() int64 {
	return s.version
}

func (s *baseSegment) UpdateBloomFilter(pks []storage.PrimaryKey) {
	s.bloomFilterSet.UpdateBloomFilter(pks)
}

// MayPkExist returns true if the given PK exists in the PK range and being positive through the bloom filter,
// false otherwise,
// may returns true even the PK doesn't exist actually
func (s *baseSegment) MayPkExist(pk storage.PrimaryKey) bool {
	return s.bloomFilterSet.MayPkExist(pk)
}

var _ Segment = (*LocalSegment)(nil)

// Segment is a wrapper of the underlying C-structure segment.
type LocalSegment struct {
	baseSegment
	mut sync.RWMutex // protects segmentPtr
	ptr C.CSegmentInterface

	size               int64
	row                int64
	lastDeltaTimestamp *atomic.Uint64
	fieldIndexes       *typeutil.ConcurrentMap[int64, *IndexedFieldInfo]
}

func NewSegment(collection *Collection,
	segmentID int64,
	partitionID int64,
	collectionID int64,
	shard string,
	segmentType SegmentType,
	version int64,
	startPosition *msgpb.MsgPosition,
	deltaPosition *msgpb.MsgPosition,
) (*LocalSegment, error) {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/
	var segmentPtr C.CSegmentInterface
	switch segmentType {
	case SegmentTypeSealed:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.Sealed, C.int64_t(segmentID))
	case SegmentTypeGrowing:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.Growing, C.int64_t(segmentID))
	default:
		return nil, fmt.Errorf("illegal segment type %d when create segment %d", segmentType, segmentID)
	}

	log.Info("create segment",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
		zap.String("segmentType", segmentType.String()))

	var segment = &LocalSegment{
		baseSegment:        newBaseSegment(segmentID, partitionID, collectionID, shard, segmentType, version, startPosition),
		ptr:                segmentPtr,
		lastDeltaTimestamp: atomic.NewUint64(deltaPosition.GetTimestamp()),
		fieldIndexes:       typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),
	}

	return segment, nil
}

func (s *LocalSegment) isValid() bool {
	return s.ptr != nil
}

func (s *LocalSegment) InsertCount() int64 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if !s.isValid() {
		return 0
	}
	var rowCount C.int64_t
	GetPool().Submit(func() (any, error) {
		rowCount = C.GetRowCount(s.ptr)
		return nil, nil
	}).Await()

	return int64(rowCount)
}

func (s *LocalSegment) RowNum() int64 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if !s.isValid() {
		return 0
	}
	var rowCount C.int64_t
	GetPool().Submit(func() (any, error) {
		rowCount = C.GetRealCount(s.ptr)
		return nil, nil
	}).Await()

	return int64(rowCount)
}

func (s *LocalSegment) MemSize() int64 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if !s.isValid() {
		return 0
	}
	var memoryUsageInBytes C.int64_t
	GetPool().Submit(func() (any, error) {
		memoryUsageInBytes = C.GetMemoryUsageInBytes(s.ptr)
		return nil, nil
	}).Await()

	return int64(memoryUsageInBytes)
}

func (s *LocalSegment) LastDeltaTimestamp() uint64 {
	return s.lastDeltaTimestamp.Load()
}

func (s *LocalSegment) AddIndex(fieldID int64, info *IndexedFieldInfo) {
	s.fieldIndexes.Insert(fieldID, info)
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
	return fieldInfo.IndexInfo != nil && fieldInfo.IndexInfo.EnableIndex
}

func (s *LocalSegment) HasRawData(fieldID int64) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.isValid() {
		return false
	}
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

func (s *LocalSegment) Type() SegmentType {
	return s.typ
}

func DeleteSegment(segment *LocalSegment) {
	/*
		void
		deleteSegment(CSegmentInterface segment);
	*/
	// wait all read ops finished
	var ptr C.CSegmentInterface

	segment.mut.Lock()
	ptr = segment.ptr
	segment.ptr = nil
	segment.mut.Unlock()

	if ptr == nil {
		return
	}

	C.DeleteSegment(ptr)
	log.Info("delete segment from memory",
		zap.Int64("collectionID", segment.collectionID),
		zap.Int64("partitionID", segment.partitionID),
		zap.Int64("segmentID", segment.ID()),
		zap.String("segmentType", segment.typ.String()))
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
		zap.String("segmentType", s.typ.String()),
	)
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return nil, merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	spanID := span.SpanContext().SpanID()
	traceCtx := C.CTraceContext{
		traceID: (*C.uint8_t)(unsafe.Pointer(&traceID[0])),
		spanID:  (*C.uint8_t)(unsafe.Pointer(&spanID[0])),
		flag:    C.uchar(span.SpanContext().TraceFlags()),
	}

	hasIndex := s.ExistIndex(searchReq.searchFieldID)
	log = log.With(zap.Bool("withIndex", hasIndex))
	log.Debug("search segment...")

	var searchResult SearchResult
	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		tr := timerecord.NewTimeRecorder("cgoSearch")
		status = C.Search(s.ptr,
			searchReq.plan.cSearchPlan,
			searchReq.cPlaceholderGroup,
			traceCtx,
			C.uint64_t(searchReq.timestamp),
			&searchResult.cSearchResult,
		)
		metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return nil, nil
	}).Await()
	if err := HandleCStatus(&status, "Search failed"); err != nil {
		return nil, err
	}
	log.Debug("search segment done")
	return &searchResult, nil
}

func (s *LocalSegment) Retrieve(ctx context.Context, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return nil, merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	log := log.With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
		zap.Int64("msgID", plan.msgID),
		zap.String("segmentType", s.typ.String()),
	)

	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	spanID := span.SpanContext().SpanID()
	traceCtx := C.CTraceContext{
		traceID: (*C.uint8_t)(unsafe.Pointer(&traceID[0])),
		spanID:  (*C.uint8_t)(unsafe.Pointer(&spanID[0])),
		flag:    C.uchar(span.SpanContext().TraceFlags()),
	}

	var retrieveResult RetrieveResult
	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		ts := C.uint64_t(plan.Timestamp)
		tr := timerecord.NewTimeRecorder("cgoRetrieve")
		status = C.Retrieve(s.ptr,
			plan.cRetrievePlan,
			traceCtx,
			ts,
			&retrieveResult.cRetrieveResult,
		)
		metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("cgo retrieve done", zap.Duration("timeTaken", tr.ElapseSpan()))
		return nil, nil
	}).Await()

	if err := HandleCStatus(&status, "Retrieve failed"); err != nil {
		return nil, err
	}

	result := new(segcorepb.RetrieveResults)
	if err := HandleCProto(&retrieveResult.cRetrieveResult, result); err != nil {
		return nil, err
	}

	log.Debug("retrieve segment done",
		zap.Int("resultNum", len(result.Offset)),
	)

	sort.Sort(&byPK{result})
	return result, nil
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

func (s *LocalSegment) ValidateIndexedFieldsData(ctx context.Context, result *segcorepb.RetrieveResults) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	for _, fieldData := range result.FieldsData {
		if !typeutil.IsVectorType(fieldData.GetType()) {
			continue
		}
		if !s.ExistIndex(fieldData.FieldId) {
			continue
		}
		if !s.HasRawData(fieldData.FieldId) {
			index := s.GetIndex(fieldData.FieldId)
			indexType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.IndexTypeKey, index.IndexInfo.GetIndexParams())
			if err != nil {
				return err
			}
			err = fmt.Errorf("vector output fields for %s index is not allowed", indexType)
			log.Warn("validate fields failed", zap.Error(err))
			return err
		}
	}

	return nil
}

// -------------------------------------------------------------------------------------- interfaces for growing segment
func (s *LocalSegment) preInsert(numOfRecords int) (int64, error) {
	/*
		long int
		PreInsert(CSegmentInterface c_segment, long int size);
	*/
	var offset int64
	cOffset := (*C.int64_t)(&offset)

	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		status = C.PreInsert(s.ptr, C.int64_t(int64(numOfRecords)), cOffset)
		return nil, nil
	}).Await()
	if err := HandleCStatus(&status, "PreInsert failed"); err != nil {
		return 0, err
	}
	return offset, nil
}

func (s *LocalSegment) Insert(rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error {
	if s.Type() != SegmentTypeGrowing {
		return fmt.Errorf("unexpected segmentType when segmentInsert, segmentType = %s", s.typ.String())
	}

	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	offset, err := s.preInsert(len(rowIDs))
	if err != nil {
		return err
	}

	insertRecordBlob, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal insert record: %s", err)
	}

	var numOfRow = len(rowIDs)
	var cOffset = C.int64_t(offset)
	var cNumOfRows = C.int64_t(numOfRow)
	var cEntityIdsPtr = (*C.int64_t)(&(rowIDs)[0])
	var cTimestampsPtr = (*C.uint64_t)(&(timestamps)[0])

	var status C.CStatus

	GetPool().Submit(func() (any, error) {
		status = C.Insert(s.ptr,
			cOffset,
			cNumOfRows,
			cEntityIdsPtr,
			cTimestampsPtr,
			(*C.uint8_t)(unsafe.Pointer(&insertRecordBlob[0])),
			(C.uint64_t)(len(insertRecordBlob)),
		)
		return nil, nil
	}).Await()
	if err := HandleCStatus(&status, "Insert failed"); err != nil {
		return err
	}
	metrics.QueryNodeNumEntities.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		fmt.Sprint(s.collectionID),
		fmt.Sprint(s.partitionID),
		s.Type().String(),
		fmt.Sprint(0),
	).Add(float64(numOfRow))
	return nil
}

func (s *LocalSegment) Delete(primaryKeys []storage.PrimaryKey, timestamps []typeutil.Timestamp) error {
	/*
		CStatus
		Delete(CSegmentInterface c_segment,
		           long int reserved_offset,
		           long size,
		           const long* primary_keys,
		           const unsigned long* timestamps);
	*/

	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	var cOffset = C.int64_t(0) // depre
	var cSize = C.int64_t(len(primaryKeys))
	var cTimestampsPtr = (*C.uint64_t)(&(timestamps)[0])

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
	GetPool().Submit(func() (any, error) {
		status = C.Delete(s.ptr,
			cOffset,
			cSize,
			(*C.uint8_t)(unsafe.Pointer(&dataBlob[0])),
			(C.uint64_t)(len(dataBlob)),
			cTimestampsPtr,
		)
		return nil, nil
	}).Await()

	if err := HandleCStatus(&status, "Delete failed"); err != nil {
		return err
	}

	s.lastDeltaTimestamp.Store(timestamps[len(timestamps)-1])

	return nil
}

// -------------------------------------------------------------------------------------- interfaces for sealed segment
func (s *LocalSegment) LoadMultiFieldData(rowCount int64, fields []*datapb.FieldBinlog) error {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	log := log.With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	loadFieldDataInfo, err := newLoadFieldDataInfo()
	defer deleteFieldDataInfo(loadFieldDataInfo)
	if err != nil {
		return err
	}

	for _, field := range fields {
		fieldID := field.FieldID
		err = loadFieldDataInfo.appendLoadFieldInfo(fieldID, rowCount)
		if err != nil {
			return err
		}

		for _, binlog := range field.Binlogs {
			err = loadFieldDataInfo.appendLoadFieldDataPath(fieldID, binlog.GetLogPath())
			if err != nil {
				return err
			}
		}

		loadFieldDataInfo.appendMMapDirPath(paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue())
	}

	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		status = C.LoadFieldData(s.ptr, loadFieldDataInfo.cLoadFieldDataInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(&status, "LoadMultiFieldData failed"); err != nil {
		return err
	}

	log.Info("load mutil field done",
		zap.Int64("row count", rowCount),
		zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *LocalSegment) LoadFieldData(fieldID int64, rowCount int64, field *datapb.FieldBinlog) error {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	log := log.With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	loadFieldDataInfo, err := newLoadFieldDataInfo()
	defer deleteFieldDataInfo(loadFieldDataInfo)
	if err != nil {
		return err
	}

	err = loadFieldDataInfo.appendLoadFieldInfo(fieldID, rowCount)
	if err != nil {
		return err
	}

	for _, binlog := range field.Binlogs {
		err = loadFieldDataInfo.appendLoadFieldDataPath(fieldID, binlog.GetLogPath())
		if err != nil {
			return err
		}
	}
	loadFieldDataInfo.appendMMapDirPath(paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue())

	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		status = C.LoadFieldData(s.ptr, loadFieldDataInfo.cLoadFieldDataInfo)
		return nil, nil
	}).Await()
	if err := HandleCStatus(&status, "LoadFieldData failed"); err != nil {
		return err
	}

	log.Info("load field done",
		zap.Int64("fieldID", fieldID),
		zap.Int64("row count", rowCount),
		zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *LocalSegment) LoadDeltaData(deltaData *storage.DeleteData) error {
	pks, tss := deltaData.Pks, deltaData.Tss
	rowNum := deltaData.RowCount

	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	log := log.With(
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
	GetPool().Submit(func() (any, error) {
		status = C.LoadDeletedRecord(s.ptr, loadInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(&status, "LoadDeletedRecord failed"); err != nil {
		return err
	}

	log.Info("load deleted record done",
		zap.Int64("rowNum", rowNum),
		zap.String("segmentType", s.Type().String()))
	return nil
}

func (s *LocalSegment) LoadIndex(bytesIndex [][]byte, indexInfo *querypb.FieldIndexInfo, fieldType schemapb.DataType) error {
	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}

	err = loadIndexInfo.appendLoadIndexInfo(bytesIndex, indexInfo, s.collectionID, s.partitionID, s.segmentID, fieldType)
	if err != nil {
		if loadIndexInfo.cleanLocalData() != nil {
			log.Warn("failed to clean cached data on disk after append index failed",
				zap.Int64("buildID", indexInfo.BuildID),
				zap.Int64("index version", indexInfo.IndexVersion))
		}
		return err
	}
	if s.Type() != SegmentTypeSealed {
		errMsg := fmt.Sprintln("updateSegmentIndex failed, illegal segment type ", s.typ, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	log := log.With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		status = C.UpdateSealedSegmentIndex(s.ptr, loadIndexInfo.cLoadIndexInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(&status, "UpdateSealedSegmentIndex failed"); err != nil {
		return err
	}

	log.Info("updateSegmentIndex done", zap.Int64("fieldID", indexInfo.FieldID))

	return nil
}

func (s *LocalSegment) LoadIndexData(indexInfo *querypb.FieldIndexInfo, fieldType schemapb.DataType) error {
	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}

	err = loadIndexInfo.appendLoadIndexInfo(nil, indexInfo, s.collectionID, s.partitionID, s.segmentID, fieldType)
	if err != nil {
		if loadIndexInfo.cleanLocalData() != nil {
			log.Warn("failed to clean cached data on disk after append index failed",
				zap.Int64("buildID", indexInfo.BuildID),
				zap.Int64("index version", indexInfo.IndexVersion))
		}
		return err
	}
	if s.Type() != SegmentTypeSealed {
		errMsg := fmt.Sprintln("updateSegmentIndex failed, illegal segment type ", s.typ, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.ptr == nil {
		return merr.WrapErrSegmentNotLoaded(s.segmentID, "segment released")
	}

	log := log.With(
		zap.Int64("collectionID", s.Collection()),
		zap.Int64("partitionID", s.Partition()),
		zap.Int64("segmentID", s.ID()),
	)

	var status C.CStatus
	GetPool().Submit(func() (any, error) {
		status = C.UpdateSealedSegmentIndex(s.ptr, loadIndexInfo.cLoadIndexInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(&status, "UpdateSealedSegmentIndex failed"); err != nil {
		return err
	}

	log.Info("updateSegmentIndex done", zap.Int64("fieldID", indexInfo.FieldID))

	return nil
}
