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

package querynode

/*
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"unsafe"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type segmentType = commonpb.SegmentState

const (
	segmentTypeGrowing = commonpb.SegmentState_Growing
	segmentTypeSealed  = commonpb.SegmentState_Sealed
)

var (
	ErrSegmentUnhealthy = errors.New("segment unhealthy")
)

// IndexedFieldInfo contains binlog info of vector field
type IndexedFieldInfo struct {
	fieldBinlog *datapb.FieldBinlog
	indexInfo   *querypb.FieldIndexInfo
}

// Segment is a wrapper of the underlying C-structure segment.
type Segment struct {
	mut        sync.RWMutex // protects segmentPtr
	segmentPtr C.CSegmentInterface

	segmentID     UniqueID
	partitionID   UniqueID
	collectionID  UniqueID
	version       UniqueID
	startPosition *msgpb.MsgPosition // for growing segment release

	vChannelID   Channel
	lastMemSize  int64
	lastRowCount int64

	recentlyModified  *atomic.Bool
	segmentType       *atomic.Int32
	destroyed         *atomic.Bool
	indexedFieldInfos *typeutil.ConcurrentMap[UniqueID, *IndexedFieldInfo]

	statLock sync.Mutex
	// only used by sealed segments
	currentStat  *storage.PkStatistics
	historyStats []*storage.PkStatistics
}

// ID returns the identity number.
func (s *Segment) ID() UniqueID {
	return s.segmentID
}

func (s *Segment) setRecentlyModified(modify bool) {
	s.recentlyModified.Store(modify)
}

func (s *Segment) getRecentlyModified() bool {
	return s.recentlyModified.Load()
}

func (s *Segment) setType(segType segmentType) {
	s.segmentType.Store(int32(segType))
}

func (s *Segment) getType() segmentType {
	return commonpb.SegmentState(s.segmentType.Load())
}

func (s *Segment) setIndexedFieldInfo(fieldID UniqueID, info *IndexedFieldInfo) {
	s.indexedFieldInfos.InsertIfNotPresent(fieldID, info)
}

func (s *Segment) getIndexedFieldInfo(fieldID UniqueID) (*IndexedFieldInfo, error) {
	info, ok := s.indexedFieldInfos.Get(fieldID)
	if !ok {
		return nil, fmt.Errorf("Invalid fieldID %d", fieldID)
	}
	return &IndexedFieldInfo{
		fieldBinlog: info.fieldBinlog,
		indexInfo:   info.indexInfo,
	}, nil
}

func (s *Segment) hasLoadIndexForIndexedField(fieldID int64) bool {
	fieldInfo, ok := s.indexedFieldInfos.Get(fieldID)
	if !ok {
		return false
	}
	return fieldInfo.indexInfo != nil && fieldInfo.indexInfo.EnableIndex
}

// healthy checks whether it's safe to use `segmentPtr`.
// shall acquire mut.RLock before check this flag.
func (s *Segment) healthy() bool {
	return !s.destroyed.Load()
}

func (s *Segment) setUnhealthy() {
	s.destroyed.Store(true)
}

func newSegment(collection *Collection,
	segmentID UniqueID,
	partitionID UniqueID,
	collectionID UniqueID,
	vChannelID Channel,
	segType segmentType,
	version UniqueID,
	startPosition *msgpb.MsgPosition) (*Segment, error) {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/
	var segmentPtr C.CSegmentInterface
	switch segType {
	case segmentTypeSealed:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.Sealed, C.int64_t(segmentID))
	case segmentTypeGrowing:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.Growing, C.int64_t(segmentID))
	default:
		err := fmt.Errorf("illegal segment type %d when create segment  %d", segType, segmentID)
		log.Warn("create new segment error",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID),
			zap.Int64("segmentID", segmentID),
			zap.String("segmentType", segType.String()),
			zap.Error(err))
		return nil, err
	}

	log.Info("create segment",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
		zap.String("segmentType", segType.String()))

	var segment = &Segment{
		segmentPtr:        segmentPtr,
		segmentType:       atomic.NewInt32(int32(segType)),
		segmentID:         segmentID,
		partitionID:       partitionID,
		collectionID:      collectionID,
		version:           version,
		startPosition:     startPosition,
		vChannelID:        vChannelID,
		indexedFieldInfos: typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),
		recentlyModified:  atomic.NewBool(false),
		destroyed:         atomic.NewBool(false),
		historyStats:      []*storage.PkStatistics{},
	}

	return segment, nil
}

func deleteSegment(segment *Segment) {
	/*
		void
		deleteSegment(CSegmentInterface segment);
	*/
	var cPtr C.CSegmentInterface
	// wait all read ops finished
	segment.mut.Lock()
	segment.setUnhealthy()
	cPtr = segment.segmentPtr
	segment.segmentPtr = nil
	segment.mut.Unlock()

	if cPtr == nil {
		return
	}

	C.DeleteSegment(cPtr)

	segment.currentStat = nil
	segment.historyStats = nil

	log.Info("delete segment from memory",
		zap.Int64("collectionID", segment.collectionID),
		zap.Int64("partitionID", segment.partitionID),
		zap.Int64("segmentID", segment.ID()),
		zap.String("segmentType", segment.getType().String()))
}

func (s *Segment) getRealCount() int64 {
	/*
		int64_t
		GetRealCount(CSegmentInterface c_segment);
	*/
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return -1
	}

	rowCount := C.GetRealCount(s.segmentPtr)

	return int64(rowCount)
}

func (s *Segment) getRowCount() int64 {
	/*
		long int
		getRowCount(CSegmentInterface c_segment);
	*/
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return -1
	}

	rowCount := C.GetRowCount(s.segmentPtr)

	return int64(rowCount)
}

func (s *Segment) getDeletedCount() int64 {
	/*
		long int
		getDeletedCount(CSegmentInterface c_segment);
	*/
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return -1
	}

	deletedCount := C.GetRowCount(s.segmentPtr)

	return int64(deletedCount)
}

func (s *Segment) getMemSize() int64 {
	/*
		long int
		GetMemoryUsageInBytes(CSegmentInterface c_segment);
	*/
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return -1
	}
	memoryUsageInBytes := C.GetMemoryUsageInBytes(s.segmentPtr)

	return int64(memoryUsageInBytes)
}

func (s *Segment) search(ctx context.Context, searchReq *searchRequest) (*SearchResult, error) {
	/*
		CStatus
		Search(void* plan,
			void* placeholder_groups,
			uint64_t* timestamps,
			int num_groups,
			long int* result_ids,
			float* result_distances);
	*/
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return nil, fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	if searchReq.plan == nil {
		return nil, fmt.Errorf("nil search plan")
	}

	loadIndex := s.hasLoadIndexForIndexedField(searchReq.searchFieldID)
	var searchResult SearchResult
	log.Ctx(ctx).Debug("start do search on segment",
		zap.Int64("msgID", searchReq.msgID),
		zap.Int64("segmentID", s.segmentID),
		zap.String("segmentType", s.segmentType.String()),
		zap.Bool("loadIndex", loadIndex))

	tr := timerecord.NewTimeRecorder("cgoSearch")
	status := C.Search(s.segmentPtr, searchReq.plan.cSearchPlan, searchReq.cPlaceholderGroup,
		C.uint64_t(searchReq.timestamp), &searchResult.cSearchResult)
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel).
		Observe(float64(tr.CtxElapse(ctx, "finish cgoSearch").Milliseconds()))
	if err := HandleCStatus(&status, "Search failed"); err != nil {
		return nil, err
	}
	log.Ctx(ctx).Debug("do search on segment done",
		zap.Int64("msgID", searchReq.msgID),
		zap.Int64("segmentID", s.segmentID),
		zap.String("segmentType", s.segmentType.String()),
		zap.Bool("loadIndex", loadIndex))
	return &searchResult, nil
}

func (s *Segment) retrieve(plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return nil, fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	var retrieveResult RetrieveResult
	ts := C.uint64_t(plan.Timestamp)

	tr := timerecord.NewTimeRecorder("cgoRetrieve")
	status := C.Retrieve(s.segmentPtr, plan.cRetrievePlan, ts, &retrieveResult.cRetrieveResult)
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Debug("do retrieve on segment",
		zap.Int64("msgID", plan.msgID),
		zap.Int64("segmentID", s.segmentID), zap.String("segmentType", s.segmentType.String()))

	if err := HandleCStatus(&status, "Retrieve failed"); err != nil {
		return nil, err
	}
	result := new(segcorepb.RetrieveResults)
	if err := HandleCProto(&retrieveResult.cRetrieveResult, result); err != nil {
		return nil, err
	}

	sort.Sort(&byPK{result})
	return result, nil
}

func (s *Segment) getFieldDataPath(indexedFieldInfo *IndexedFieldInfo, offset int64) (dataPath string, offsetInBinlog int64) {
	offsetInBinlog = offset
	for _, binlog := range indexedFieldInfo.fieldBinlog.Binlogs {
		if offsetInBinlog < binlog.EntriesNum {
			dataPath = binlog.GetLogPath()
			break
		} else {
			offsetInBinlog -= binlog.EntriesNum
		}
	}
	return dataPath, offsetInBinlog
}

func fillBinVecFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	dim := fieldData.GetVectors().GetDim()
	rowBytes := dim / 8
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	x := fieldData.GetVectors().GetData().(*schemapb.VectorField_BinaryVector)
	resultLen := dim / 8
	copy(x.BinaryVector[i*int(resultLen):(i+1)*int(resultLen)], content)
	return nil
}

func fillFloatVecFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	dim := fieldData.GetVectors().GetDim()
	rowBytes := dim * 4
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	x := fieldData.GetVectors().GetData().(*schemapb.VectorField_FloatVector)
	floatResult := make([]float32, dim)
	buf := bytes.NewReader(content)
	if err = binary.Read(buf, endian, &floatResult); err != nil {
		return err
	}
	resultLen := dim
	copy(x.FloatVector.Data[i*int(resultLen):(i+1)*int(resultLen)], floatResult)
	return nil
}

func fillBoolFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read whole file.
	// TODO: optimize here.
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var arr schemapb.BoolArray
	err = proto.Unmarshal(content, &arr)
	if err != nil {
		return err
	}
	fieldData.GetScalars().GetBoolData().GetData()[i] = arr.Data[offset]
	return nil
}

func fillStringFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read whole file.
	// TODO: optimize here.
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var arr schemapb.StringArray
	err = proto.Unmarshal(content, &arr)
	if err != nil {
		return err
	}
	fieldData.GetScalars().GetStringData().GetData()[i] = arr.Data[offset]
	return nil
}

func fillInt8FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(1)
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	var i8 int8
	if err := funcutil.ReadBinary(endian, content, &i8); err != nil {
		return err
	}
	fieldData.GetScalars().GetIntData().GetData()[i] = int32(i8)
	return nil
}

func fillInt16FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(2)
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	var i16 int16
	if err := funcutil.ReadBinary(endian, content, &i16); err != nil {
		return err
	}
	fieldData.GetScalars().GetIntData().GetData()[i] = int32(i16)
	return nil
}

func fillInt32FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(4)
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetIntData().GetData()[i]))
}

func fillInt64FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(8)
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetLongData().GetData()[i]))
}

func fillFloatFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(4)
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetFloatData().GetData()[i]))
}

func fillDoubleFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(8)
	content, err := vcm.ReadAt(ctx, dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetDoubleData().GetData()[i]))
}

func fillFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	switch fieldData.Type {
	case schemapb.DataType_BinaryVector:
		return fillBinVecFieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_FloatVector:
		return fillFloatVecFieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Bool:
		return fillBoolFieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return fillStringFieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int8:
		return fillInt8FieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int16:
		return fillInt16FieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int32:
		return fillInt32FieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int64:
		return fillInt64FieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Float:
		return fillFloatFieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Double:
		return fillDoubleFieldData(ctx, vcm, dataPath, fieldData, i, offset, endian)
	default:
		return fmt.Errorf("invalid data type: %s", fieldData.Type.String())
	}
}

func (s *Segment) fillIndexedFieldsData(ctx context.Context, collectionID UniqueID,
	vcm storage.ChunkManager, result *segcorepb.RetrieveResults) error {

	for _, fieldData := range result.FieldsData {
		// If the vector field doesn't have indexed. Vector data is in memory for
		// brute force search. No need to download data from remote.
		if fieldData.GetType() != schemapb.DataType_FloatVector && fieldData.GetType() != schemapb.DataType_BinaryVector ||
			!s.hasLoadIndexForIndexedField(fieldData.FieldId) {
			continue
		}

		indexedFieldInfo, err := s.getIndexedFieldInfo(fieldData.FieldId)
		if err != nil {
			continue
		}

		// TODO: optimize here. Now we'll read a whole file from storage every time we retrieve raw data by offset.
		for i, offset := range result.Offset {
			dataPath, offsetInBinlog := s.getFieldDataPath(indexedFieldInfo, offset)
			endian := common.Endian

			// fill field data that fieldData[i] = dataPath[offsetInBinlog*rowBytes, (offsetInBinlog+1)*rowBytes]
			if err := fillFieldData(ctx, vcm, dataPath, fieldData, i, offsetInBinlog, endian); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Segment) updateBloomFilter(pks []primaryKey) {
	s.statLock.Lock()
	defer s.statLock.Unlock()
	s.InitCurrentStat()
	buf := make([]byte, 8)
	for _, pk := range pks {
		s.currentStat.UpdateMinMax(pk)
		switch pk.Type() {
		case schemapb.DataType_Int64:
			int64Value := pk.(*int64PrimaryKey).Value
			common.Endian.PutUint64(buf, uint64(int64Value))
			s.currentStat.PkFilter.Add(buf)
		case schemapb.DataType_VarChar:
			stringValue := pk.(*varCharPrimaryKey).Value
			s.currentStat.PkFilter.AddString(stringValue)
		default:
			log.Error("failed to update bloomfilter", zap.Any("PK type", pk.Type()))
			panic("failed to update bloomfilter")
		}
	}
}

func (s *Segment) InitCurrentStat() {
	if s.currentStat == nil {
		s.currentStat = &storage.PkStatistics{
			PkFilter: bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive),
		}
	}
}

// check if PK exists is current
func (s *Segment) isPKExist(pk primaryKey) bool {
	s.statLock.Lock()
	defer s.statLock.Unlock()
	if s.currentStat != nil && s.currentStat.PkExist(pk) {
		return true
	}

	// for sealed, if one of the stats shows it exist, then we have to check it
	for _, historyStat := range s.historyStats {
		if historyStat.PkExist(pk) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------- interfaces for growing segment
func (s *Segment) segmentPreInsert(numOfRecords int) (int64, error) {
	/*
		long int
		PreInsert(CSegmentInterface c_segment, long int size);
	*/
	if s.getType() != segmentTypeGrowing {
		return 0, nil
	}

	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return -1, fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	var offset int64
	cOffset := (*C.int64_t)(&offset)
	status := C.PreInsert(s.segmentPtr, C.int64_t(int64(numOfRecords)), cOffset)

	if err := HandleCStatus(&status, "PreInsert failed"); err != nil {
		return 0, err
	}
	return offset, nil
}

func (s *Segment) segmentPreDelete(numOfRecords int) int64 {
	/*
		long int
		PreDelete(CSegmentInterface c_segment, long int size);
	*/
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return -1
	}

	offset := C.PreDelete(s.segmentPtr, C.int64_t(int64(numOfRecords)))

	return int64(offset)
}

func (s *Segment) segmentInsert(offset int64, entityIDs []UniqueID, timestamps []Timestamp, record *segcorepb.InsertRecord) error {
	if s.getType() != segmentTypeGrowing {
		return fmt.Errorf("unexpected segmentType when segmentInsert, segmentType = %s", s.segmentType.String())
	}

	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	insertRecordBlob, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal insert record: %s", err)
	}

	var numOfRow = len(entityIDs)
	var cOffset = C.int64_t(offset)
	var cNumOfRows = C.int64_t(numOfRow)
	var cEntityIdsPtr = (*C.int64_t)(&(entityIDs)[0])
	var cTimestampsPtr = (*C.uint64_t)(&(timestamps)[0])

	status := C.Insert(s.segmentPtr,
		cOffset,
		cNumOfRows,
		cEntityIdsPtr,
		cTimestampsPtr,
		(*C.uint8_t)(unsafe.Pointer(&insertRecordBlob[0])),
		(C.uint64_t)(len(insertRecordBlob)))

	if err := HandleCStatus(&status, "Insert failed"); err != nil {
		return err
	}
	metrics.QueryNodeNumEntities.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		fmt.Sprint(s.collectionID),
		fmt.Sprint(s.partitionID),
		s.segmentType.String(),
		fmt.Sprint(0),
	).Add(float64(numOfRow))
	s.setRecentlyModified(true)
	return nil
}

func (s *Segment) segmentDelete(offset int64, entityIDs []primaryKey, timestamps []Timestamp) error {
	/*
		CStatus
		Delete(CSegmentInterface c_segment,
		           long int reserved_offset,
		           long size,
		           const long* primary_keys,
		           const unsigned long* timestamps);
	*/
	if len(entityIDs) <= 0 {
		return fmt.Errorf("empty pks to delete")
	}

	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	if len(entityIDs) != len(timestamps) {
		return errors.New("length of entityIDs not equal to length of timestamps")
	}

	var cOffset = C.int64_t(offset)
	var cSize = C.int64_t(len(entityIDs))
	var cTimestampsPtr = (*C.uint64_t)(&(timestamps)[0])

	ids := &schemapb.IDs{}
	pkType := entityIDs[0].Type()
	switch pkType {
	case schemapb.DataType_Int64:
		int64Pks := make([]int64, len(entityIDs))
		for index, entity := range entityIDs {
			int64Pks[index] = entity.(*int64PrimaryKey).Value
		}
		ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Pks,
			},
		}
	case schemapb.DataType_VarChar:
		varCharPks := make([]string, len(entityIDs))
		for index, entity := range entityIDs {
			varCharPks[index] = entity.(*varCharPrimaryKey).Value
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

	status := C.Delete(s.segmentPtr, cOffset, cSize, (*C.uint8_t)(unsafe.Pointer(&dataBlob[0])), (C.uint64_t)(len(dataBlob)), cTimestampsPtr)

	if err := HandleCStatus(&status, "Delete failed"); err != nil {
		return err
	}

	return nil
}

// -------------------------------------------------------------------------------------- interfaces for sealed segment

func (s *Segment) segmentLoadFieldData(fieldID int64, rowCount int64, data *schemapb.FieldData) error {
	/*
		CStatus
		LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info);
	*/
	if s.getType() != segmentTypeSealed {
		errMsg := fmt.Sprintln("segmentLoadFieldData failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	dataBlob, err := proto.Marshal(data)
	if err != nil {
		return err
	}

	var mmapDirPath *C.char = nil
	path := paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()
	if len(path) > 0 {
		mmapDirPath = C.CString(path)
		defer C.free(unsafe.Pointer(mmapDirPath))
	}
	loadInfo := C.CLoadFieldDataInfo{
		field_id:      C.int64_t(fieldID),
		blob:          (*C.uint8_t)(unsafe.Pointer(&dataBlob[0])),
		blob_size:     C.uint64_t(len(dataBlob)),
		row_count:     C.int64_t(rowCount),
		mmap_dir_path: mmapDirPath,
	}

	status := C.LoadFieldData(s.segmentPtr, loadInfo)

	if err := HandleCStatus(&status, "LoadFieldData failed"); err != nil {
		return err
	}

	log.Info("load field done",
		zap.Int64("fieldID", fieldID),
		zap.Int64("row count", rowCount),
		zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *Segment) segmentLoadDeletedRecord(primaryKeys []primaryKey, timestamps []Timestamp, rowCount int64) error {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	if len(primaryKeys) <= 0 {
		return fmt.Errorf("empty pks to delete")
	}
	pkType := primaryKeys[0].Type()
	ids := &schemapb.IDs{}
	switch pkType {
	case schemapb.DataType_Int64:
		int64Pks := make([]int64, len(primaryKeys))
		for index, pk := range primaryKeys {
			int64Pks[index] = pk.(*int64PrimaryKey).Value
		}
		ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Pks,
			},
		}
	case schemapb.DataType_VarChar:
		varCharPks := make([]string, len(primaryKeys))
		for index, pk := range primaryKeys {
			varCharPks[index] = pk.(*varCharPrimaryKey).Value
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
		timestamps:        unsafe.Pointer(&timestamps[0]),
		primary_keys:      (*C.uint8_t)(unsafe.Pointer(&idsBlob[0])),
		primary_keys_size: C.uint64_t(len(idsBlob)),
		row_count:         C.int64_t(rowCount),
	}
	/*
		CStatus
		LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info)
	*/
	status := C.LoadDeletedRecord(s.segmentPtr, loadInfo)

	if err := HandleCStatus(&status, "LoadDeletedRecord failed"); err != nil {
		return err
	}

	log.Info("load deleted record done",
		zap.Int64("row count", rowCount),
		zap.Int64("segmentID", s.ID()),
		zap.String("segmentType", s.getType().String()))
	return nil
}

func (s *Segment) segmentLoadIndexData(bytesIndex [][]byte, indexInfo *querypb.FieldIndexInfo, fieldType schemapb.DataType) error {
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
	if s.getType() != segmentTypeSealed {
		errMsg := fmt.Sprintln("updateSegmentIndex failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}
	s.mut.RLock()
	defer s.mut.RUnlock()
	if !s.healthy() {
		return fmt.Errorf("%w(segmentID=%d)", ErrSegmentUnhealthy, s.segmentID)
	}

	status := C.UpdateSealedSegmentIndex(s.segmentPtr, loadIndexInfo.cLoadIndexInfo)

	if err := HandleCStatus(&status, "UpdateSealedSegmentIndex failed"); err != nil {
		return err
	}

	log.Info("updateSegmentIndex done", zap.Int64("segmentID", s.ID()), zap.Int64("fieldID", indexInfo.FieldID))

	return nil
}
