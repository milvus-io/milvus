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
#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/util/timerecord"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/cgoconverter"
)

type segmentType = commonpb.SegmentState

const (
	segmentTypeGrowing = commonpb.SegmentState_Growing
	segmentTypeSealed  = commonpb.SegmentState_Sealed
)

const (
	bloomFilterSize       uint    = 100000
	maxBloomFalsePositive float64 = 0.005
)

// IndexedFieldInfo contains binlog info of vector field
type IndexedFieldInfo struct {
	fieldBinlog *datapb.FieldBinlog
	indexInfo   *querypb.FieldIndexInfo
}

// Segment is a wrapper of the underlying C-structure segment.
type Segment struct {
	segPtrMu   sync.RWMutex // guards segmentPtr
	segmentPtr C.CSegmentInterface

	segmentID    UniqueID
	partitionID  UniqueID
	collectionID UniqueID

	vChannelID   Channel
	lastMemSize  int64
	lastRowCount int64

	rmMutex          sync.RWMutex // guards recentlyModified
	recentlyModified bool

	typeMu      sync.Mutex // guards builtIndex
	segmentType segmentType

	idBinlogRowSizes []int64

	indexedFieldMutex sync.RWMutex // guards indexedFieldInfos
	indexedFieldInfos map[UniqueID]*IndexedFieldInfo

	pkFilter *bloom.BloomFilter //  bloom filter of pk inside a segment
}

// ID returns the identity number.
func (s *Segment) ID() UniqueID {
	return s.segmentID
}

func (s *Segment) setIDBinlogRowSizes(sizes []int64) {
	s.idBinlogRowSizes = sizes
}

func (s *Segment) getIDBinlogRowSizes() []int64 {
	return s.idBinlogRowSizes
}

func (s *Segment) setRecentlyModified(modify bool) {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
	s.recentlyModified = modify
}

func (s *Segment) getRecentlyModified() bool {
	s.rmMutex.RLock()
	defer s.rmMutex.RUnlock()
	return s.recentlyModified
}

func (s *Segment) setType(segType segmentType) {
	s.typeMu.Lock()
	defer s.typeMu.Unlock()
	s.segmentType = segType
}

func (s *Segment) getType() segmentType {
	s.typeMu.Lock()
	defer s.typeMu.Unlock()
	return s.segmentType
}

func (s *Segment) setIndexedFieldInfo(fieldID UniqueID, info *IndexedFieldInfo) {
	s.indexedFieldMutex.Lock()
	defer s.indexedFieldMutex.Unlock()
	s.indexedFieldInfos[fieldID] = info
}

func (s *Segment) getIndexedFieldInfo(fieldID UniqueID) (*IndexedFieldInfo, error) {
	s.indexedFieldMutex.RLock()
	defer s.indexedFieldMutex.RUnlock()
	if info, ok := s.indexedFieldInfos[fieldID]; ok {
		return &IndexedFieldInfo{
			fieldBinlog: info.fieldBinlog,
			indexInfo:   info.indexInfo,
		}, nil
	}
	return nil, fmt.Errorf("Invalid fieldID %d", fieldID)
}

func (s *Segment) hasLoadIndexForIndexedField(fieldID int64) bool {
	s.indexedFieldMutex.RLock()
	defer s.indexedFieldMutex.RUnlock()

	if fieldInfo, ok := s.indexedFieldInfos[fieldID]; ok {
		return fieldInfo.indexInfo != nil && fieldInfo.indexInfo.EnableIndex
	}

	return false
}

func newSegment(collection *Collection, segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, vChannelID Channel, segType segmentType) (*Segment, error) {
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
		log.Error("create new segment error",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID),
			zap.Int64("segmentID", segmentID),
			zap.Int32("segment type", int32(segType)),
			zap.Error(err))
		return nil, err
	}

	log.Info("create segment",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
		zap.Int32("segmentType", int32(segType)))

	var segment = &Segment{
		segmentPtr:        segmentPtr,
		segmentType:       segType,
		segmentID:         segmentID,
		partitionID:       partitionID,
		collectionID:      collectionID,
		vChannelID:        vChannelID,
		indexedFieldInfos: make(map[UniqueID]*IndexedFieldInfo),

		pkFilter: bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive),
	}

	return segment, nil
}

func deleteSegment(segment *Segment) {
	/*
		void
		deleteSegment(CSegmentInterface segment);
	*/
	if segment.segmentPtr == nil {
		return
	}

	segment.segPtrMu.Lock()
	defer segment.segPtrMu.Unlock()
	cPtr := segment.segmentPtr
	C.DeleteSegment(cPtr)
	segment.segmentPtr = nil

	log.Info("delete segment from memory", zap.Int64("collectionID", segment.collectionID), zap.Int64("partitionID", segment.partitionID), zap.Int64("segmentID", segment.ID()))

	segment = nil
}

func (s *Segment) getRowCount() int64 {
	/*
		long int
		getRowCount(CSegmentInterface c_segment);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock()
	if s.segmentPtr == nil {
		return -1
	}
	var rowCount = C.GetRowCount(s.segmentPtr)
	return int64(rowCount)
}

func (s *Segment) getDeletedCount() int64 {
	/*
		long int
		getDeletedCount(CSegmentInterface c_segment);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock()
	if s.segmentPtr == nil {
		return -1
	}
	var deletedCount = C.GetDeletedCount(s.segmentPtr)
	return int64(deletedCount)
}

func (s *Segment) getMemSize() int64 {
	/*
		long int
		GetMemoryUsageInBytes(CSegmentInterface c_segment);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock()
	if s.segmentPtr == nil {
		return -1
	}
	var memoryUsageInBytes = C.GetMemoryUsageInBytes(s.segmentPtr)

	return int64(memoryUsageInBytes)
}

func (s *Segment) search(searchReq *searchRequest) (*SearchResult, error) {
	/*
		CStatus
		Search(void* plan,
			void* placeholder_groups,
			uint64_t* timestamps,
			int num_groups,
			long int* result_ids,
			float* result_distances);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock()
	if s.segmentPtr == nil {
		return nil, errors.New("null seg core pointer")
	}

	if searchReq.plan == nil {
		return nil, fmt.Errorf("nil search plan")
	}

	var searchResult SearchResult
	log.Debug("do search on segment", zap.Int64("segmentID", s.segmentID), zap.Int32("segmentType", int32(s.segmentType)))
	tr := timerecord.NewTimeRecorder("cgoSearch")
	status := C.Search(s.segmentPtr, searchReq.plan.cSearchPlan, searchReq.cPlaceholderGroup,
		C.uint64_t(searchReq.timestamp), &searchResult.cSearchResult, C.int64_t(s.segmentID))
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	if err := HandleCStatus(&status, "Search failed"); err != nil {
		return nil, err
	}
	return &searchResult, nil
}

// HandleCProto deal with the result proto returned from CGO
func HandleCProto(cRes *C.CProto, msg proto.Message) error {
	// Standalone CProto is protobuf created by C side,
	// Passed from c side
	// memory is managed manually
	lease, blob := cgoconverter.UnsafeGoBytes(&cRes.proto_blob, int(cRes.proto_size))
	defer cgoconverter.Release(lease)

	return proto.Unmarshal(blob, msg)
}

func (s *Segment) retrieve(plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock()
	if s.segmentPtr == nil {
		return nil, errors.New("null seg core pointer")
	}

	var retrieveResult RetrieveResult
	ts := C.uint64_t(plan.Timestamp)
	tr := timerecord.NewTimeRecorder("cgoRetrieve")
	status := C.Retrieve(s.segmentPtr, plan.cRetrievePlan, ts, &retrieveResult.cRetrieveResult)
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	log.Debug("do retrieve on segment", zap.Int64("segmentID", s.segmentID), zap.Int32("segmentType", int32(s.segmentType)))
	if err := HandleCStatus(&status, "Retrieve failed"); err != nil {
		return nil, err
	}
	result := new(segcorepb.RetrieveResults)
	if err := HandleCProto(&retrieveResult.cRetrieveResult, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Segment) getFieldDataPath(indexedFieldInfo *IndexedFieldInfo, offset int64) (dataPath string, offsetInBinlog int64) {
	offsetInBinlog = offset
	for index, idBinlogRowSize := range s.idBinlogRowSizes {
		if offsetInBinlog < idBinlogRowSize {
			dataPath = indexedFieldInfo.fieldBinlog.Binlogs[index].GetLogPath()
			break
		} else {
			offsetInBinlog -= idBinlogRowSize
		}
	}
	return dataPath, offsetInBinlog
}

func fillBinVecFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	dim := fieldData.GetVectors().GetDim()
	rowBytes := dim / 8
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	x := fieldData.GetVectors().GetData().(*schemapb.VectorField_BinaryVector)
	resultLen := dim / 8
	copy(x.BinaryVector[i*int(resultLen):(i+1)*int(resultLen)], content)
	return nil
}

func fillFloatVecFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	dim := fieldData.GetVectors().GetDim()
	rowBytes := dim * 4
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
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

func fillBoolFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read whole file.
	// TODO: optimize here.
	content, err := vcm.Read(dataPath)
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

func fillStringFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read whole file.
	// TODO: optimize here.
	content, err := vcm.Read(dataPath)
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

func fillInt8FieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(1)
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
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

func fillInt16FieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(2)
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
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

func fillInt32FieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(4)
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetIntData().GetData()[i]))
}

func fillInt64FieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(8)
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetLongData().GetData()[i]))
}

func fillFloatFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(4)
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetFloatData().GetData()[i]))
}

func fillDoubleFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(8)
	content, err := vcm.ReadAt(dataPath, offset*rowBytes, rowBytes)
	if err != nil {
		return err
	}
	return funcutil.ReadBinary(endian, content, &(fieldData.GetScalars().GetDoubleData().GetData()[i]))
}

func fillFieldData(vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, i int, offset int64, endian binary.ByteOrder) error {
	switch fieldData.Type {
	case schemapb.DataType_BinaryVector:
		return fillBinVecFieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_FloatVector:
		return fillFloatVecFieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Bool:
		return fillBoolFieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return fillStringFieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int8:
		return fillInt8FieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int16:
		return fillInt16FieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int32:
		return fillInt32FieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Int64:
		return fillInt64FieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Float:
		return fillFloatFieldData(vcm, dataPath, fieldData, i, offset, endian)
	case schemapb.DataType_Double:
		return fillDoubleFieldData(vcm, dataPath, fieldData, i, offset, endian)
	default:
		return fmt.Errorf("invalid data type: %s", fieldData.Type.String())
	}
}

func (s *Segment) fillIndexedFieldsData(collectionID UniqueID,
	vcm storage.ChunkManager, result *segcorepb.RetrieveResults) error {

	for _, fieldData := range result.FieldsData {
		// If the vector field doesn't have indexed. Vector data is in memory for
		// brute force search. No need to download data from remote.
		if !s.hasLoadIndexForIndexedField(fieldData.FieldId) {
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
			if err := fillFieldData(vcm, dataPath, fieldData, i, offsetInBinlog, endian); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Segment) updateBloomFilter(pks []primaryKey) {
	buf := make([]byte, 8)
	for _, pk := range pks {
		switch pk.Type() {
		case schemapb.DataType_Int64:
			int64Value := pk.(*int64PrimaryKey).Value
			common.Endian.PutUint64(buf, uint64(int64Value))
			s.pkFilter.Add(buf)
		case schemapb.DataType_VarChar:
			stringValue := pk.(*varCharPrimaryKey).Value
			s.pkFilter.AddString(stringValue)
		default:
			log.Warn("failed to update bloomfilter", zap.Any("PK type", pk.Type()))
		}
	}
}

//-------------------------------------------------------------------------------------- interfaces for growing segment
func (s *Segment) segmentPreInsert(numOfRecords int) (int64, error) {
	/*
		long int
		PreInsert(CSegmentInterface c_segment, long int size);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentType != segmentTypeGrowing {
		return 0, nil
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
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	var offset = C.PreDelete(s.segmentPtr, C.int64_t(int64(numOfRecords)))

	return int64(offset)
}

func (s *Segment) segmentInsert(offset int64, entityIDs []UniqueID, timestamps []Timestamp, record *segcorepb.InsertRecord) error {
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentType != segmentTypeGrowing {
		return nil
	}

	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
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

	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
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

//-------------------------------------------------------------------------------------- interfaces for sealed segment
func (s *Segment) segmentLoadFieldData(fieldID int64, rowCount int64, data *schemapb.FieldData) error {
	/*
		CStatus
		LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	if s.segmentType != segmentTypeSealed {
		errMsg := fmt.Sprintln("segmentLoadFieldData failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}

	dataBlob, err := proto.Marshal(data)
	if err != nil {
		return err
	}

	loadInfo := C.CLoadFieldDataInfo{
		field_id:  C.int64_t(fieldID),
		blob:      (*C.uint8_t)(unsafe.Pointer(&dataBlob[0])),
		blob_size: C.uint64_t(len(dataBlob)),
		row_count: C.int64_t(rowCount),
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
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
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
		zap.Int64("segmentID", s.ID()))
	return nil
}

func (s *Segment) segmentLoadIndexData(bytesIndex [][]byte, indexInfo *querypb.FieldIndexInfo, fieldType schemapb.DataType) error {
	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}

	err = loadIndexInfo.appendIndexInfo(bytesIndex, indexInfo, fieldType)
	if err != nil {
		return err
	}

	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	if s.segmentType != segmentTypeSealed {
		errMsg := fmt.Sprintln("updateSegmentIndex failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}

	status := C.UpdateSealedSegmentIndex(s.segmentPtr, loadIndexInfo.cLoadIndexInfo)
	if err := HandleCStatus(&status, "UpdateSealedSegmentIndex failed"); err != nil {
		return err
	}

	log.Info("updateSegmentIndex done", zap.Int64("segmentID", s.ID()))

	return nil
}
