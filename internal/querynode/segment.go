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
	"strconv"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/util/timerecord"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
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
	"github.com/milvus-io/milvus/internal/util/typeutil"
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

// VectorFieldInfo contains binlog info of vector field
type VectorFieldInfo struct {
	fieldBinlog *datapb.FieldBinlog
	indexInfo   *querypb.VecFieldIndexInfo
}

// Segment is a wrapper of the underlying C-structure segment.
type Segment struct {
	segPtrMu   sync.RWMutex // guards segmentPtr
	segmentPtr C.CSegmentInterface

	segmentID    UniqueID
	partitionID  UniqueID
	collectionID UniqueID

	onService bool

	vChannelID   Channel
	lastMemSize  int64
	lastRowCount int64

	rmMutex          sync.RWMutex // guards recentlyModified
	recentlyModified bool

	typeMu      sync.Mutex // guards builtIndex
	segmentType segmentType

	idBinlogRowSizes []int64

	vectorFieldMutex sync.RWMutex // guards vectorFieldInfos
	vectorFieldInfos map[UniqueID]*VectorFieldInfo

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

func (s *Segment) getOnService() bool {
	return s.onService
}

func (s *Segment) setOnService(onService bool) {
	s.onService = onService
}

func (s *Segment) setVectorFieldInfo(fieldID UniqueID, info *VectorFieldInfo) {
	s.vectorFieldMutex.Lock()
	defer s.vectorFieldMutex.Unlock()
	s.vectorFieldInfos[fieldID] = info
}

func (s *Segment) getVectorFieldInfo(fieldID UniqueID) (*VectorFieldInfo, error) {
	s.vectorFieldMutex.RLock()
	defer s.vectorFieldMutex.RUnlock()
	if info, ok := s.vectorFieldInfos[fieldID]; ok {
		return &VectorFieldInfo{
			fieldBinlog: info.fieldBinlog,
			indexInfo:   info.indexInfo,
		}, nil
	}
	return nil, errors.New("Invalid fieldID " + strconv.Itoa(int(fieldID)))
}

func (s *Segment) hasLoadIndexForVecField(fieldID int64) bool {
	s.vectorFieldMutex.RLock()
	defer s.vectorFieldMutex.RUnlock()

	if fieldInfo, ok := s.vectorFieldInfos[fieldID]; ok {
		return fieldInfo.indexInfo != nil && fieldInfo.indexInfo.EnableIndex
	}

	return false
}

func newSegment(collection *Collection, segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, vChannelID Channel, segType segmentType, onService bool) (*Segment, error) {
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

	log.Debug("create segment",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
		zap.Int32("segmentType", int32(segType)))

	var segment = &Segment{
		segmentPtr:       segmentPtr,
		segmentType:      segType,
		segmentID:        segmentID,
		partitionID:      partitionID,
		collectionID:     collectionID,
		vChannelID:       vChannelID,
		onService:        onService,
		vectorFieldInfos: make(map[UniqueID]*VectorFieldInfo),

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

	log.Debug("delete segment from memory", zap.Int64("collectionID", segment.collectionID), zap.Int64("partitionID", segment.partitionID), zap.Int64("segmentID", segment.ID()))

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

func (s *Segment) search(plan *SearchPlan,
	searchRequests []*searchRequest,
	timestamp []Timestamp) (*SearchResult, error) {
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
	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range searchRequests {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}

	var searchResult SearchResult
	ts := C.uint64_t(timestamp[0])
	cPlaceHolderGroup := cPlaceholderGroups[0]

	log.Debug("do search on segment", zap.Int64("segmentID", s.segmentID), zap.Int32("segmentType", int32(s.segmentType)))
	tr := timerecord.NewTimeRecorder("cgoSearch")
	status := C.Search(s.segmentPtr, plan.cSearchPlan, cPlaceHolderGroup, ts, &searchResult.cSearchResult, C.int64_t(s.segmentID))
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.SearchLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
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
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID),
		metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	if err := HandleCStatus(&status, "Retrieve failed"); err != nil {
		return nil, err
	}
	result := new(segcorepb.RetrieveResults)
	if err := HandleCProto(&retrieveResult.cRetrieveResult, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Segment) fillVectorFieldsData(collectionID UniqueID,
	vcm storage.ChunkManager, result *segcorepb.RetrieveResults) error {

	for _, fieldData := range result.FieldsData {
		if !typeutil.IsVectorType(fieldData.Type) {
			continue
		}

		vecFieldInfo, err := s.getVectorFieldInfo(fieldData.FieldId)
		if err != nil {
			continue
		}

		// If the vector field doesn't have indexed. Vector data is in memory for
		// brute force search. No need to download data from remote.
		if !s.hasLoadIndexForVecField(fieldData.FieldId) {
			continue
		}

		dim := fieldData.GetVectors().GetDim()
		log.Debug("FillVectorFieldData", zap.Int64("fieldId", fieldData.FieldId),
			zap.Any("datatype", fieldData.Type), zap.Int64("dim", dim))

		for i, offset := range result.Offset {
			var vecPath string
			for index, idBinlogRowSize := range s.idBinlogRowSizes {
				if offset < idBinlogRowSize {
					vecPath = vecFieldInfo.fieldBinlog.Binlogs[index].GetLogPath()
					break
				} else {
					offset -= idBinlogRowSize
				}
			}
			log.Debug("FillVectorFieldData", zap.String("path", vecPath))

			switch fieldData.Type {
			case schemapb.DataType_BinaryVector:
				rowBytes := dim / 8
				x := fieldData.GetVectors().GetData().(*schemapb.VectorField_BinaryVector)
				content, err := vcm.ReadAt(vecPath, offset*rowBytes, rowBytes)
				if err != nil {
					return err
				}
				resultLen := dim / 8
				copy(x.BinaryVector[i*int(resultLen):(i+1)*int(resultLen)], content)
			case schemapb.DataType_FloatVector:
				x := fieldData.GetVectors().GetData().(*schemapb.VectorField_FloatVector)
				rowBytes := dim * 4
				content, err := vcm.ReadAt(vecPath, offset*rowBytes, rowBytes)
				if err != nil {
					return err
				}
				floatResult := make([]float32, dim)
				buf := bytes.NewReader(content)
				if err = binary.Read(buf, common.Endian, &floatResult); err != nil {
					return err
				}
				resultLen := dim
				copy(x.FloatVector.Data[i*int(resultLen):(i+1)*int(resultLen)], floatResult)
			}
		}
	}
	return nil
}

func (s *Segment) updateBloomFilter(pks []int64) {
	buf := make([]byte, 8)
	for _, pk := range pks {
		common.Endian.PutUint64(buf, uint64(pk))
		s.pkFilter.Add(buf)
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

// TODO: remove reference of slice
func (s *Segment) segmentInsert(offset int64, entityIDs *[]UniqueID, timestamps *[]Timestamp, records *[]*commonpb.Blob) error {
	/*
		CStatus
		Insert(CSegmentInterface c_segment,
		           long int reserved_offset,
		           signed long int size,
		           const long* primary_keys,
		           const unsigned long* timestamps,
		           void* raw_data,
		           int sizeof_per_row,
		           signed long int count);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentType != segmentTypeGrowing {
		return nil
	}

	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	// Blobs to one big blob
	var numOfRow = len(*entityIDs)
	var sizeofPerRow = len((*records)[0].Value)

	assert.Equal(nil, numOfRow, len(*records))
	if numOfRow != len(*records) {
		return errors.New("entityIDs row num not equal to length of records")
	}

	var rawData = make([]byte, numOfRow*sizeofPerRow)
	var copyOffset = 0
	for i := 0; i < len(*records); i++ {
		copy(rawData[copyOffset:], (*records)[i].Value)
		copyOffset += sizeofPerRow
	}

	var cOffset = C.int64_t(offset)
	var cNumOfRows = C.int64_t(numOfRow)
	var cEntityIdsPtr = (*C.int64_t)(&(*entityIDs)[0])
	var cTimestampsPtr = (*C.uint64_t)(&(*timestamps)[0])
	var cSizeofPerRow = C.int(sizeofPerRow)
	var cRawDataVoidPtr = unsafe.Pointer(&rawData[0])
	status := C.Insert(s.segmentPtr,
		cOffset,
		cNumOfRows,
		cEntityIdsPtr,
		cTimestampsPtr,
		cRawDataVoidPtr,
		cSizeofPerRow,
		cNumOfRows)
	if err := HandleCStatus(&status, "Insert failed"); err != nil {
		return err
	}

	s.setRecentlyModified(true)
	return nil
}

func (s *Segment) segmentInsertColumnData(offset int64, entityIDs *[]UniqueID, timestamps *[]Timestamp, columns *[]*schemapb.FieldData, schema *schemapb.CollectionSchema) error {
	/*
		CStatus
		InsertColumnData(CSegmentInterface c_segment,
						 int64_t reserved_offset,
						 int64_t size,
						 const int64_t* row_ids,
						 const uint64_t* timestamps,
						 void* raw_data,
						 int64_t count);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentType != segmentTypeGrowing {
		return nil
	}

	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	rawData, err := typeutil.TransferColumnBasedDataToByteSlice(schema, *columns)
	if err != nil {
		return err
	}

	var cOffset = C.int64_t(offset)
	var cNumOfRows = C.int64_t(len(*entityIDs))
	var cEntityIdsPtr = (*C.int64_t)(&(*entityIDs)[0])
	var cTimestampsPtr = (*C.uint64_t)(&(*timestamps)[0])
	var cRawDataVoidPtr = unsafe.Pointer(&rawData[0])

	status := C.InsertColumnData(s.segmentPtr,
		cOffset,
		cNumOfRows,
		cEntityIdsPtr,
		cTimestampsPtr,
		cRawDataVoidPtr,
		cNumOfRows)
	if err := HandleCStatus(&status, "InsertColumnData failed"); err != nil {
		return err
	}

	s.setRecentlyModified(true)
	return nil
}

func (s *Segment) segmentDelete(offset int64, entityIDs *[]UniqueID, timestamps *[]Timestamp) error {
	/*
		CStatus
		Delete(CSegmentInterface c_segment,
		           long int reserved_offset,
		           long size,
		           const long* primary_keys,
		           const unsigned long* timestamps);
	*/
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	if len(*entityIDs) != len(*timestamps) {
		return errors.New("length of entityIDs not equal to length of timestamps")
	}

	var cOffset = C.int64_t(offset)
	var cSize = C.int64_t(len(*entityIDs))
	var cEntityIdsPtr = (*C.int64_t)(&(*entityIDs)[0])
	var cTimestampsPtr = (*C.uint64_t)(&(*timestamps)[0])

	status := C.Delete(s.segmentPtr, cOffset, cSize, cEntityIdsPtr, cTimestampsPtr)
	if err := HandleCStatus(&status, "Delete failed"); err != nil {
		return err
	}

	return nil
}

//-------------------------------------------------------------------------------------- interfaces for sealed segment
func (s *Segment) segmentLoadFieldData(fieldID int64, rowCount int, data interface{}) error {
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

	// data interface check
	var dataPointer unsafe.Pointer
	emptyErr := errors.New("null field data to be loaded")
	switch d := data.(type) {
	case []bool:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []byte:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []int8:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []int16:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []int32:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []int64:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []float32:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []float64:
		if len(d) <= 0 {
			return emptyErr
		}
		dataPointer = unsafe.Pointer(&d[0])
	case []string:
		// TODO: support string type
		return errors.New("we cannot support string type now")
	default:
		return errors.New("illegal field data type")
	}

	/*
		typedef struct CLoadFieldDataInfo {
		    int64_t field_id;
		    void* blob;
		    int64_t row_count;
		} CLoadFieldDataInfo;
	*/
	loadInfo := C.CLoadFieldDataInfo{
		field_id:  C.int64_t(fieldID),
		blob:      dataPointer,
		row_count: C.int64_t(rowCount),
	}

	status := C.LoadFieldData(s.segmentPtr, loadInfo)
	if err := HandleCStatus(&status, "LoadFieldData failed"); err != nil {
		return err
	}

	log.Debug("load field done",
		zap.Int64("fieldID", fieldID),
		zap.Int("row count", rowCount),
		zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *Segment) segmentLoadDeletedRecord(primaryKeys []IntPrimaryKey, timestamps []Timestamp, rowCount int64) error {
	s.segPtrMu.RLock()
	defer s.segPtrMu.RUnlock() // thread safe guaranteed by segCore, use RLock
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	if s.segmentType != segmentTypeSealed {
		errMsg := fmt.Sprintln("segmentLoadFieldData failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}
	loadInfo := C.CLoadDeletedRecordInfo{
		timestamps:   unsafe.Pointer(&timestamps[0]),
		primary_keys: unsafe.Pointer(&primaryKeys[0]),
		row_count:    C.int64_t(rowCount),
	}
	/*
		CStatus
		LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info)
	*/
	status := C.LoadDeletedRecord(s.segmentPtr, loadInfo)
	if err := HandleCStatus(&status, "LoadDeletedRecord failed"); err != nil {
		return err
	}

	log.Debug("load deleted record done",
		zap.Int64("row count", rowCount),
		zap.Int64("segmentID", s.ID()))
	return nil
}

func (s *Segment) segmentLoadIndexData(bytesIndex [][]byte, indexInfo *querypb.VecFieldIndexInfo) error {
	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}

	err = loadIndexInfo.appendIndexInfo(bytesIndex, indexInfo)
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

	log.Debug("updateSegmentIndex done", zap.Int64("segmentID", s.ID()))

	return nil
}
