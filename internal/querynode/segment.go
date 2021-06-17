// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

type segmentType int32

const (
	segmentTypeInvalid segmentType = iota
	segmentTypeGrowing
	segmentTypeSealed
	segmentTypeIndexing
)

type Segment struct {
	segmentPtr C.CSegmentInterface

	segmentID    UniqueID
	partitionID  UniqueID
	collectionID UniqueID

	onService bool

	vChannelID   Channel
	lastMemSize  int64
	lastRowCount int64

	once        sync.Once // guards enableIndex
	enableIndex bool

	rmMutex          sync.Mutex // guards recentlyModified
	recentlyModified bool

	typeMu      sync.Mutex // guards builtIndex
	segmentType segmentType

	paramMutex sync.RWMutex // guards index
	indexInfos map[int64]*indexInfo
}

//-------------------------------------------------------------------------------------- common interfaces
func (s *Segment) ID() UniqueID {
	return s.segmentID
}

func (s *Segment) setEnableIndex(enable bool) {
	setOnce := func() {
		s.enableIndex = enable
	}

	s.once.Do(setOnce)
}

func (s *Segment) getEnableIndex() bool {
	return s.enableIndex
}

func (s *Segment) setRecentlyModified(modify bool) {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
	s.recentlyModified = modify
}

func (s *Segment) getRecentlyModified() bool {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
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

func newSegment(collection *Collection, segmentID int64, partitionID UniqueID, collectionID UniqueID, vChannelID Channel, segType segmentType, onService bool) *Segment {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/
	indexInfos := make(map[int64]*indexInfo)
	var segmentPtr C.CSegmentInterface
	switch segType {
	case segmentTypeInvalid:
		log.Error("illegal segment type when create segment")
		return nil
	case segmentTypeSealed:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.ulong(segmentID), C.Sealed)
	case segmentTypeGrowing:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.ulong(segmentID), C.Growing)
	default:
		log.Error("illegal segment type when create segment")
		return nil
	}

	log.Debug("create segment", zap.Int64("segmentID", segmentID))

	var newSegment = &Segment{
		segmentPtr:   segmentPtr,
		segmentType:  segType,
		segmentID:    segmentID,
		partitionID:  partitionID,
		collectionID: collectionID,
		vChannelID:   vChannelID,
		onService:    onService,
		indexInfos:   indexInfos,
	}

	return newSegment
}

func deleteSegment(segment *Segment) {
	/*
		void
		deleteSegment(CSegmentInterface segment);
	*/
	cPtr := segment.segmentPtr
	C.DeleteSegment(cPtr)
	segment.segmentPtr = nil

	log.Debug("delete segment", zap.Int64("segmentID", segment.ID()))

	segment = nil
}

func (s *Segment) getRowCount() int64 {
	/*
		long int
		getRowCount(CSegmentInterface c_segment);
	*/
	segmentPtrIsNil := s.segmentPtr == nil
	log.Debug("QueryNode::Segment::getRowCount", zap.Any("segmentPtrIsNil", segmentPtrIsNil))
	if s.segmentPtr == nil {
		return -1
	}
	var rowCount = C.GetRowCount(s.segmentPtr)
	log.Debug("QueryNode::Segment::getRowCount", zap.Any("rowCount", rowCount))
	return int64(rowCount)
}

func (s *Segment) getDeletedCount() int64 {
	/*
		long int
		getDeletedCount(CSegmentInterface c_segment);
	*/
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
	if s.segmentPtr == nil {
		return -1
	}
	var memoryUsageInBytes = C.GetMemoryUsageInBytes(s.segmentPtr)

	return int64(memoryUsageInBytes)
}

func (s *Segment) segmentSearch(plan *Plan,
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
	if s.segmentPtr == nil {
		return nil, errors.New("null seg core pointer")
	}
	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range searchRequests {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}

	var searchResult SearchResult
	var cTimestamp = (*C.ulong)(&timestamp[0])
	var cPlaceHolder = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	var cNumGroups = C.int(len(searchRequests))

	log.Debug("do search on segment", zap.Int64("segmentID", s.segmentID), zap.Int32("segmentType", int32(s.segmentType)))
	var status = C.Search(s.segmentPtr, plan.cPlan, cPlaceHolder, cTimestamp, cNumGroups, &searchResult.cQueryResult)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("Search failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return &searchResult, nil
}

func (s *Segment) segmentGetEntityByIds(plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	resProto := C.GetEntityByIds(s.segmentPtr, plan.RetrievePlanPtr, C.uint64_t(plan.Timestamp))
	result := new(segcorepb.RetrieveResults)
	err := HandleCProtoResult(&resProto, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Segment) fillTargetEntry(plan *Plan,
	result *SearchResult) error {
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	log.Debug("segment fill target entry, ", zap.Int64("segment ID = ", s.segmentID))
	var status = C.FillTargetEntry(s.segmentPtr, plan.cPlan, result.cQueryResult)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("FillTargetEntry failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return nil
}

//-------------------------------------------------------------------------------------- index info interface
func (s *Segment) setIndexName(fieldID int64, name string) error {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return errors.New("index info hasn't been init")
	}
	s.indexInfos[fieldID].setIndexName(name)
	return nil
}

func (s *Segment) setIndexParam(fieldID int64, indexParams map[string]string) error {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if indexParams == nil {
		return errors.New("empty loadIndexMsg's indexParam")
	}
	if _, ok := s.indexInfos[fieldID]; !ok {
		return errors.New("index info hasn't been init")
	}
	s.indexInfos[fieldID].setIndexParams(indexParams)
	return nil
}

func (s *Segment) setIndexPaths(fieldID int64, indexPaths []string) error {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return errors.New("index info hasn't been init")
	}
	s.indexInfos[fieldID].setIndexPaths(indexPaths)
	return nil
}

func (s *Segment) setIndexID(fieldID int64, id UniqueID) error {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return errors.New("index info hasn't been init")
	}
	s.indexInfos[fieldID].setIndexID(id)
	return nil
}

func (s *Segment) setBuildID(fieldID int64, id UniqueID) error {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return errors.New("index info hasn't been init")
	}
	s.indexInfos[fieldID].setBuildID(id)
	return nil
}

func (s *Segment) getIndexName(fieldID int64) string {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return ""
	}
	return s.indexInfos[fieldID].getIndexName()
}

func (s *Segment) getIndexID(fieldID int64) UniqueID {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return -1
	}
	return s.indexInfos[fieldID].getIndexID()
}

func (s *Segment) getBuildID(fieldID int64) UniqueID {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return -1
	}
	return s.indexInfos[fieldID].getBuildID()
}

func (s *Segment) getIndexPaths(fieldID int64) []string {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return nil
	}
	return s.indexInfos[fieldID].getIndexPaths()
}

func (s *Segment) getIndexParams(fieldID int64) map[string]string {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return nil
	}
	return s.indexInfos[fieldID].getIndexParams()
}

func (s *Segment) matchIndexParam(fieldID int64, indexParams indexParam) bool {
	s.paramMutex.RLock()
	defer s.paramMutex.RUnlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return false
	}
	fieldIndexParam := s.indexInfos[fieldID].getIndexParams()
	if fieldIndexParam == nil {
		return false
	}
	paramSize := len(s.indexInfos)
	matchCount := 0
	for k, v := range indexParams {
		value, ok := fieldIndexParam[k]
		if !ok {
			return false
		}
		if v != value {
			return false
		}
		matchCount++
	}
	return paramSize == matchCount
}

func (s *Segment) setIndexInfo(fieldID int64, info *indexInfo) error {
	s.paramMutex.RLock()
	defer s.paramMutex.RUnlock()
	if s.indexInfos == nil {
		return errors.New("indexInfos hasn't been init")
	}
	s.indexInfos[fieldID] = info
	return nil
}

func (s *Segment) checkIndexReady(fieldID int64) bool {
	s.paramMutex.RLock()
	defer s.paramMutex.RUnlock()
	if _, ok := s.indexInfos[fieldID]; !ok {
		return false
	}
	return s.indexInfos[fieldID].getReadyLoad()
}

//-------------------------------------------------------------------------------------- interfaces for growing segment
func (s *Segment) segmentPreInsert(numOfRecords int) (int64, error) {
	/*
		long int
		PreInsert(CSegmentInterface c_segment, long int size);
	*/
	if s.segmentType != segmentTypeGrowing {
		return 0, nil
	}
	var offset int64
	cOffset := (*C.long)(&offset)
	status := C.PreInsert(s.segmentPtr, C.long(int64(numOfRecords)), cOffset)

	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return 0, errors.New("PreInsert failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return offset, nil
}

func (s *Segment) segmentPreDelete(numOfRecords int) int64 {
	/*
		long int
		PreDelete(CSegmentInterface c_segment, long int size);
	*/
	var offset = C.PreDelete(s.segmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

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
	if s.segmentType != segmentTypeGrowing {
		return nil
	}
	log.Debug("QueryNode::Segment::segmentInsert:", zap.Any("s.sgmentPtr", s.segmentPtr))

	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	// Blobs to one big blob
	var numOfRow = len(*entityIDs)
	var sizeofPerRow = len((*records)[0].Value)

	assert.Equal(nil, numOfRow, len(*records))

	var rawData = make([]byte, numOfRow*sizeofPerRow)
	var copyOffset = 0
	for i := 0; i < len(*records); i++ {
		copy(rawData[copyOffset:], (*records)[i].Value)
		copyOffset += sizeofPerRow
	}

	var cOffset = C.long(offset)
	var cNumOfRows = C.long(numOfRow)
	var cEntityIdsPtr = (*C.long)(&(*entityIDs)[0])
	var cTimestampsPtr = (*C.ulong)(&(*timestamps)[0])
	var cSizeofPerRow = C.int(sizeofPerRow)
	var cRawDataVoidPtr = unsafe.Pointer(&rawData[0])
	log.Debug("QueryNode::Segment::InsertBegin", zap.Any("cNumOfRows", cNumOfRows))
	var status = C.Insert(s.segmentPtr,
		cOffset,
		cNumOfRows,
		cEntityIdsPtr,
		cTimestampsPtr,
		cRawDataVoidPtr,
		cSizeofPerRow,
		cNumOfRows)

	errorCode := status.error_code
	log.Debug("QueryNode::Segment::InsertEnd", zap.Any("errorCode", errorCode))

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		err := errors.New("Insert failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
		log.Debug("QueryNode::Segment::InsertEnd failed", zap.Error(err))
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
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	var cOffset = C.long(offset)
	var cSize = C.long(len(*entityIDs))
	var cEntityIdsPtr = (*C.long)(&(*entityIDs)[0])
	var cTimestampsPtr = (*C.ulong)(&(*timestamps)[0])

	var status = C.Delete(s.segmentPtr, cOffset, cSize, cEntityIdsPtr, cTimestampsPtr)

	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("Delete failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return nil
}

//-------------------------------------------------------------------------------------- interfaces for sealed segment
func (s *Segment) segmentLoadFieldData(fieldID int64, rowCount int, data interface{}) error {
	/*
		CStatus
		LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info);
	*/
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

	var status = C.LoadFieldData(s.segmentPtr, loadInfo)
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("LoadFieldData failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	log.Debug("load field done",
		zap.Int64("fieldID", fieldID),
		zap.Int("row count", rowCount),
		zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *Segment) dropFieldData(fieldID int64) error {
	/*
		CStatus
		DropFieldData(CSegmentInterface c_segment, int64_t field_id);
	*/
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	if s.segmentType != segmentTypeIndexing {
		errMsg := fmt.Sprintln("dropFieldData failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}

	var status = C.DropFieldData(s.segmentPtr, C.long(fieldID))
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("dropFieldData failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	log.Debug("dropFieldData done", zap.Int64("fieldID", fieldID), zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *Segment) updateSegmentIndex(bytesIndex [][]byte, fieldID UniqueID) error {
	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}
	err = loadIndexInfo.appendFieldInfo(fieldID)
	if err != nil {
		return err
	}
	indexParams := s.getIndexParams(fieldID)
	for k, v := range indexParams {
		err = loadIndexInfo.appendIndexParam(k, v)
		if err != nil {
			return err
		}
	}
	indexPaths := s.getIndexPaths(fieldID)
	err = loadIndexInfo.appendIndex(bytesIndex, indexPaths)
	if err != nil {
		return err
	}

	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	if s.segmentType != segmentTypeSealed {
		errMsg := fmt.Sprintln("updateSegmentIndex failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}

	status := C.UpdateSealedSegmentIndex(s.segmentPtr, loadIndexInfo.cLoadIndexInfo)
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("updateSegmentIndex failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	s.setType(segmentTypeIndexing)
	log.Debug("updateSegmentIndex done", zap.Int64("segmentID", s.ID()))

	return nil
}

func (s *Segment) dropSegmentIndex(fieldID int64) error {
	/*
		CStatus
		DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id);
	*/
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	if s.segmentType != segmentTypeIndexing {
		errMsg := fmt.Sprintln("dropFieldData failed, illegal segment type ", s.segmentType, "segmentID = ", s.ID())
		return errors.New(errMsg)
	}

	var status = C.DropSealedSegmentIndex(s.segmentPtr, C.long(fieldID))
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("dropSegmentIndex failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	log.Debug("dropSegmentIndex done", zap.Int64("fieldID", fieldID), zap.Int64("segmentID", s.ID()))

	return nil
}
