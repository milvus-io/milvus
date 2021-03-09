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
	"strconv"
	"sync"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"errors"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

const (
	segTypeInvalid = iota
	segTypeGrowing
	segTypeSealed
	segTypeIndexing
)

type segmentType = int
type indexParam = map[string]string

type Segment struct {
	segmentPtr C.CSegmentInterface

	segmentID    UniqueID
	partitionID  UniqueID
	collectionID UniqueID
	lastMemSize  int64
	lastRowCount int64

	rmMutex          sync.Mutex // guards recentlyModified
	recentlyModified bool

	typeMu      sync.Mutex // guards builtIndex
	segmentType int

	paramMutex sync.RWMutex // guards index
	indexParam map[int64]indexParam
	indexName  string
	indexID    UniqueID
}

//-------------------------------------------------------------------------------------- common interfaces
func (s *Segment) ID() UniqueID {
	return s.segmentID
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

func (s *Segment) setIndexName(name string) {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
	s.indexName = name
}

func (s *Segment) getIndexName() string {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
	return s.indexName
}

func (s *Segment) setIndexID(id UniqueID) {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
	s.indexID = id
}

func (s *Segment) getIndexID() UniqueID {
	s.rmMutex.Lock()
	defer s.rmMutex.Unlock()
	return s.indexID
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

func newSegment(collection *Collection, segmentID int64, partitionID UniqueID, collectionID UniqueID, segType segmentType) *Segment {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/
	initIndexParam := make(map[int64]indexParam)
	var segmentPtr C.CSegmentInterface
	switch segType {
	case segTypeInvalid:
		log.Error("illegal segment type when create segment")
		return nil
	case segTypeSealed:
		segmentPtr = C.NewSegment(collection.collectionPtr, C.ulong(segmentID), C.Sealed)
	case segTypeGrowing:
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
		indexParam:   initIndexParam,
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
	placeHolderGroups []*PlaceholderGroup,
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
	for _, pg := range placeHolderGroups {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}

	var searchResult SearchResult
	var cTimestamp = (*C.ulong)(&timestamp[0])
	var cPlaceHolder = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	var cNumGroups = C.int(len(placeHolderGroups))

	log.Debug("do search on segment", zap.Int64("segmentID", s.segmentID), zap.Int("segType", s.segmentType))
	var status = C.Search(s.segmentPtr, plan.cPlan, cPlaceHolder, cTimestamp, cNumGroups, &searchResult.cQueryResult)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("Search failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return &searchResult, nil
}

func (s *Segment) fillTargetEntry(plan *Plan,
	result *SearchResult) error {
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}

	var status = C.FillTargetEntry(s.segmentPtr, plan.cPlan, result.cQueryResult)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("FillTargetEntry failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return nil
}

// segment, err := loadService.replica.getSegmentByID(segmentID)
func (s *Segment) updateSegmentIndex(loadIndexInfo *LoadIndexInfo) error {
	if s.segmentPtr == nil {
		return errors.New("null seg core pointer")
	}
	var status C.CStatus

	if s.segmentType == segTypeGrowing {
		status = C.UpdateSegmentIndex(s.segmentPtr, loadIndexInfo.cLoadIndexInfo)
	} else if s.segmentType == segTypeSealed {
		status = C.UpdateSealedSegmentIndex(s.segmentPtr, loadIndexInfo.cLoadIndexInfo)
	} else {
		return errors.New("illegal segment type")
	}

	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("updateSegmentIndex failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	s.setType(segTypeIndexing)

	return nil
}

func (s *Segment) setIndexParam(fieldID int64, indexParamKv []*commonpb.KeyValuePair) error {
	s.paramMutex.Lock()
	defer s.paramMutex.Unlock()
	indexParamMap := make(indexParam)
	if indexParamKv == nil {
		return errors.New("empty loadIndexMsg's indexParam")
	}
	for _, param := range indexParamKv {
		indexParamMap[param.Key] = param.Value
	}
	s.indexParam[fieldID] = indexParamMap
	return nil
}

func (s *Segment) matchIndexParam(fieldID int64, indexParams indexParam) bool {
	s.paramMutex.RLock()
	defer s.paramMutex.RUnlock()
	fieldIndexParam := s.indexParam[fieldID]
	if fieldIndexParam == nil {
		return false
	}
	paramSize := len(s.indexParam)
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

//-------------------------------------------------------------------------------------- interfaces for growing segment
func (s *Segment) segmentPreInsert(numOfRecords int) int64 {
	/*
		long int
		PreInsert(CSegmentInterface c_segment, long int size);
	*/
	var offset = C.PreInsert(s.segmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
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

	var status = C.Insert(s.segmentPtr,
		cOffset,
		cNumOfRows,
		cEntityIdsPtr,
		cTimestampsPtr,
		cRawDataVoidPtr,
		cSizeofPerRow,
		cNumOfRows)

	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("Insert failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
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
	if s.segmentType != segTypeSealed {
		return errors.New("illegal segment type when loading field data")
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

	log.Debug("load field done", zap.Int64("fieldID", fieldID), zap.Int("row count", rowCount))

	return nil
}
