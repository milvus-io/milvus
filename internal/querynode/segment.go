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

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

const (
	segTypeInvalid  = C.Invalid
	segTypeGrowing  = C.Growing
	segTypeSealed   = C.Sealed
	segTypeIndexing = C.Indexing
)

type segmentType = C.SegmentType
type indexParam = map[string]string

type Segment struct {
	segmentPtr C.CSegmentInterface

	segmentID    UniqueID
	partitionTag string // TODO: use partitionID
	partitionID  UniqueID
	collectionID UniqueID
	lastMemSize  int64
	lastRowCount int64

	rmMutex          sync.Mutex // guards recentlyModified
	recentlyModified bool

	typeMu      sync.Mutex // guards builtIndex
	segmentType C.SegmentType

	paramMutex sync.RWMutex // guards indexParam
	indexParam map[int64]indexParam
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

func newSegment2(collection *Collection, segmentID int64, partitionTag string, collectionID UniqueID, segType segmentType) *Segment {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/
	initIndexParam := make(map[int64]indexParam)
	segmentPtr := C.NewSegment(collection.collectionPtr, C.ulong(segmentID), segType)
	var newSegment = &Segment{
		segmentPtr:   segmentPtr,
		segmentType:  segType,
		segmentID:    segmentID,
		partitionTag: partitionTag,
		collectionID: collectionID,
		indexParam:   initIndexParam,
	}

	return newSegment
}

func newSegment(collection *Collection, segmentID int64, partitionID UniqueID, collectionID UniqueID, segType segmentType) *Segment {
	/*
		CSegmentInterface
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);
	*/
	initIndexParam := make(map[int64]indexParam)
	segmentPtr := C.NewSegment(collection.collectionPtr, C.ulong(segmentID), segType)
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
}

func (s *Segment) getRowCount() int64 {
	/*
		long int
		getRowCount(CSegmentInterface c_segment);
	*/
	var rowCount = C.GetRowCount(s.segmentPtr)
	return int64(rowCount)
}

func (s *Segment) getDeletedCount() int64 {
	/*
		long int
		getDeletedCount(CSegmentInterface c_segment);
	*/
	var deletedCount = C.GetDeletedCount(s.segmentPtr)
	return int64(deletedCount)
}

func (s *Segment) getMemSize() int64 {
	/*
		long int
		GetMemoryUsageInBytes(CSegmentInterface c_segment);
	*/
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

	cPlaceholderGroups := make([]C.CPlaceholderGroup, 0)
	for _, pg := range placeHolderGroups {
		cPlaceholderGroups = append(cPlaceholderGroups, (*pg).cPlaceholderGroup)
	}

	var searchResult SearchResult
	var cTimestamp = (*C.ulong)(&timestamp[0])
	var cPlaceHolder = (*C.CPlaceholderGroup)(&cPlaceholderGroups[0])
	var cNumGroups = C.int(len(placeHolderGroups))
	var cQueryResult = (*C.CQueryResult)(&searchResult.cQueryResult)

	var status = C.Search(s.segmentPtr, plan.cPlan, cPlaceHolder, cTimestamp, cNumGroups, cQueryResult)
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
		return errors.New("loadIndexMsg's indexParam empty")
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

	return nil
}
