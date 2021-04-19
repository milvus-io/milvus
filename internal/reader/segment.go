package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"
import (
	"strconv"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	servicePb "github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type Segment struct {
	segmentPtr       C.CSegmentBase
	segmentID        UniqueID
	lastMemSize      int64
	lastRowCount     int64
	recentlyModified bool
}

func (s *Segment) ID() UniqueID {
	return s.segmentID
}

//-------------------------------------------------------------------------------------- constructor and destructor
func newSegment(collection *Collection, segmentID int64) *Segment {
	/*
		CSegmentBase
		newSegment(CPartition partition, unsigned long segment_id);
	*/
	var tmp C.CPartition
	segmentPtr := C.NewSegment(tmp, C.ulong(segmentID))
	var newSegment = &Segment{segmentPtr: segmentPtr, segmentID: segmentID}

	return newSegment
}

func deleteSegment(segment *Segment) {
	/*
		void
		deleteSegment(CSegmentBase segment);
	*/
	cPtr := segment.segmentPtr
	C.DeleteSegment(cPtr)
}

//-------------------------------------------------------------------------------------- stats functions
func (s *Segment) getRowCount() int64 {
	/*
		long int
		getRowCount(CSegmentBase c_segment);
	*/
	var rowCount = C.GetRowCount(s.segmentPtr)
	return int64(rowCount)
}

func (s *Segment) getDeletedCount() int64 {
	/*
		long int
		getDeletedCount(CSegmentBase c_segment);
	*/
	var deletedCount = C.GetDeletedCount(s.segmentPtr)
	return int64(deletedCount)
}

func (s *Segment) getMemSize() int64 {
	/*
		long int
		GetMemoryUsageInBytes(CSegmentBase c_segment);
	*/
	var memoryUsageInBytes = C.GetMemoryUsageInBytes(s.segmentPtr)

	return int64(memoryUsageInBytes)
}

//-------------------------------------------------------------------------------------- preprocess functions
func (s *Segment) segmentPreInsert(numOfRecords int) int64 {
	/*
		long int
		PreInsert(CSegmentBase c_segment, long int size);
	*/
	var offset = C.PreInsert(s.segmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

func (s *Segment) segmentPreDelete(numOfRecords int) int64 {
	/*
		long int
		PreDelete(CSegmentBase c_segment, long int size);
	*/
	var offset = C.PreDelete(s.segmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

//-------------------------------------------------------------------------------------- dm & search functions
func (s *Segment) segmentInsert(offset int64, entityIDs *[]UniqueID, timestamps *[]Timestamp, records *[]*commonpb.Blob) error {
	/*
		int
		Insert(CSegmentBase c_segment,
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

	if status != 0 {
		return errors.New("Insert failed, error code = " + strconv.Itoa(int(status)))
	}

	return nil
}

func (s *Segment) segmentDelete(offset int64, entityIDs *[]UniqueID, timestamps *[]Timestamp) error {
	/*
		int
		Delete(CSegmentBase c_segment,
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

	if status != 0 {
		return errors.New("Delete failed, error code = " + strconv.Itoa(int(status)))
	}

	return nil
}

func (s *Segment) segmentSearch(query *queryInfo, timestamp Timestamp, vectorRecord *servicePb.PlaceholderValue) (*SearchResult, error) {
	/*
	*/
	//type CQueryInfo C.CQueryInfo


	/*
		void* Search(void* plan, void* placeholder_groups, uint64_t* timestamps, int num_groups, long int* result_ids,
		       float* result_distances)
	*/


	cQuery := C.CQueryInfo{
		num_queries: C.long(query.NumQueries),
		topK:        C.int(query.TopK),
		field_name:  C.CString(query.FieldName),
	}

	resultIds := make([]IntPrimaryKey, int64(query.TopK)*query.NumQueries)
	resultDistances := make([]float32, int64(query.TopK)*query.NumQueries)

	var cTimestamp = C.ulong(timestamp)
	var cResultIds = (*C.long)(&resultIds[0])
	var cResultDistances = (*C.float)(&resultDistances[0])
	var cQueryRawData *C.float
	var cQueryRawDataLength C.int

	//if vectorRecord.BinaryData != nil {
	//	return nil, errors.New("data of binary type is not supported yet")
	//} else if len(vectorRecord.FloatData) <= 0 {
	//	return nil, errors.New("null query vector data")
	//} else {
	//	cQueryRawData = (*C.float)(&vectorRecord.FloatData[0])
	//	cQueryRawDataLength = (C.int)(len(vectorRecord.FloatData))
	//}

	var status = C.Search(s.segmentPtr, cQuery, cTimestamp, cQueryRawData, cQueryRawDataLength, cResultIds, cResultDistances)

	if status != 0 {
		return nil, errors.New("search failed, error code = " + strconv.Itoa(int(status)))
	}

	//fmt.Println("search Result---- Ids =", resultIds, ", Distances =", resultDistances)

	return &SearchResult{ResultIds: resultIds, ResultDistances: resultDistances}, nil
}
