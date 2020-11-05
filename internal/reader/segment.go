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
	"github.com/stretchr/testify/assert"
	"strconv"
	"unsafe"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type IntPrimaryKey = typeutil.IntPrimaryKey

type Segment struct {
	SegmentPtr       C.CSegmentBase
	SegmentID        UniqueID
	SegmentCloseTime Timestamp
	LastMemSize      int64
	SegmentStatus    int
	recentlyModified bool
}

func (s *Segment) getRowCount() int64 {
	/*
		long int
		getRowCount(CSegmentBase c_segment);
	*/
	var rowCount = C.GetRowCount(s.SegmentPtr)
	return int64(rowCount)
}

func (s *Segment) getDeletedCount() int64 {
	/*
		long int
		getDeletedCount(CSegmentBase c_segment);
	*/
	var deletedCount = C.GetDeletedCount(s.SegmentPtr)
	return int64(deletedCount)
}

func (s *Segment) getMemSize() int64 {
	/*
		long int
		GetMemoryUsageInBytes(CSegmentBase c_segment);
	*/
	var memoryUsageInBytes = C.GetMemoryUsageInBytes(s.SegmentPtr)

	return int64(memoryUsageInBytes)
}

////////////////////////////////////////////////////////////////////////////
func (s *Segment) segmentPreInsert(numOfRecords int) int64 {
	/*
		long int
		PreInsert(CSegmentBase c_segment, long int size);
	*/
	var offset = C.PreInsert(s.SegmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

func (s *Segment) segmentPreDelete(numOfRecords int) int64 {
	/*
		long int
		PreDelete(CSegmentBase c_segment, long int size);
	*/
	var offset = C.PreDelete(s.SegmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

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

	var status = C.Insert(s.SegmentPtr,
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

	var status = C.Delete(s.SegmentPtr, cOffset, cSize, cEntityIdsPtr, cTimestampsPtr)

	if status != 0 {
		return errors.New("Delete failed, error code = " + strconv.Itoa(int(status)))
	}

	return nil
}

func (s *Segment) segmentSearch(query *queryInfo, timestamp Timestamp, vectorRecord *msgPb.VectorRowRecord) (*SearchResult, error) {
	/*
		int
		search(CSegmentBase c_segment,
		       CQueryInfo  c_query_info,
		       unsigned long timestamp,
		       float* query_raw_data,
		       int num_of_query_raw_data,
		       long int* result_ids,
		       float* result_distances);
	*/
	//type CQueryInfo C.CQueryInfo

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

	if vectorRecord.BinaryData != nil {
		return nil, errors.New("data of binary type is not supported yet")
	} else if len(vectorRecord.FloatData) <= 0 {
		return nil, errors.New("null query vector data")
	} else {
		cQueryRawData = (*C.float)(&vectorRecord.FloatData[0])
		cQueryRawDataLength = (C.int)(len(vectorRecord.FloatData))
	}

	var status = C.Search(s.SegmentPtr, cQuery, cTimestamp, cQueryRawData, cQueryRawDataLength, cResultIds, cResultDistances)

	if status != 0 {
		return nil, errors.New("search failed, error code = " + strconv.Itoa(int(status)))
	}

	//fmt.Println("search Result---- Ids =", resultIds, ", Distances =", resultDistances)

	return &SearchResult{ResultIds: resultIds, ResultDistances: resultDistances}, nil
}
