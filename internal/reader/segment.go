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
	"github.com/zilliztech/milvus-distributed/internal/errors"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"strconv"
	"unsafe"
)

const SegmentLifetime = 20000

const (
//SegmentOpened  = 0
//SegmentClosed  = 1
//SegmentIndexing = 2
//SegmentIndexed = 3
)

type Segment struct {
	SegmentPtr       C.CSegmentBase
	SegmentId        int64
	SegmentCloseTime uint64
	LastMemSize      int64
	SegmentStatus    int
}

//func (s *Segment) GetStatus() int {
//	/*
//	bool
//	IsOpened(CSegmentBase c_segment);
//	*/
//	var isOpened = C.IsOpened(s.SegmentPtr)
//	if isOpened {
//		return SegmentOpened
//	} else {
//		return SegmentClosed
//	}
//}

func (s *Segment) GetRowCount() int64 {
	/*
		long int
		GetRowCount(CSegmentBase c_segment);
	*/
	var rowCount = C.GetRowCount(s.SegmentPtr)
	return int64(rowCount)
}

func (s *Segment) GetDeletedCount() int64 {
	/*
		long int
		GetDeletedCount(CSegmentBase c_segment);
	*/
	var deletedCount = C.GetDeletedCount(s.SegmentPtr)
	return int64(deletedCount)
}

//func (s *Segment) CloseSegment(collection* Collection) error {
//	/*
//	int
//	Close(CSegmentBase c_segment);
//	*/
//	fmt.Println("Closing segment :", s.SegmentId)
//
//	var status = C.Close(s.SegmentPtr)
//	s.SegmentStatus = SegmentClosed
//
//	if status != 0 {
//		return errors.New("Close segment failed, error code = " + strconv.Itoa(int(status)))
//	}
//
//	// Build index after closing segment
//	//s.SegmentStatus = SegmentIndexing
//	//fmt.Println("Building index...")
//	//s.BuildIndex(collection)
//
//	// TODO: remove redundant segment indexed status
//	// Change segment status to indexed
//	//s.SegmentStatus = SegmentIndexed
//	//fmt.Println("Segment closed and indexed")
//
//	fmt.Println("Segment closed")
//	return nil
//}

func (s *Segment) GetMemSize() int64 {
	/*
		long int
		GetMemoryUsageInBytes(CSegmentBase c_segment);
	*/
	var memoryUsageInBytes = C.GetMemoryUsageInBytes(s.SegmentPtr)

	return int64(memoryUsageInBytes)
}

////////////////////////////////////////////////////////////////////////////
func (s *Segment) SegmentPreInsert(numOfRecords int) int64 {
	/*
		long int
		PreInsert(CSegmentBase c_segment, long int size);
	*/
	var offset = C.PreInsert(s.SegmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

func (s *Segment) SegmentPreDelete(numOfRecords int) int64 {
	/*
		long int
		PreDelete(CSegmentBase c_segment, long int size);
	*/
	var offset = C.PreDelete(s.SegmentPtr, C.long(int64(numOfRecords)))

	return int64(offset)
}

func (s *Segment) SegmentInsert(offset int64, entityIDs *[]int64, timestamps *[]uint64, records *[][]byte) error {
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
	var sizeofPerRow = len((*records)[0])

	assert.Equal(nil, numOfRow, len(*records))

	var rawData = make([]byte, numOfRow*sizeofPerRow)
	var copyOffset = 0
	for i := 0; i < len(*records); i++ {
		copy(rawData[copyOffset:], (*records)[i])
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

func (s *Segment) SegmentDelete(offset int64, entityIDs *[]int64, timestamps *[]uint64) error {
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

func (s *Segment) SegmentSearch(query *QueryInfo, timestamp uint64, vectorRecord *msgPb.VectorRowRecord) (*SearchResult, error) {
	/*
		int
		Search(CSegmentBase c_segment,
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

	resultIds := make([]int64, int64(query.TopK)*query.NumQueries)
	resultDistances := make([]float32, int64(query.TopK)*query.NumQueries)

	var cTimestamp = C.ulong(timestamp)
	var cResultIds = (*C.long)(&resultIds[0])
	var cResultDistances = (*C.float)(&resultDistances[0])
	var cQueryRawData *C.float
	var cQueryRawDataLength C.int

	if vectorRecord.BinaryData != nil {
		return nil, errors.New("Data of binary type is not supported yet")
	} else if len(vectorRecord.FloatData) <= 0 {
		return nil, errors.New("Null query vector data")
	} else {
		cQueryRawData = (*C.float)(&vectorRecord.FloatData[0])
		cQueryRawDataLength = (C.int)(len(vectorRecord.FloatData))
	}

	var status = C.Search(s.SegmentPtr, cQuery, cTimestamp, cQueryRawData, cQueryRawDataLength, cResultIds, cResultDistances)

	if status != 0 {
		return nil, errors.New("Search failed, error code = " + strconv.Itoa(int(status)))
	}

	//fmt.Println("Search Result---- Ids =", resultIds, ", Distances =", resultDistances)

	return &SearchResult{ResultIds: resultIds, ResultDistances: resultDistances}, nil
}
