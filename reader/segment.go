package reader

/*

#cgo CFLAGS: -I../core/include

#cgo LDFLAGS: -L../core/lib -lmilvus_dog_segment -Wl,-rpath=../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"
import (
	"github.com/czs007/suvlim/errors"
	schema "github.com/czs007/suvlim/pkg/message"
	"strconv"
	"unsafe"
)

const SegmentLifetime = 20000

const (
	SegmentOpened = 0
	SegmentClosed = 1
)

type Segment struct {
	SegmentPtr C.CSegmentBase
	SegmentId	int64
	SegmentCloseTime uint64
}

func (s *Segment) GetStatus() int {
	/*C.IsOpened
	bool
	IsOpened(CSegmentBase c_segment);
	*/
	var isOpened = C.IsOpened(s.SegmentPtr)
	if isOpened {
		return SegmentOpened
	} else {
		return SegmentClosed
	}
}

func (s *Segment) GetRowCount() int64 {
	/*C.GetRowCount
	long int
	GetRowCount(CSegmentBase c_segment);
	*/
	var rowCount = C.GetRowCount(s.SegmentPtr)
	return int64(rowCount)
}

func (s *Segment) GetDeletedCount() int64 {
	/*C.GetDeletedCount
	long int
	GetDeletedCount(CSegmentBase c_segment);
	*/
	var deletedCount = C.GetDeletedCount(s.SegmentPtr)
	return int64(deletedCount)
}

func (s *Segment) Close() error {
	/*C.Close
	int
	Close(CSegmentBase c_segment);
	*/
	var status = C.Close(s.SegmentPtr)
	if status != 0 {
		return errors.New("Close segment failed, error code = " + strconv.Itoa(int(status)))
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////
func (s *Segment) SegmentPreInsert(numOfRecords int) int64 {
	var offset = C.PreInsert(numOfRecords)

	return offset
}

func (s *Segment) SegmentPreDelete(numOfRecords int) int64 {
	var offset = C.PreDelete(numOfRecords)

	return offset
}

func (s *Segment) SegmentInsert(entityIds *[]int64, timestamps *[]uint64, records *[][]byte) error {
	/*C.Insert
	int
	Insert(CSegmentBase c_segment,
	           signed long int size,
	           const unsigned long* primary_keys,
	           const unsigned long* timestamps,
	           void* raw_data,
	           int sizeof_per_row,
	           signed long int count,
			   unsigned long timestamp_min,
			   unsigned long timestamp_max);
	*/
	// Blobs to one big blob
	var rowData []byte
	for i := 0; i < len(*records); i++ {
		copy(rowData, (*records)[i])
	}

	// TODO: remove hard code schema
	// auto schema_tmp = std::make_shared<Schema>();
	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
	// schema_tmp->AddField("age", DataType::INT32);
	// TODO: remove hard code & fake dataChunk
	const DIM = 4
	const N = 3
	var vec = [DIM]float32{1.1, 2.2, 3.3, 4.4}
	var rawData []int8
	for i := 0; i <= N; i++ {
		for _, ele := range vec {
			rawData=append(rawData, int8(ele))
		}
		rawData=append(rawData, int8(i))
	}
	const sizeofPerRow = 4 + DIM * 4

	var status = C.Insert(s.SegmentPtr, C.long(N), (*C.ulong)(&(*entityIds)[0]), (*C.ulong)(&(*timestamps)[0]), unsafe.Pointer(&rawData[0]), C.int(sizeofPerRow), C.long(N), C.ulong(timestampMin), C.ulong(timestampMax))

	if status != 0 {
		return errors.New("Insert failed, error code = " + strconv.Itoa(int(status)))
	}

	return nil
}

func (s *Segment) SegmentDelete(entityIds *[]int64, timestamps *[]uint64) error {
	/*C.Delete
	int
	Delete(CSegmentBase c_segment,
	           long size,
	           const unsigned long* primary_keys,
	           const unsigned long* timestamps,
			   unsigned long timestamp_min,
			   unsigned long timestamp_max);
	*/
	size := len(*entityIds)

	var status = C.Delete(s.SegmentPtr, C.long(size), (*C.ulong)(&(*entityIds)[0]), (*C.ulong)(&(*timestamps)[0]), C.ulong(timestampMin), C.ulong(timestampMax))

	if status != 0 {
		return errors.New("Delete failed, error code = " + strconv.Itoa(int(status)))
	}

	return nil
}

func (s *Segment) SegmentSearch(queryString string, timestamp uint64, vectorRecord *schema.VectorRowRecord) (*SearchResult, error) {
	/*C.Search
	int
	Search(CSegmentBase c_segment,
	           void* fake_query,
	           unsigned long timestamp,
	           long int* result_ids,
	           float* result_distances);
	*/
	// TODO: get top-k's k from queryString
	const TopK = 1

	resultIds := make([]int64, TopK)
	resultDistances := make([]float32, TopK)

	var status = C.Search(s.SegmentPtr, unsafe.Pointer(nil), C.ulong(timestamp), (*C.long)(&resultIds[0]), (*C.float)(&resultDistances[0]))
	if status != 0 {
		return nil, errors.New("Search failed, error code = " + strconv.Itoa(int(status)))
	}

	return &SearchResult{ResultIds: resultIds, ResultDistances: resultDistances}, nil
}
