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
	"github.com/czs007/suvlim/pulsar/client-go/schema"
	"unsafe"
)

const SegmentLifetime = 20000

type Segment struct {
	SegmentPtr C.CSegmentBase
	SegmentId	uint64
	SegmentCloseTime uint64
}

func (s *Segment) GetRowCount() int64 {
	// TODO: C type to go type
	//return C.GetRowCount(s)
	return 0
}

func (s *Segment) GetStatus() int {
	// TODO: C type to go type
	//return C.GetStatus(s)
	return 0
}

func (s *Segment) GetMaxTimestamp() uint64 {
	// TODO: C type to go type
	//return C.GetMaxTimestamp(s)
	return 0
}

func (s *Segment) GetMinTimestamp() uint64 {
	// TODO: C type to go type
	//return C.GetMinTimestamp(s)
	return 0
}

func (s *Segment) GetDeletedCount() uint64 {
	// TODO: C type to go type
	//return C.GetDeletedCount(s)
	return 0
}

func (s *Segment) Close() {
	// TODO: C type to go type
	//C.CloseSegment(s)
}

////////////////////////////////////////////////////////////////////////////
func SegmentInsert(segment *Segment, entityIds *[]uint64, timestamps *[]uint64, dataChunk [][]*schema.FieldValue) (ResultEntityIds, error) {
	// TODO: remove hard code schema
	// auto schema_tmp = std::make_shared<Schema>();
	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
	// schema_tmp->AddField("age", DataType::INT32);

	/*C.Insert
	int
	Insert(CSegmentBase c_segment,
	           signed long int size,
	           const unsigned long* primary_keys,
	           const unsigned long* timestamps,
	           void* raw_data,
	           int sizeof_per_row,
	           signed long int count);
	*/

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

	var status = C.Insert(segment.SegmentPtr, (*C.ulong)(entityIds), (*C.ulong)(timestamps), unsafe.Pointer(&rawData[0]), C.int(sizeofPerRow), C.long(N))

	if status != 0 {
		return nil, errors.New("Insert failed, error code = " + status)
	}

	return ResultEntityIds{}, nil
}

func SegmentDelete(segment *Segment, entityIds *[]uint64, timestamps *[]uint64) (ResultEntityIds, error) {
	/*C.Delete
	int
	Delete(CSegmentBase c_segment,
	           long size,
	           const unsigned long* primary_keys,
	           const unsigned long* timestamps);
	*/
	size := len(*entityIds)

	var status = C.Delete(segment.SegmentPtr, C.long(size), (*C.ulong)(entityIds), (*C.ulong)(timestamps))

	if status != 0 {
		return nil, errors.New("Delete failed, error code = " + status)
	}

	return ResultEntityIds{}, nil
}

func SegmentSearch(segment *Segment, queryString string, timestamps *[]uint64, vectorRecord *[]schema.VectorRecord) (*[]SearchResult, error) {
	/*C.Search
	int
	Search(CSegmentBase c_segment,
	           void* fake_query,
	           unsigned long timestamp,
	           long int* result_ids,
	           float* result_distances);
	*/
	var results []SearchResult

	// TODO: get top-k's k from queryString
	const TopK = 1

	for timestamp := range *timestamps {
		resultIds := make([]int64, TopK)
		resultDistances  := make([]float32, TopK)

		var status = C.Search(segment.SegmentPtr, unsafe.Pointer(nil), C.ulong(timestamp), (*C.long)(&resultIds[0]), (*C.float)(&resultDistances[0]))
		if status != 0 {
			return nil, errors.New("Search failed, error code = " + status)
		}

		results = append(results, SearchResult{ResultIds: resultIds, ResultDistances: resultDistances})
	}

	return &results, nil
}
