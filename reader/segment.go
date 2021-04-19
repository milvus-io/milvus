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
	"github.com/czs007/suvlim/pulsar/schema"
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
func SegmentInsert(segment *Segment, entityIds *[]int64, timestamps *[]uint64, dataChunk [][]*schema.FieldValue) ResultEntityIds {
	//  void* raw_data,
	//	int sizeof_per_row,
	//	signed long int count

	return ResultEntityIds{}
}

func SegmentDelete(segment *Segment, collectionName string, entityIds *[]int64, timestamps *[]uint64) ResultEntityIds {
	// TODO: wrap cgo
	return ResultEntityIds{}
}

func SegmentSearch(segment *Segment, collectionName string, queryString string, timestamps *[]uint64, vectorRecord *[]schema.VectorRecord) ResultEntityIds {
	// TODO: wrap cgo
	return ResultEntityIds{}
}
