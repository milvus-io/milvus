package reader

import "C"
import (
	"errors"
	"suvlim/pulsar/schema"
)

const SEGMENT_LIFETIME = 20000

type Segment struct {
	SegmentPtr *C.SegmentBase
	SegmentId	int32
	SegmentCloseTime uint64
}

func (p *Partition) NewSegment() (*Segment, error) {
	// TODO: add segment id
	segmentPtr, status := C.CreateSegment(p.PartitionPtr)

	if status != 0 {
		return nil, errors.New("create segment failed")
	}

	return &Segment{SegmentPtr: segmentPtr}, nil
}

func (p *Partition) DeleteSegment() error {
	status := C.DeleteSegment(p.PartitionPtr)

	if status != 0 {
		return errors.New("delete segment failed")
	}

	return nil
}

func (s *Segment) GetRowCount() int64 {
	// TODO: C type to go type
	return C.GetRowCount(s)
}

func (s *Segment) GetStatus() int {
	// TODO: C type to go type
	return C.GetStatus(s)
}

func (s *Segment) GetMaxTimestamp() uint64 {
	// TODO: C type to go type
	return C.GetMaxTimestamp(s)
}

func (s *Segment) GetMinTimestamp() uint64 {
	// TODO: C type to go type
	return C.GetMinTimestamp(s)
}

func (s *Segment) GetDeletedCount() uint64 {
	// TODO: C type to go type
	return C.GetDeletedCount(s)
}

func (s *Segment) Close() {
	// TODO: C type to go type
	C.CloseSegment(s)
}

////////////////////////////////////////////////////////////////////////////
func SegmentInsert(segment *Segment, collectionName string, partitionTag string, entityIds *[]int64, timestamps *[]uint64, dataChunk [][]*schema.FieldValue) ResultEntityIds {
	// TODO: wrap cgo
	return ResultEntityIds{}
}

func SegmentDelete(segment *Segment, collectionName string, entityIds *[]int64, timestamps *[]uint64) ResultEntityIds {
	// TODO: wrap cgo
	return ResultEntityIds{}
}

func SegmentSearch(segment *Segment, collectionName string, queryString string, timestamps *[]int64, vectorRecord *[]schema.VectorRecord) ResultEntityIds {
	// TODO: wrap cgo
	return ResultEntityIds{}
}
