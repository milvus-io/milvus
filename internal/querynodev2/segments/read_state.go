package segments

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

// ReadState owns one immutable set of sealed columns/indexes and shares the
// physical segment's continuously applied deletes. A view owns the initial
// reference; each query retains another reference until result reduction ends.
type ReadState interface {
	Segment
	RetainReadState() func()
	ReleaseReadState()
}

type sealedReadState struct {
	Segment
	parent *LocalSegment
	native segcore.CSegment
	refs   atomic.Int64
}

func (s *LocalSegment) PinReadState() (ReadState, error) {
	if err := s.PinIfNotReleased(); err != nil {
		return nil, err
	}
	native, err := segcore.PinSealedReadState(s.csegment)
	if err != nil {
		s.Unpin()
		return nil, err
	}
	read := &sealedReadState{Segment: s, parent: s, native: native}
	read.refs.Store(1)
	return read, nil
}

func (s *sealedReadState) RetainReadState() func() {
	s.refs.Add(1)
	return s.ReleaseReadState
}

func (s *sealedReadState) ReleaseReadState() {
	if s.refs.Add(-1) == 0 {
		s.native.Release()
		s.parent.Unpin()
	}
}

func (s *sealedReadState) Search(ctx context.Context, req *segcore.SearchRequest) (*segcore.SearchResult, error) {
	return s.parent.searchWithSegment(ctx, req, s.native)
}

func (s *sealedReadState) Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	return s.parent.retrieveWithSegment(ctx, plan, s.native)
}

func (s *sealedReadState) RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
	return s.parent.retrieveByOffsetsWithSegment(ctx, plan, s.native)
}

func (s *sealedReadState) HasRawData(fieldID int64) bool {
	return s.native.HasRawData(fieldID)
}

func (s *sealedReadState) RowNum() int64 {
	return s.native.RowNum()
}
