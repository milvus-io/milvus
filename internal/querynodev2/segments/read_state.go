package segments

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

// PrepareReadState builds a private successor, sharing unchanged native resources.
// Neither the source read state nor the physical segment's schema is modified.
// The caller pins reuse for the duration of this operation.
func (s *LocalSegment) PrepareReadState(ctx context.Context, reuse ReadState, info *querypb.SegmentLoadInfo, collection *Collection) (ReadState, error) {
	if err := s.PinIfNotReleased(); err != nil {
		return nil, err
	}
	source := s.csegment
	if reuse != nil {
		read := reuse.(*sealedReadState)
		if read.parent != s {
			s.Unpin()
			return nil, merr.WrapErrServiceInternalMsg("read state belongs to another physical segment")
		}
		source = read.native
	}
	native, err := segcore.PinSealedReadState(source)
	if err != nil {
		s.Unpin()
		return nil, err
	}
	read := &sealedReadState{Segment: s, parent: s, native: native}
	read.refs.Store(1)
	if err := prepareIndexLoadParams(info.GetIndexInfos()); err != nil {
		read.ReleaseReadState()
		return nil, err
	}
	if err := native.Reopen(ctx, &segcore.ReopenRequest{LoadInfo: info, Schema: collection.Schema(), SchemaVersion: uint64(collection.Schema().GetVersion()), LoadFields: collection.loadFields.Collect()}); err != nil {
		read.ReleaseReadState()
		return nil, err
	}
	for _, field := range typeutil.GetAllFieldSchemas(collection.Schema()) {
		id := field.GetFieldID()
		if !field.GetIsFunctionOutput() || !collection.loadFields.Contain(id) || native.HasFieldData(id) {
			continue
		}
		indexed := false
		for _, index := range info.GetIndexInfos() {
			if index.GetFieldID() == id && index.GetEnableIndex() {
				indexed = true
				break
			}
		}
		if !indexed {
			read.ReleaseReadState()
			return nil, merr.WrapErrServiceNotReadyMsg("segment %d has not materialized function output field %d for query view schema %d", s.ID(), id, collection.Schema().GetVersion())
		}
	}
	return read, nil
}

func (s *sealedReadState) HasFieldData(fieldID int64) bool {
	return s.native.HasFieldData(fieldID)
}

// AdoptReadState releases the base's references to columns no live view needs.
// Existing query forks retain their own immutable published state.
func (s *LocalSegment) AdoptReadState(state ReadState) error {
	read := state.(*sealedReadState)
	if read.parent != s {
		return merr.WrapErrServiceInternalMsg("read state belongs to another physical segment")
	}
	return segcore.AdoptSealedReadState(s.csegment, read.native)
}
