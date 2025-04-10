package segcore

/*
#cgo pkg-config: milvus_core

#include "common/type_c.h"
#include "futures/future_c.h"
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	SegmentTypeGrowing SegmentType = commonpb.SegmentState_Growing
	SegmentTypeSealed  SegmentType = commonpb.SegmentState_Sealed
)

type (
	SegmentType       = commonpb.SegmentState
	CSegmentInterface C.CSegmentInterface
)

// CreateCSegmentRequest is a request to create a segment.
type CreateCSegmentRequest struct {
	Collection    *CCollection
	SegmentID     int64
	SegmentType   SegmentType
	IsSorted      bool
	EnableChunked bool
}

func (req *CreateCSegmentRequest) getCSegmentType() C.SegmentType {
	var segmentType C.SegmentType
	switch req.SegmentType {
	case SegmentTypeGrowing:
		segmentType = C.Growing
	case SegmentTypeSealed:
		if req.EnableChunked {
			segmentType = C.ChunkedSealed
			break
		}
		segmentType = C.Sealed
	default:
		panic(fmt.Sprintf("invalid segment type: %d", req.SegmentType))
	}
	return segmentType
}

// CreateCSegment creates a segment from a CreateCSegmentRequest.
func CreateCSegment(req *CreateCSegmentRequest) (CSegment, error) {
	var ptr C.CSegmentInterface
	status := C.NewSegment(req.Collection.rawPointer(), req.getCSegmentType(), C.int64_t(req.SegmentID), &ptr, C.bool(req.IsSorted))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &cSegmentImpl{id: req.SegmentID, ptr: ptr}, nil
}

// cSegmentImpl is a wrapper for cSegmentImplInterface.
type cSegmentImpl struct {
	id  int64
	ptr C.CSegmentInterface
}

// ID returns the ID of the segment.
func (s *cSegmentImpl) ID() int64 {
	return s.id
}

// RawPointer returns the raw pointer of the segment.
func (s *cSegmentImpl) RawPointer() CSegmentInterface {
	return CSegmentInterface(s.ptr)
}

// RowNum returns the number of rows in the segment.
func (s *cSegmentImpl) RowNum() int64 {
	rowCount := C.GetRealCount(s.ptr)
	return int64(rowCount)
}

// MemSize returns the memory size of the segment.
func (s *cSegmentImpl) MemSize() int64 {
	cMemSize := C.GetMemoryUsageInBytes(s.ptr)
	return int64(cMemSize)
}

// HasRawData checks if the segment has raw data.
func (s *cSegmentImpl) HasRawData(fieldID int64) bool {
	ret := C.HasRawData(s.ptr, C.int64_t(fieldID))
	return bool(ret)
}

// Search requests a search on the segment.
func (s *cSegmentImpl) Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error) {
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(searchReq)

	future := cgo.Async(ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncSearch(
				traceCtx.ctx,
				s.ptr,
				searchReq.plan.cSearchPlan,
				searchReq.cPlaceholderGroup,
				C.uint64_t(searchReq.mvccTimestamp),
				C.int32_t(searchReq.consistencyLevel),
			))
		},
		cgo.WithName("search"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &SearchResult{cSearchResult: (C.CSearchResult)(result)}, nil
}

// Retrieve retrieves entities from the segment.
func (s *cSegmentImpl) Retrieve(ctx context.Context, plan *RetrievePlan) (*RetrieveResult, error) {
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)
	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncRetrieve(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				C.uint64_t(plan.Timestamp),
				C.int64_t(plan.maxLimitSize),
				C.bool(plan.ignoreNonPk),
				C.int32_t(plan.consistencyLevel),
			))
		},
		cgo.WithName("retrieve"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &RetrieveResult{cRetrieveResult: (*C.CRetrieveResult)(result)}, nil
}

// RetrieveByOffsets retrieves entities from the segment by offsets.
func (s *cSegmentImpl) RetrieveByOffsets(ctx context.Context, plan *RetrievePlanWithOffsets) (*RetrieveResult, error) {
	if len(plan.Offsets) == 0 {
		return nil, merr.WrapErrParameterInvalid("segment offsets", "empty offsets")
	}

	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)
	defer runtime.KeepAlive(plan.Offsets)

	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncRetrieveByOffsets(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				(*C.int64_t)(unsafe.Pointer(&plan.Offsets[0])),
				C.int64_t(len(plan.Offsets)),
			))
		},
		cgo.WithName("retrieve-by-offsets"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &RetrieveResult{cRetrieveResult: (*C.CRetrieveResult)(result)}, nil
}

// Insert inserts entities into the segment.
func (s *cSegmentImpl) Insert(ctx context.Context, request *InsertRequest) (*InsertResult, error) {
	offset, err := s.preInsert(len(request.RowIDs))
	if err != nil {
		return nil, err
	}

	insertRecordBlob, err := proto.Marshal(request.Record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal insert record: %s", err)
	}

	numOfRow := len(request.RowIDs)
	cOffset := C.int64_t(offset)
	cNumOfRows := C.int64_t(numOfRow)
	cEntityIDsPtr := (*C.int64_t)(&(request.RowIDs)[0])
	cTimestampsPtr := (*C.uint64_t)(&(request.Timestamps)[0])

	status := C.Insert(s.ptr,
		cOffset,
		cNumOfRows,
		cEntityIDsPtr,
		cTimestampsPtr,
		(*C.uint8_t)(unsafe.Pointer(&insertRecordBlob[0])),
		(C.uint64_t)(len(insertRecordBlob)),
	)
	return &InsertResult{InsertedRows: int64(numOfRow)}, ConsumeCStatusIntoError(&status)
}

func (s *cSegmentImpl) preInsert(numOfRecords int) (int64, error) {
	var offset int64
	cOffset := (*C.int64_t)(&offset)
	status := C.PreInsert(s.ptr, C.int64_t(int64(numOfRecords)), cOffset)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return 0, err
	}
	return offset, nil
}

// Delete deletes entities from the segment.
func (s *cSegmentImpl) Delete(ctx context.Context, request *DeleteRequest) (*DeleteResult, error) {
	cOffset := C.int64_t(0) // depre

	cSize := C.int64_t(request.PrimaryKeys.Len())
	cTimestampsPtr := (*C.uint64_t)(&(request.Timestamps)[0])

	ids, err := storage.ParsePrimaryKeysBatch2IDs(request.PrimaryKeys)
	if err != nil {
		return nil, err
	}

	dataBlob, err := proto.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ids: %s", err)
	}
	status := C.Delete(s.ptr,
		cOffset,
		cSize,
		(*C.uint8_t)(unsafe.Pointer(&dataBlob[0])),
		(C.uint64_t)(len(dataBlob)),
		cTimestampsPtr,
	)
	return &DeleteResult{}, ConsumeCStatusIntoError(&status)
}

// LoadFieldData loads field data into the segment.
func (s *cSegmentImpl) LoadFieldData(ctx context.Context, request *LoadFieldDataRequest) (*LoadFieldDataResult, error) {
	creq, err := request.getCLoadFieldDataRequest()
	if err != nil {
		return nil, err
	}
	defer creq.Release()

	status := C.LoadFieldData(s.ptr, creq.cLoadFieldDataInfo)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "failed to load field data")
	}
	return &LoadFieldDataResult{}, nil
}

// AddFieldDataInfo adds field data info into the segment.
func (s *cSegmentImpl) AddFieldDataInfo(ctx context.Context, request *AddFieldDataInfoRequest) (*AddFieldDataInfoResult, error) {
	creq, err := request.getCLoadFieldDataRequest()
	if err != nil {
		return nil, err
	}
	defer creq.Release()

	status := C.AddFieldDataInfoForSealed(s.ptr, creq.cLoadFieldDataInfo)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "failed to add field data info")
	}
	return &AddFieldDataInfoResult{}, nil
}

// Release releases the segment.
func (s *cSegmentImpl) Release() {
	C.DeleteSegment(s.ptr)
}
