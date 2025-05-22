package segcore

/*
#cgo pkg-config: milvus_core

#include "common/type_c.h"

*/
import "C"

import "context"

// CSegment is the interface of a segcore segment.
// TODO: We should separate the interface of CGrowingSegment and CSealedSegment,
// Because they have different implementations, GrowingSegment will only be used at streamingnode, SealedSegment will only be used at querynode.
// But currently, we just use the same interface to represent them to keep compatible with querynode LocalSegment.
type CSegment interface {
	GrowingSegment

	SealedSegment
}

// GrowingSegment is the interface of a growing segment.
type GrowingSegment interface {
	basicSegmentMethodSet

	// Insert inserts data into the segment.
	Insert(ctx context.Context, request *InsertRequest) (*InsertResult, error)
}

// SealedSegment is the interface of a sealed segment.
type SealedSegment interface {
	basicSegmentMethodSet

	// LoadFieldData loads field data into the segment.
	LoadFieldData(ctx context.Context, request *LoadFieldDataRequest) (*LoadFieldDataResult, error)

	// AddFieldDataInfo adds field data info into the segment.
	AddFieldDataInfo(ctx context.Context, request *AddFieldDataInfoRequest) (*AddFieldDataInfoResult, error)
}

// basicSegmentMethodSet is the basic method set of a segment.
type basicSegmentMethodSet interface {
	// ID returns the ID of the segment.
	ID() int64

	// RawPointer returns the raw pointer of the segment.
	// TODO: should be removed in future.
	RawPointer() CSegmentInterface

	// RawPointer returns the raw pointer of the segment.
	RowNum() int64

	// MemSize returns the memory size of the segment.
	MemSize() int64

	// HasRawData checks if the segment has raw data.
	HasRawData(fieldID int64) bool

	// Search requests a search on the segment.
	Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error)

	// Retrieve retrieves entities from the segment.
	Retrieve(ctx context.Context, plan *RetrievePlan) (*RetrieveResult, error)

	// RetrieveByOffsets retrieves entities from the segment by offsets.
	RetrieveByOffsets(ctx context.Context, plan *RetrievePlanWithOffsets) (*RetrieveResult, error)

	// Delete deletes data from the segment.
	Delete(ctx context.Context, request *DeleteRequest) (*DeleteResult, error)

	// FinishLoad wraps up the load process and let segcore do the leftover jobs.
	FinishLoad() error

	// Release releases the segment.
	Release()
}
