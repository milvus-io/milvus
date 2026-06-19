// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <string.h>
#include "futures/future_c.h"
#include "segcore/boost_score_c.h"

// Arrow C Data Interface structs (standard ABI, layout matches Go cdata package)
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

static inline void
MilvusGoBoostScoreArrowSchemaRelease(struct ArrowSchema* schema) {
    if (schema != NULL && schema->release != NULL) {
        schema->release(schema);
    }
}

static inline int
MilvusGoBoostScoreArrowArrayIsReleased(const struct ArrowArray* array) {
    return array == NULL || array->release == NULL;
}

static inline void
MilvusGoBoostScoreArrowArrayRelease(struct ArrowArray* array) {
    if (!MilvusGoBoostScoreArrowArrayIsReleased(array)) {
        array->release(array);
    }
}

static inline void**
MilvusGoBoostScoreAllocPointerArray(int64_t count) {
    return (void**)calloc((size_t)count, sizeof(void*));
}

static inline void
MilvusGoBoostScoreSetPointerArray(void** array, int64_t index, void* value) {
    array[index] = value;
}
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ComputeScorerScoresOnChunkedOffsets(segment CSegment, searchReq *SearchRequest, scoreFunction *planpb.ScoreFunction, offsets *arrow.Chunked) (*arrow.Chunked, error) {
	return computeScorerScoresOnChunkedOffsets(context.Background(), segment, searchReq, scoreFunction, offsets, false)
}

func AsyncComputeScorerScoresOnChunkedOffsets(ctx context.Context, segment CSegment, searchReq *SearchRequest, scoreFunction *planpb.ScoreFunction, offsets *arrow.Chunked) (*arrow.Chunked, error) {
	return computeScorerScoresOnChunkedOffsets(ctx, segment, searchReq, scoreFunction, offsets, true)
}

func computeScorerScoresOnChunkedOffsets(ctx context.Context, segment CSegment, searchReq *SearchRequest, scoreFunction *planpb.ScoreFunction, offsets *arrow.Chunked, async bool) (*arrow.Chunked, error) {
	if segment == nil {
		return nil, merr.WrapErrParameterInvalidMsg("segment is nil")
	}
	if searchReq == nil || searchReq.plan == nil {
		return nil, merr.WrapErrParameterInvalidMsg("search request or plan is nil")
	}
	if scoreFunction == nil {
		return nil, merr.WrapErrParameterInvalidMsg("score function is nil")
	}
	if offsets == nil {
		return nil, merr.WrapErrParameterInvalidMsg("offsets is nil")
	}
	if offsets.DataType().ID() != arrow.INT64 {
		return nil, merr.WrapErrParameterInvalidMsg("offset column must be Int64, got %s", offsets.DataType())
	}

	scoreFunctionBlob, err := proto.Marshal(scoreFunction)
	if err != nil {
		return nil, merr.Wrap(err, "failed to marshal score function")
	}
	if len(scoreFunctionBlob) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty score function")
	}

	numChunks := len(offsets.Chunks())
	scoreArrays := make([]arrow.Array, numChunks)
	if numChunks == 0 {
		return arrow.NewChunked(arrow.PrimitiveTypes.Float32, scoreArrays), nil
	}

	offsetArrays := make([]C.struct_ArrowArray, numChunks)
	offsetSchemas := make([]C.struct_ArrowSchema, numChunks)
	scoreChunks := make([][]float32, numChunks)
	hasScoreChunks := make([][]C.bool, numChunks)
	pins := make([]runtime.Pinner, 0, numChunks*2)
	defer func() {
		for i := range pins {
			pins[i].Unpin()
		}
	}()
	defer func() {
		for i := range offsetArrays {
			C.MilvusGoBoostScoreArrowArrayRelease(&offsetArrays[i])
			C.MilvusGoBoostScoreArrowSchemaRelease(&offsetSchemas[i])
		}
	}()

	scorePtrArray := C.MilvusGoBoostScoreAllocPointerArray(C.int64_t(numChunks))
	hasScorePtrArray := C.MilvusGoBoostScoreAllocPointerArray(C.int64_t(numChunks))
	if scorePtrArray == nil || hasScorePtrArray == nil {
		if scorePtrArray != nil {
			C.free(unsafe.Pointer(scorePtrArray))
		}
		if hasScorePtrArray != nil {
			C.free(unsafe.Pointer(hasScorePtrArray))
		}
		return nil, merr.WrapErrServiceInternalMsg("failed to allocate boost score pointer arrays")
	}
	defer C.free(unsafe.Pointer(scorePtrArray))
	defer C.free(unsafe.Pointer(hasScorePtrArray))

	for chunkIdx, chunk := range offsets.Chunks() {
		offsetChunk, ok := chunk.(*array.Int64)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("offset chunk %d must be Int64, got %T", chunkIdx, chunk)
		}
		if offsetChunk.NullN() > 0 {
			return nil, merr.WrapErrParameterInvalidMsg("offset chunk %d contains null", chunkIdx)
		}

		cdata.ExportArrowArray(
			offsetChunk,
			(*cdata.CArrowArray)(unsafe.Pointer(&offsetArrays[chunkIdx])),
			(*cdata.CArrowSchema)(unsafe.Pointer(&offsetSchemas[chunkIdx])),
		)

		length := offsetChunk.Len()
		if length == 0 {
			continue
		}

		scoreChunks[chunkIdx] = make([]float32, length)
		hasScoreChunks[chunkIdx] = make([]C.bool, length)
		var scorePin runtime.Pinner
		scorePin.Pin(&scoreChunks[chunkIdx][0])
		pins = append(pins, scorePin)
		var hasScorePin runtime.Pinner
		hasScorePin.Pin(&hasScoreChunks[chunkIdx][0])
		pins = append(pins, hasScorePin)
		C.MilvusGoBoostScoreSetPointerArray(scorePtrArray, C.int64_t(chunkIdx), unsafe.Pointer(&scoreChunks[chunkIdx][0]))
		C.MilvusGoBoostScoreSetPointerArray(hasScorePtrArray, C.int64_t(chunkIdx), unsafe.Pointer(&hasScoreChunks[chunkIdx][0]))
	}

	cSegment := C.CSegmentInterface(segment.RawPointer())
	cPlan := searchReq.plan.cSearchPlan
	scoreFunctionPtr := unsafe.Pointer(&scoreFunctionBlob[0])
	scoreFunctionSize := C.int64_t(len(scoreFunctionBlob))
	offsetArrayPtr := &offsetArrays[0]
	offsetSchemaPtr := &offsetSchemas[0]
	numChunk := C.int64_t(numChunks)
	mvccTimestamp := C.uint64_t(searchReq.mvccTimestamp)
	collectionTTL := C.uint64_t(searchReq.collectionTTL)
	consistencyLevel := C.int32_t(searchReq.consistencyLevel)
	entityTTLPhysicalTime := C.uint64_t(searchReq.entityTTLPhysicalTime)
	scorePtr := (**C.float)(unsafe.Pointer(scorePtrArray))
	hasScorePtr := (**C.bool)(unsafe.Pointer(hasScorePtrArray))

	if async {
		future := cgo.Async(ctx,
			func() cgo.CFuturePtr {
				return (cgo.CFuturePtr)(C.AsyncComputeScorerScoresOnOffsetChunks(
					cSegment,
					cPlan,
					scoreFunctionPtr,
					scoreFunctionSize,
					offsetArrayPtr,
					offsetSchemaPtr,
					numChunk,
					mvccTimestamp,
					collectionTTL,
					consistencyLevel,
					entityTTLPhysicalTime,
					scorePtr,
					hasScorePtr,
				))
			},
			cgo.WithName("boost_score"),
		)
		defer future.Release()
		if _, err := future.BlockAndLeakyGet(); err != nil {
			return nil, err
		}
	} else {
		status := C.ComputeScorerScoresOnOffsetChunks(
			cSegment,
			cPlan,
			scoreFunctionPtr,
			scoreFunctionSize,
			offsetArrayPtr,
			offsetSchemaPtr,
			numChunk,
			mvccTimestamp,
			collectionTTL,
			consistencyLevel,
			entityTTLPhysicalTime,
			scorePtr,
			hasScorePtr,
		)
		if err := ConsumeCStatusIntoError(&status); err != nil {
			return nil, err
		}
	}

	for chunkIdx := range scoreChunks {
		builder := array.NewFloat32Builder(memory.DefaultAllocator)
		for rowIdx, score := range scoreChunks[chunkIdx] {
			if bool(hasScoreChunks[chunkIdx][rowIdx]) {
				builder.Append(score)
			} else {
				builder.AppendNull()
			}
		}
		scoreArrays[chunkIdx] = builder.NewArray()
		builder.Release()
	}
	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, scoreArrays)
	for _, scoreArray := range scoreArrays {
		if scoreArray != nil {
			scoreArray.Release()
		}
	}

	runtime.KeepAlive(segment)
	runtime.KeepAlive(searchReq)
	runtime.KeepAlive(scoreFunction)
	runtime.KeepAlive(scoreFunctionBlob)
	runtime.KeepAlive(offsets)
	runtime.KeepAlive(scoreChunks)
	runtime.KeepAlive(hasScoreChunks)

	return result, nil
}
