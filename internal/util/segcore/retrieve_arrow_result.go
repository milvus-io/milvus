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

#include "segcore/plan_c.h"
#include "segcore/segment_c.h"
#include "common/arrow_c_data_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// RetrieveArrowResult owns one C CRetrieveArrowResult: the protobuf header
// plus the exported Arrow C structs for the user output columns.
//
// Release frees all of it. It stays correct after GetResult: importing the
// Arrow structs nulls their release callbacks, so the C side skips them.
type RetrieveArrowResult struct {
	cResult  *C.CRetrieveArrowResult
	consumed bool
}

func (r *RetrieveArrowResult) Release() {
	if r == nil || r.cResult == nil {
		return
	}
	C.DeleteRetrieveArrowResult(r.cResult)
	r.cResult = nil
}

// GetResult unmarshals the protobuf header and imports the Arrow record.
//
// Ownership of the returned record moves to the caller, which must Release it
// independently of this RetrieveArrowResult. The header is fully copied into
// Go memory, so it outlives both.
func (r *RetrieveArrowResult) GetResult() (*segcorepb.RetrieveResults, arrow.Record, error) {
	if r == nil || r.cResult == nil {
		return nil, nil, merr.WrapErrServiceInternal("retrieve arrow result already released")
	}
	// One-shot. Importing moves ownership of the Arrow buffers out and nulls
	// the release callbacks in the C structs, so a second import would read
	// a consumed ArrowArray -- silent corruption rather than an error.
	if r.consumed {
		return nil, nil, merr.WrapErrServiceInternal(
			"retrieve arrow result already consumed; GetResult is one-shot")
	}
	r.consumed = true

	// Header first: a plain copy into Go memory, independent of the Arrow
	// buffers.
	header := new(segcorepb.RetrieveResults)
	if err := unmarshalCProto(r.cResult.header, header); err != nil {
		return nil, nil, err
	}

	// consumeArrowRecordBatch takes ownership of both C structs, including on
	// failure, and nulls their release callbacks on success.
	record, err := consumeArrowRecordBatch(
		(*C.struct_ArrowSchema)(unsafe.Pointer(r.cResult.schema)),
		(*C.struct_ArrowArray)(unsafe.Pointer(r.cResult.array)),
		nil,
	)
	if err != nil {
		return nil, nil, err
	}
	return header, record, nil
}

// RetrieveAsArrow mirrors Retrieve, but returns the protobuf header and the
// user output columns separately. See CRetrieveArrowResult in segment_c.h for
// the split and its preconditions.
func (s *cSegmentImpl) RetrieveAsArrow(ctx context.Context, plan *RetrievePlan) (*RetrieveArrowResult, error) {
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)

	// Use physical time for entity-level TTL (issue #47413)
	physicalTimeUs := int64(plan.entityTTLPhysicalTime)
	if physicalTimeUs == 0 {
		physicalTimeMs, _ := tsoutil.ParseHybridTs(plan.Timestamp)
		physicalTimeUs = physicalTimeMs * 1000
	}

	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return cgo.CFuturePtr(C.AsyncRetrieveAsArrow(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				C.uint64_t(plan.Timestamp),
				C.int64_t(plan.maxLimitSize),
				C.int32_t(plan.consistencyLevel),
				C.uint64_t(plan.collectionTTL),
				C.uint64_t(physicalTimeUs),
			))
		},
		cgo.WithName("retrieveAsArrow"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &RetrieveArrowResult{cResult: (*C.CRetrieveArrowResult)(result)}, nil
}
