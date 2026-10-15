// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore

/*
#cgo pkg-config: milvus_core
#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RetrieveIdentitiesByOffsets borrows a pinned segment and retrieves PK and
// requested system fields only. Offsets preserve their supplied row order.
// takeAllowed applies only to the identity projection, not the shared output plan.
func RetrieveIdentitiesByOffsets(ctx context.Context, segment CSegment, plan *RetrievePlanWithOffsets, takeAllowed bool) (*RetrieveResult, error) {
	if ctx == nil || segment == nil || plan == nil || plan.RetrievePlan == nil || plan.cRetrievePlan == nil || len(plan.Offsets) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("identity retrieval requires segment, plan and selected offsets")
	}
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(segment)
	defer runtime.KeepAlive(plan)
	future := cgo.Async(ctx, func() cgo.CFuturePtr {
		return cgo.CFuturePtr(C.AsyncRetrieveIdentitiesByOffsets(traceCtx.ctx, C.CSegmentInterface(segment.RawPointer()),
			plan.cRetrievePlan, (*C.int64_t)(unsafe.Pointer(&plan.Offsets[0])), C.int64_t(len(plan.Offsets)), C.bool(takeAllowed)))
	}, cgo.WithName("retrieve-identities-by-offsets"))
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &RetrieveResult{cRetrieveResult: (*C.CRetrieveResult)(result)}, nil
}
