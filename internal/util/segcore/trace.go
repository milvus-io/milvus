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

#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"unsafe"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
)

// CTraceContext is the wrapper for `C.CTraceContext`
// it stores the internal C.CTraceContext and
type CTraceContext struct {
	traceID   trace.TraceID
	spanID    trace.SpanID
	requestID *C.char
	ctx       C.CTraceContext
}

// ParseCTraceContext parses tracing span and convert it into `C.CTraceContext`.
func ParseCTraceContext(ctx context.Context) *CTraceContext {
	return ParseCTraceContextWithRequestID(ctx, contextutil.RequestID(ctx))
}

// ParseCTraceContextWithRequestID parses tracing span and request ID, then
// converts them into `C.CTraceContext`.
func ParseCTraceContextWithRequestID(ctx context.Context, requestID string) *CTraceContext {
	if requestID == "" {
		requestID = contextutil.RequestID(ctx)
	}
	span := trace.SpanFromContext(ctx)

	cctx := &CTraceContext{
		traceID: span.SpanContext().TraceID(),
		spanID:  span.SpanContext().SpanID(),
	}
	if requestID != "" {
		cctx.requestID = C.CString(requestID)
	}
	cctx.ctx = C.CTraceContext{
		traceID:    (*C.uint8_t)(unsafe.Pointer(&cctx.traceID[0])),
		spanID:     (*C.uint8_t)(unsafe.Pointer(&cctx.spanID[0])),
		traceFlags: (C.uint8_t)(span.SpanContext().TraceFlags()),
		requestID:  cctx.requestID,
	}

	return cctx
}

func (c *CTraceContext) Close() {
	if c != nil && c.requestID != nil {
		C.free(unsafe.Pointer(c.requestID))
		c.requestID = nil
	}
}
