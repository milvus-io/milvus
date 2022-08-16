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

package msgstream

import (
	"context"
	"errors"
	"runtime"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// ExtractFromPulsarMsgProperties extracts trace span from msg.properties.
// And it will attach some default tags to the span.
func ExtractFromPulsarMsgProperties(msg TsMsg, properties map[string]string) (*trace.Span, bool) {
	if !allowTrace(msg) {
		return trace.NoopSpan(), false
	}
	ctx := trace.ExtractContextFromProperties(msg.TraceCtx(), properties)
	_, span := trace.StartSpanFromContextWithOperationName(ctx, "recv_pulsar_msg")
	return span, true
}

// MsgSpanFromCtx extracts the span from context.
// And it will attach some default tags to the span.
func MsgSpanFromCtx(ctx context.Context, msg TsMsg) (*trace.Span, context.Context) {
	if ctx == nil {
		return trace.NoopSpan(), ctx
	}
	if !allowTrace(msg) {
		return trace.NoopSpan(), ctx
	}
	operationName := "send_pulsar_msg"

	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		ctx, span := trace.StartSpanFromContextWithOperationName(ctx, operationName)
		span.RecordError(errors.New("runtime.Callers failed"))
		return span, ctx
	}
	file, line := runtime.FuncForPC(pcs[0]).FileLine(pcs[0])

	ctx, span := trace.StartSpanFromContextWithOperationName(ctx, operationName)
	span.RecordInt64("msg_id", msg.ID())
	span.RecordString("msg_type", msg.Type().String())
	span.RecordAnyIgnoreErr("msg_pos", msg.Position())
	span.RecordAnyIgnoreErr("msg_hash", msg.HashKeys())
	span.RecordString("file", file)
	span.RecordInt("line", line)

	return span, ctx
}

func allowTrace(in interface{}) bool {
	if in == nil {
		return false
	}
	switch res := in.(type) {
	case TsMsg:
		return !(res.Type() == commonpb.MsgType_TimeTick ||
			res.Type() == commonpb.MsgType_LoadIndex)
	default:
		return false
	}
}
