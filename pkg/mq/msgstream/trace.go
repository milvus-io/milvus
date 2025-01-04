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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// ExtractCtx extracts trace span from msg.properties.
// And it will attach some default tags to the span.
func ExtractCtx(msg PackMsg, properties map[string]string) (context.Context, trace.Span) {
	ctx := context.Background()
	if !allowTrace(msg) {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(properties))
	name := "ReceieveMsg"
	return otel.Tracer(name).Start(ctx, name, trace.WithAttributes(
		attribute.Int64("ID", msg.GetID()),
		attribute.String("Type", msg.GetType().String()),
		attribute.String("Position", msg.GetPosition().String()),
	))
}

// InjectCtx is a method inject span to pulsr message.
func InjectCtx(sc context.Context, properties map[string]string) {
	if sc == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(sc, propagation.MapCarrier(properties))
}

// MsgSpanFromCtx extracts the span from context.
// And it will attach some default tags to the span.
func MsgSpanFromCtx(ctx context.Context, msg TsMsg) (context.Context, trace.Span) {
	if ctx == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	if !allowTrace(msg) {
		return ctx, trace.SpanFromContext(ctx)
	}
	operationName := "SendMsg"
	opts := trace.WithAttributes(
		attribute.Int64("ID", msg.ID()),
		attribute.String("Type", msg.Type().String()),
		// attribute.Int64Value("HashKeys", msg.HashKeys()),
		attribute.String("Position", msg.Position().String()),
	)
	return otel.Tracer(operationName).Start(ctx, operationName, opts)
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
