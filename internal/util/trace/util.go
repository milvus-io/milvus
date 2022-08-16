// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package trace

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	sampleOutputKey   = "opentelemetry.sampleFile"
	jaegerEndpointKey = "opentelemetry.jaegerEndpoint"
	sampleFracKey     = "opentelemetry.sampleFraction"
	devKey            = "development"
)

var tracingCloserMtx sync.Mutex
var tracingCloser io.Closer

// InitTracing init global trace from env. If not specified, use default config.
func InitTracing(serviceName string, params *paramtable.BaseTable, hostname string) io.Closer {
	tracingCloserMtx.Lock()
	defer tracingCloserMtx.Unlock()

	if tracingCloser != nil {
		return tracingCloser
	}
	output, jaegerEP := "stdout", ""
	sampleFrac := 1.0

	if v := params.Get(sampleOutputKey); v != "" {
		output = v
	}
	if v := params.Get(jaegerEndpointKey); v != "" {
		jaegerEP = v
	}
	if fracStr := params.Get(sampleFracKey); fracStr != "" {
		v, err := strconv.ParseFloat(fracStr, 64)
		if err == nil {
			sampleFrac = v
		} else {
			log.Error("parse fraction failed", zap.Error(err))
		}
	}

	develop := params.ParseBool(devKey, false)
	opts := make([]Option, 0)
	opts = append(opts,
		WithServiceName(serviceName),
		WithDevelopmentFlag(develop),
		WithJaegerEndpoint(jaegerEP),
		WithSampleFraction(sampleFrac),
		WithExportFile(output),
	)
	if len(hostname) > 0 {
		opts = append(opts, WithHostName(hostname))
	}
	tracer, err := NewGlobalTracerProvider(opts...)
	if err != nil {
		log.Error("init global tracer provider failed", zap.Error(err))
		return nil
	}
	tracingCloser = tracer
	return tracer
}

func callerInfo(skip int) (fnName, loc string) {
	var pcs [1]uintptr
	n := runtime.Callers(skip, pcs[:])
	if n < 1 {
		return "unknown", "unknown"
	}
	frames := runtime.CallersFrames(pcs[:])
	frame, _ := frames.Next()
	fileName := frame.File
	if lastSlashIdx := strings.LastIndexByte(fileName, '/'); lastSlashIdx > 0 {
		fileName = fileName[lastSlashIdx+1:]
	}
	return frame.Function, fmt.Sprintf("%s:%d", fileName, frame.Line)
}

// StartSpanFromContextWithOperationName starts an opentracing span with specific operation name.
// And will log print the current call line number and file name.
func StartSpanFromContextWithOperationName(ctx context.Context, operationName string, opts ...trace.SpanStartOption) (context.Context, *Span) {
	if ctx == nil {
		return context.TODO(), NoopSpan()
	}
	fnName, loc := callerInfo(3)
	traceID, _, _ := InfoFromContext(ctx)
	ctx, span := DefaultTracer().Start(ctx, operationName, opts...)
	span.SetAttributes(attribute.String("function", fnName))
	span.SetAttributes(attribute.String("location", loc))
	span.SetAttributes(attribute.String("traceID", traceID))
	span.SetAttributes(attribute.String("clientRequestID", logutil.GetClientRequestID(ctx)))
	return ctx, &Span{Span: span}
}

// InfoFromSpan is a method return span details.
func InfoFromSpan(span trace.Span) (traceID string, sampled, found bool) {
	if span != nil {
		spanctx := span.SpanContext()
		return spanctx.TraceID().String(), spanctx.IsSampled(), true
	}
	return "", false, false
}

// InfoFromContext is a method return details of span associated with context.
func InfoFromContext(ctx context.Context) (traceID string, sampled, found bool) {
	if ctx != nil {
		spanctx := trace.SpanContextFromContext(ctx)
		return spanctx.TraceID().String(), spanctx.IsSampled(), true
	}
	return "", false, false
}

func NoopSpan() *Span {
	_, span := trace.NewNoopTracerProvider().Tracer(globalNoopTracer).Start(context.TODO(), "noop")
	return &Span{
		Span: span,
	}
}

func CtxWithNoopSpan() context.Context {
	ctx, _ := trace.NewNoopTracerProvider().Tracer(globalNoopTracer).Start(context.TODO(), "noop")
	return ctx
}

func InjectContextToPulsarMsgProperties(ctx context.Context, properties map[string]string) {
	if properties == nil || ctx == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(properties))
}

func ExtractContextFromProperties(ctx context.Context, properties map[string]string) context.Context {
	if properties == nil || ctx == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(properties))
}
