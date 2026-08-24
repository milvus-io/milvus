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

package tracer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestClientRequestIDPropagatorExtract(t *testing.T) {
	requestTraceID := "0102030405060708090a0b0c0d0e0f10"
	ctx := clientRequestIDPropagator{}.Extract(context.Background(), propagation.MapCarrier{
		clientRequestIDKey: requestTraceID,
	})

	spanCtx := trace.SpanContextFromContext(ctx)
	assert.Equal(t, requestTraceID, spanCtx.TraceID().String())
	assert.True(t, spanCtx.HasSpanID())
	assert.True(t, spanCtx.IsRemote())
}

func TestClientRequestIDPropagatorExtractLegacyKey(t *testing.T) {
	requestTraceID := "0102030405060708090a0b0c0d0e0f10"
	ctx := clientRequestIDPropagator{}.Extract(context.Background(), propagation.MapCarrier{
		clientRequestIDKeyLegacy: requestTraceID,
	})

	assert.Equal(t, requestTraceID, trace.SpanContextFromContext(ctx).TraceID().String())
}

func TestClientRequestIDPropagatorIgnoreInvalidTraceID(t *testing.T) {
	ctx := clientRequestIDPropagator{}.Extract(context.Background(), propagation.MapCarrier{
		clientRequestIDKey: "invalid",
	})

	assert.False(t, trace.SpanContextFromContext(ctx).IsValid())
}

func TestClientRequestIDPropagatorDoesNotOverrideExistingContext(t *testing.T) {
	existingTraceID := "0102030405060708090a0b0c0d0e0f10"
	traceID, err := trace.TraceIDFromHex(existingTraceID)
	assert.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	assert.NoError(t, err)

	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))
	ctx = clientRequestIDPropagator{}.Extract(ctx, propagation.MapCarrier{
		clientRequestIDKey: "11111111111111111111111111111111",
	})

	assert.Equal(t, existingTraceID, trace.SpanContextFromContext(ctx).TraceID().String())
}

func TestClientRequestIDPropagatorDoesNotOverrideTraceContext(t *testing.T) {
	requestTraceID := "0102030405060708090a0b0c0d0e0f10"
	traceparentTraceID := "11111111111111111111111111111111"
	ctx := propagation.NewCompositeTextMapPropagator(
		clientRequestIDPropagator{},
		propagation.TraceContext{},
	).Extract(context.Background(), propagation.MapCarrier{
		clientRequestIDKey: requestTraceID,
		"traceparent":      "00-" + traceparentTraceID + "-2222222222222222-01",
	})

	assert.Equal(t, traceparentTraceID, trace.SpanContextFromContext(ctx).TraceID().String())
}
