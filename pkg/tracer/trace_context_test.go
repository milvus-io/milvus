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
	"go.opentelemetry.io/otel/trace"
)

// mapCarrier implements propagation.TextMapCarrier for testing
type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string {
	return c[key]
}

func (c mapCarrier) Set(key, value string) {
	c[key] = value
}

func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func TestTraceContext_Inject(t *testing.T) {
	tc := TraceContext{}
	ctx := context.Background()

	// Create a span context with valid trace ID
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx = trace.ContextWithSpanContext(ctx, sc)

	carrier := make(mapCarrier)
	tc.Inject(ctx, carrier)

	// Should have injected traceparent header
	assert.NotEmpty(t, carrier.Get("traceparent"))
}

func TestTraceContext_Extract_WithValidTraceParent(t *testing.T) {
	tc := TraceContext{}
	ctx := context.Background()

	carrier := make(mapCarrier)
	// Set a valid W3C traceparent header
	carrier["traceparent"] = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"

	ctx = tc.Extract(ctx, carrier)

	// Should have extracted the span context
	sc := trace.SpanFromContext(ctx).SpanContext()
	assert.True(t, sc.IsValid())
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", sc.TraceID().String())
}

func TestTraceContext_Extract_WithLegacyClientRequestID(t *testing.T) {
	tc := TraceContext{}

	t.Run("client-request-id key", func(t *testing.T) {
		ctx := context.Background()
		carrier := make(mapCarrier)
		// Set legacy client-request-id header
		carrier["client-request-id"] = "0102030405060708090a0b0c0d0e0f10"

		ctx = tc.Extract(ctx, carrier)

		// Should have extracted the span context
		sc := trace.SpanFromContext(ctx).SpanContext()
		assert.True(t, sc.IsValid())
		assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", sc.TraceID().String())
	})

	t.Run("client_request_id key", func(t *testing.T) {
		ctx := context.Background()
		carrier := make(mapCarrier)
		// Set legacy client_request_id header
		carrier["client_request_id"] = "0102030405060708090a0b0c0d0e0f10"

		ctx = tc.Extract(ctx, carrier)

		// Should have extracted the span context
		sc := trace.SpanFromContext(ctx).SpanContext()
		assert.True(t, sc.IsValid())
		assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", sc.TraceID().String())
	})
}

func TestTraceContext_Extract_WithInvalidLegacyID(t *testing.T) {
	tc := TraceContext{}
	ctx := context.Background()

	carrier := make(mapCarrier)
	// Set an invalid trace ID (not a valid hex)
	carrier["client-request-id"] = "invalid-trace-id"

	ctx = tc.Extract(ctx, carrier)

	// Should not have a valid span context
	sc := trace.SpanFromContext(ctx).SpanContext()
	assert.False(t, sc.IsValid())
}

func TestTraceContext_Extract_WithNoTraceContext(t *testing.T) {
	tc := TraceContext{}
	ctx := context.Background()

	carrier := make(mapCarrier)
	// Empty carrier

	ctx = tc.Extract(ctx, carrier)

	// Should not have a valid span context
	sc := trace.SpanFromContext(ctx).SpanContext()
	assert.False(t, sc.IsValid())
}

func TestTraceContext_Fields(t *testing.T) {
	tc := TraceContext{}
	fields := tc.Fields()

	// TraceContext should return the standard W3C trace context fields
	assert.Contains(t, fields, "traceparent")
}
