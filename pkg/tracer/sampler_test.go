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
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	testClientTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testUpstreamSpan  = "00f067aa0ba902b7"
)

// serverPropagator mirrors the composition used by getServerHandlerOpts.
func serverPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
		clientRequestIDPropagator{},
	)
}

// startServerSpan extracts headers the way the gRPC server stats handler does, then starts
// the span that handler would start, and reports the resulting span context.
func startServerSpan(t *testing.T, headers map[string]string, ratio float64) trace.SpanContext {
	t.Helper()

	tp := sdk.NewTracerProvider(sdk.WithSampler(newClientRequestIDSampler(sdk.TraceIDRatioBased(ratio))))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx := serverPropagator().Extract(context.Background(), propagation.MapCarrier(headers))
	_, span := tp.Tracer("test").Start(ctx, "server-rpc")
	defer span.End()

	return span.SpanContext()
}

// TestClientRequestIDIsSampledByRatio is the regression test for client_request_id
// suppressing tracing: the synthesized parent carries no sampled flag, so a plain
// ParentBased sampler mapped it to NeverSample and dropped the trace no matter what
// trace.sampleFraction said.
func TestClientRequestIDIsSampledByRatio(t *testing.T) {
	t.Run("sampled when fraction is 1.0", func(t *testing.T) {
		sc := startServerSpan(t, map[string]string{clientRequestIDKey: testClientTraceID}, 1.0)

		assert.True(t, sc.IsSampled(), "client_request_id must not opt the request out of sampling")
		assert.Equal(t, testClientTraceID, sc.TraceID().String(), "trace must adopt the client supplied id")
	})

	t.Run("not sampled when fraction is 0.0", func(t *testing.T) {
		sc := startServerSpan(t, map[string]string{clientRequestIDKey: testClientTraceID}, 0.0)

		assert.False(t, sc.IsSampled(), "the configured ratio must still govern")
		assert.Equal(t, testClientTraceID, sc.TraceID().String())
	})

	t.Run("legacy header behaves the same", func(t *testing.T) {
		sc := startServerSpan(t, map[string]string{clientRequestIDKeyLegacy: testClientTraceID}, 1.0)

		assert.True(t, sc.IsSampled())
		assert.Equal(t, testClientTraceID, sc.TraceID().String())
	})
}

// TestTraceparentKeepsW3CSemantics guards the other half: a real upstream decision must be
// honored exactly, never overridden by the ratio.
func TestTraceparentKeepsW3CSemantics(t *testing.T) {
	t.Run("upstream sampled wins over ratio 0.0", func(t *testing.T) {
		sc := startServerSpan(t, map[string]string{
			"traceparent": "00-" + testClientTraceID + "-" + testUpstreamSpan + "-01",
		}, 0.0)

		assert.True(t, sc.IsSampled(), "an upstream sampled=1 must be honored")
		assert.Equal(t, testClientTraceID, sc.TraceID().String())
	})

	t.Run("upstream not sampled wins over ratio 1.0", func(t *testing.T) {
		sc := startServerSpan(t, map[string]string{
			"traceparent": "00-" + testClientTraceID + "-" + testUpstreamSpan + "-00",
		}, 1.0)

		assert.False(t, sc.IsSampled(), "an upstream sampled=0 must not be re-sampled")
	})
}

// TestTraceparentWinsOverClientRequestID pins the propagator ordering. Both headers present
// must resolve to the upstream decision, not the synthesized one.
func TestTraceparentWinsOverClientRequestID(t *testing.T) {
	otherTraceID := "0af7651916cd43dd8448eb211c80319c"

	sc := startServerSpan(t, map[string]string{
		"traceparent":      "00-" + otherTraceID + "-" + testUpstreamSpan + "-00",
		clientRequestIDKey: testClientTraceID,
	}, 1.0)

	assert.Equal(t, otherTraceID, sc.TraceID().String(), "traceparent must win")
	assert.False(t, sc.IsSampled(), "and its sampling decision must be preserved")
}

func TestNoHeadersFallsBackToRatio(t *testing.T) {
	assert.True(t, startServerSpan(t, nil, 1.0).IsSampled())
	assert.False(t, startServerSpan(t, nil, 0.0).IsSampled())
}

// TestClientCannotForceSamplingByChoosingTraceID is the regression test for a caller
// sampling itself at 100% against any ratio.
//
// TraceIDRatioBased reads its verdict from TraceID[8:16], and on the client_request_id path
// the trace ID is whatever the caller sent. Passing it to the sampler unchanged let a
// caller pick an ID under the threshold and be sampled every time -- defeating the whole
// reason the synthesized parent carries no sampled flag.
//
// The IDs below have all-zero low bytes, so they beat any non-zero threshold. With a
// fraction this small, a server-keyed decision rejects them.
func TestClientCannotForceSamplingByChoosingTraceID(t *testing.T) {
	luckyIDs := []string{
		"ffffffffffffffff0000000000000000",
		"00000000000000010000000000000000",
		"deadbeefdeadbeef0000000000000000",
	}

	const tinyFraction = 1e-9

	for _, id := range luckyIDs {
		t.Run(id, func(t *testing.T) {
			sc := startServerSpan(t, map[string]string{clientRequestIDKey: id}, tinyFraction)

			assert.False(t, sc.IsSampled(),
				"a caller must not be able to sample itself by choosing the trace ID")
			assert.Equal(t, id, sc.TraceID().String(),
				"the caller's ID is still adopted for log correlation, only the sampling key changes")
		})
	}
}

// TestSamplingKeyIsServerGenerated pins the mechanism: the ratio decision must come from
// the SpanID this server generated, never from the caller's trace ID.
func TestSamplingKeyIsServerGenerated(t *testing.T) {
	spanID, err := trace.SpanIDFromHex("00000000000000ff")
	require.NoError(t, err)

	key := samplingKeyFromSpanID(spanID)

	assert.Equal(t, "000000000000000000000000000000ff", key.String())
	assert.Equal(t, spanID[:], key[8:], "the low 8 bytes -- what TraceIDRatioBased reads -- must be the SpanID")
}

// TestSyntheticParent covers the guard that keeps the marker from leaking onto child
// spans. The marker lives in the context for the whole request, so without the span-ID
// check an in-process child would be re-sampled as a root and could disagree with its
// parent, splitting the trace.
func TestSyntheticParent(t *testing.T) {
	traceID, err := trace.TraceIDFromHex(testClientTraceID)
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex(testUpstreamSpan)
	require.NoError(t, err)

	t.Run("nil context", func(t *testing.T) {
		//nolint:staticcheck // SA1012: exercising the nil guard is the point of this case
		_, ok := syntheticParent(nil)
		assert.False(t, ok)
	})

	t.Run("no marker", func(t *testing.T) {
		_, ok := syntheticParent(context.Background())
		assert.False(t, ok)
	})

	t.Run("marker matching the remote parent", func(t *testing.T) {
		ctx := withSyntheticParent(context.Background(), spanID)
		ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
			Remote:  true,
		}))

		_, ok := syntheticParent(ctx)
		assert.True(t, ok)
	})

	t.Run("marker present but parent is a local child span", func(t *testing.T) {
		localSpanID, err := trace.SpanIDFromHex("1112131415161718")
		require.NoError(t, err)

		ctx := withSyntheticParent(context.Background(), spanID)
		// A child span started in-process: same trace, different span, not remote.
		ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  localSpanID,
		}))

		_, ok := syntheticParent(ctx)
		assert.False(t, ok,
			"a child span must fall through to ParentBased, not be re-sampled as a root")
	})
}

// TestChildSpanFollowsParentDecision is the end-to-end consequence of the guard above:
// once the root decision is made, in-process children must inherit it.
func TestChildSpanFollowsParentDecision(t *testing.T) {
	tp := sdk.NewTracerProvider(sdk.WithSampler(newClientRequestIDSampler(sdk.TraceIDRatioBased(1.0))))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test")

	ctx := serverPropagator().Extract(context.Background(),
		propagation.MapCarrier{clientRequestIDKey: testClientTraceID})

	ctx, root := tracer.Start(ctx, "server-rpc")
	defer root.End()
	require.True(t, root.SpanContext().IsSampled())

	_, child := tracer.Start(ctx, "child")
	defer child.End()

	assert.True(t, child.SpanContext().IsSampled())
	assert.Equal(t, root.SpanContext().TraceID(), child.SpanContext().TraceID())

	ro, ok := child.(sdk.ReadOnlySpan)
	require.True(t, ok, "a sampled span is recording and exposes ReadOnlySpan")
	assert.Equal(t, root.SpanContext().SpanID(), ro.Parent().SpanID(),
		"child must hang off the real root span, not the synthesized parent")
}

func TestClientRequestIDSamplerDescription(t *testing.T) {
	s := newClientRequestIDSampler(sdk.TraceIDRatioBased(0.5))
	assert.Contains(t, s.Description(), "ClientRequestIDSampler")
}
