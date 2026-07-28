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
	"crypto/rand"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	clientRequestIDKeyLegacy = "client-request-id"
	clientRequestIDKey       = "client_request_id"
)

// syntheticParentKey marks a context whose remote span context was synthesized from a
// client_request_id header rather than extracted from a real upstream traceparent. The
// value is the synthesized SpanID, so the sampler can tell that span context apart from
// any other parent it might later be asked about.
//
// This is an in-process marker only: it is a context value, never a span-context field, so
// it is not propagated to downstream components. Those receive a normal traceparent whose
// sampled flag already reflects the decision made here.
type syntheticParentKey struct{}

// withSyntheticParent records that spanID was synthesized locally rather than propagated.
func withSyntheticParent(ctx context.Context, spanID trace.SpanID) context.Context {
	return context.WithValue(ctx, syntheticParentKey{}, spanID)
}

// hasSyntheticParent reports whether the parent span context in ctx is one this package
// synthesized from a client_request_id.
//
// It re-checks the span context rather than trusting the marker alone: the marker stays in
// the context for the whole request, so a child span started later in the same process
// would otherwise be misidentified as having a synthetic parent and be re-sampled
// independently of its real parent.
func hasSyntheticParent(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	spanID, ok := ctx.Value(syntheticParentKey{}).(trace.SpanID)
	if !ok {
		return false
	}
	psc := trace.SpanContextFromContext(ctx)
	return psc.IsRemote() && psc.SpanID() == spanID
}

type clientRequestIDPropagator struct{}

func (clientRequestIDPropagator) Inject(_ context.Context, _ propagation.TextMapCarrier) {
}

func (clientRequestIDPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	// A real traceparent always wins. This propagator is composed after the W3C one, so a
	// valid span context here means TraceContext already extracted an upstream decision.
	if trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}

	traceID, err := trace.TraceIDFromHex(firstCarrierValue(carrier, clientRequestIDKey, clientRequestIDKeyLegacy))
	if err != nil || !traceID.IsValid() {
		return ctx
	}

	spanID := newSpanID()
	if !spanID.IsValid() {
		return ctx
	}

	// The synthesized context carries no sampled flag on purpose: a client must not be
	// able to force sampling on. Instead it is marked as synthetic, and clientRequestIDSampler
	// samples it as a root span so trace.sampleFraction governs the decision. Without that
	// marker the ParentBased sampler would read "remote parent, not sampled" and drop the
	// trace outright.
	ctx = withSyntheticParent(ctx, spanID)

	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
		Remote:  true,
	}))
}

func (clientRequestIDPropagator) Fields() []string {
	return []string{clientRequestIDKey, clientRequestIDKeyLegacy}
}

func firstCarrierValue(carrier propagation.TextMapCarrier, keys ...string) string {
	for _, key := range keys {
		if value := carrier.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func newSpanID() trace.SpanID {
	var spanID trace.SpanID
	if _, err := rand.Read(spanID[:]); err != nil {
		return trace.SpanID{}
	}
	return spanID
}
