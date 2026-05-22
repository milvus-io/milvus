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

type clientRequestIDPropagator struct{}

func (clientRequestIDPropagator) Inject(_ context.Context, _ propagation.TextMapCarrier) {
}

func (clientRequestIDPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
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
