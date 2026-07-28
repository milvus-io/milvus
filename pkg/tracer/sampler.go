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
	sdk "go.opentelemetry.io/otel/sdk/trace"
)

// clientRequestIDSampler makes a client-supplied request ID seed the trace ID without also
// acting as an upstream sampling decision.
//
// A client_request_id is turned into a remote span context by clientRequestIDPropagator so
// that the resulting trace carries the caller's ID. That span context has no sampled flag,
// because a client must not be able to switch sampling on. But ParentBased maps
// "remote parent, not sampled" to NeverSample, so on its own that arrangement drops every
// request carrying a client_request_id -- regardless of trace.sampleFraction, and for the
// whole downstream call tree.
//
// This sampler restores the intended behavior: a synthetic parent is sampled as if the
// span were a root, so trace.sampleFraction decides. Everything else, including a genuine
// upstream traceparent that says sampled=0, is delegated to ParentBased and keeps standard
// W3C semantics.
type clientRequestIDSampler struct {
	// root decides for spans whose parent was synthesized locally.
	root sdk.Sampler
	// delegate decides for every other span.
	delegate sdk.Sampler
}

// newClientRequestIDSampler builds the sampler used by SetTracerProvider. root is applied
// to synthetic parents and is also the root sampler of the ParentBased delegate, so both
// paths use the same sampling ratio.
func newClientRequestIDSampler(root sdk.Sampler) sdk.Sampler {
	return clientRequestIDSampler{
		root:     root,
		delegate: sdk.ParentBased(root),
	}
}

func (s clientRequestIDSampler) ShouldSample(p sdk.SamplingParameters) sdk.SamplingResult {
	if hasSyntheticParent(p.ParentContext) {
		// Not a real upstream decision -- decide as a root span would.
		return s.root.ShouldSample(p)
	}
	return s.delegate.ShouldSample(p)
}

func (s clientRequestIDSampler) Description() string {
	return "ClientRequestIDSampler{" + s.delegate.Description() + "}"
}
