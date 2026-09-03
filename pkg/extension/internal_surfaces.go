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

package extension

// InternalListeners is what a form asks InternalSurfaces to open. It is a
// struct so that a later need - a TLS configuration, a separate metrics port
// - can be carried as a new field without a new method (see the package
// evolution policy).
type InternalListeners struct {
	// GRPCPort is the port of the internal-domain gRPC listener, which
	// serves the unauthenticated MilvusService. Zero leaves it closed.
	GRPCPort int

	// RESTPort is the port of the internal REST listener, which serves the
	// unauthenticated /v2/vectordb surface and /metrics. Zero leaves it
	// closed.
	RESTPort int

	// BindAddress is the interface both listeners bind to. Empty binds every
	// interface, which is what the fork this replaces did; a form whose
	// internal domain reaches the pod on one interface names it here so the
	// unauthenticated surface is not reachable on the others.
	BindAddress string
}

// InternalSurfaces declares the unauthenticated internal-domain listeners a
// deployment form serves its own control plane on.
//
// The shape comes from how a managed cloud reaches an instance it operates.
// The instance's EXTERNAL listeners authenticate end users - and a form may
// close them entirely until credentials are provisioned - but the control
// plane that creates databases, seeds accounts and sizes query clusters
// reaches the instance over a network path that is not the public one: a
// cross-cluster internal domain whose access control lives in the gateway in
// front of it, not in milvus. The fork this mechanism replaces served that
// plane on two fixed listeners - a second MilvusService gRPC server and a
// second REST server carrying /metrics - with no authentication interceptor,
// and every existing instance's control plane still speaks to those ports.
// A form that declares this capability is asking for exactly those listeners,
// so a new instance is operable by the same control plane as every old one.
//
// # What declaring this exposes
//
// Both listeners serve the FULL MilvusService with neither authentication nor
// privilege checks. That is safe under exactly one assumption: nothing
// reaches the ports except the control plane, enforced outside the process.
// A form must not declare this capability unless its deployment guarantees
// that isolation.
//
// With no provider installed - or the capability nil - no listener is opened
// and milvus serves exactly the surfaces it always did.
//
// FROZEN under the package evolution policy: there is no inert answer to
// "which ports", so there is no Noop base and no method is ever added; what
// the listeners need is carried by fields on InternalListeners.
type InternalSurfaces interface {
	// InternalDomainListeners returns the listeners to open. It is called
	// once, while the proxy starts.
	InternalDomainListeners() InternalListeners
}
