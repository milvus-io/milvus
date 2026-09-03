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

// Package extension is milvus's deployment-form extension framework.
//
// A distribution links its own implementation of the capability interfaces and
// installs one Provider at boot. A stock milvus binary installs none, so every
// capability is nil and each seam falls through to the native path unchanged.
//
// # What consumes the table
//
// This package defines the table and the interfaces; it does not consult
// them. The seams - the places in proxy, rootcoord, querycoord and the
// coordinator server that resolve a capability and act on it - are added by
// the changes that wire each interface, and every doc comment below that
// describes what a seam does is the contract those changes must meet, not a
// description of code that already exists. Until a seam lands, the
// corresponding capability is declared but inert, and a form implementing it
// gets nothing from it yet. The design document
// (docs/design-docs/design_docs/20260831-in_tree_extension_mechanism.md)
// tracks which seams have landed.
//
// # Evolution policy
//
// The table and the interfaces are consumed by code outside this repository,
// so they change under one rule set, written down once here and referenced by
// the types:
//
//   - Capabilities gains fields; it never loses or renames one. A new
//     capability is a new field with a new interface, which keeps every
//     existing Provider compiling.
//   - An interface a FORM implements either carries a Noop base type (every
//     method has an inert answer, and NoopXxx gives it) or is FROZEN. A method
//     may be added to an interface with a Noop base only together with its
//     inert default, so an implementation that embeds the base keeps
//     compiling and keeps the native behavior for the new method. A FROZEN
//     interface never gains a method: a need it cannot express becomes a new
//     Capabilities field. Each interface says which it is.
//   - An interface MILVUS implements and hands to a form (MixCoord,
//     ProxyConnections, CredentialStore) may gain methods freely and never
//     loses one; a form only calls these.
//   - Structs a form receives or returns (QueryPlacement,
//     ResourceGroupUpdate, ShardLeaderReadiness, InternalListeners) gain
//     fields, never lose them, so a later decision can be carried without a
//     new method.
//
// # Relation to hookutil
//
// milvus's older extension point (internal/util/hookutil) covers the request
// path: Mock, Before and After are consulted for every unary RPC, and
// VerifyAPIKey answers the API key. That is not duplicated here. A form that
// needs any of it fills in the Hook field and gets milvus's own mechanism,
// unchanged; hookutil's only gap was that its sole installation path was
// proxy.soPath, which dlopens a file a compiled-in form does not have.
//
// What this package adds is the places hookutil never reaches - coordinator
// internals, listeners, load semantics, resource groups - and the typed
// interfaces they need. hookutil itself is frozen at what its existing users
// need: a capability is added here, never to it.
package extension

import (
	"reflect"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CapabilityID names one entry of the capability table.
type CapabilityID string

const (
	// CapCoordinatorEngine is control-plane machinery hosted in the
	// coordinator process, with its own gRPC services on the coordinator's
	// server.
	CapCoordinatorEngine CapabilityID = "coordinator_engine"

	// CapHook is milvus's own request hook, compiled into the binary rather
	// than loaded from proxy.soPath.
	CapHook CapabilityID = "hook"
)

// Capabilities is the table a Provider fills in. A nil field means the
// capability is not taken over and the native path applies.
//
// Fields are only ever added, never removed or renamed - see the evolution
// policy in the package documentation. Adding a capability means adding a
// field here, a CapabilityID above, and one line in entries below; the
// registry tests walk entries against the CapabilityID list, so a field
// forgotten in entries fails the tests instead of being silently
// unrequirable.
type Capabilities struct {
	CoordinatorEngine CoordinatorEngine

	// Hook is milvus's own request hook (milvus-proto go-api/v3/hook), the
	// interface whose Mock, Before and After the proxy's unary interceptor
	// already consults for every RPC and whose VerifyAPIKey already answers
	// the API key.
	//
	// It is here rather than as another interface of this package's own
	// because it is not this package's to define: the interception a form
	// needs on the request path is the interception milvus already performs,
	// and the only thing that was missing was a way to install one without a
	// .so. Filling this field is that way. hookutil installs it at start-up,
	// and refuses to when proxy.soPath also names a plug-in, because two
	// authorities for the same question is a deployment mistake rather than
	// something to merge.
	//
	// The interface is milvus-proto's and FROZEN from this package's side. A
	// need it cannot express becomes a new field here, not a method on it.
	Hook hook.Hook
}

// capabilityEntry is one row of the table in a form the registry can walk:
// the identifier and the interface value stored under it.
type capabilityEntry struct {
	id  CapabilityID
	val any
}

// entries is the ONE place the table's fields are enumerated. has,
// typedNilCapability and the registry tests all go through it, so a
// capability is either listed here - and therefore requirable, typed-nil
// checked and tested - or does not exist.
func (c Capabilities) entries() []capabilityEntry {
	return []capabilityEntry{
		{CapCoordinatorEngine, c.CoordinatorEngine},
		{CapHook, c.Hook},
	}
}

// lookup returns the value stored under id and whether id names a
// capability at all. The second result is what lets SetProvider tell a
// mistyped requirement from a real one that was not supplied.
func (c Capabilities) lookup(id CapabilityID) (any, bool) {
	for _, e := range c.entries() {
		if e.id == id {
			return e.val, true
		}
	}
	return nil, false
}

func (c Capabilities) has(id CapabilityID) bool {
	v, known := c.lookup(id)
	return known && v != nil
}

// typedNilCapability reports the first capability field holding a typed nil:
// an interface that is non-nil (so has() counts it as supplied) wrapping a nil
// pointer (so its first method call panics). reflect runs once, at install
// time, never on a hot path.
func (c Capabilities) typedNilCapability() (CapabilityID, bool) {
	for _, e := range c.entries() {
		if e.val == nil {
			continue
		}
		v := reflect.ValueOf(e.val)
		switch v.Kind() {
		case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			if v.IsNil() {
				return e.id, true
			}
		}
	}
	return "", false
}

// Provider is one deployment form's extension implementation.
type Provider interface {
	// Name identifies the provider in logs and errors.
	Name() string
	// Requires lists the capabilities this form cannot run without. SetProvider
	// fails when any of them is absent from the table, so a wiring mistake stops
	// the process instead of silently degrading to the native path.
	Requires() []CapabilityID
	// Capabilities returns the table. SetProvider calls it exactly once and
	// stores what it returned; a later call would not be consulted.
	Capabilities() Capabilities
}

type box struct {
	provider Provider
	caps     Capabilities
}

var installed atomic.Pointer[box]

// SetProvider installs the provider and verifies its declared requirements.
// It may be called at most once, before any component starts - nothing
// enforces the ordering, but a provider installed after a component consulted
// the table has already been ignored by that component, so the requirement is
// on the wiring (a distribution's main installs before it calls
// cmd/milvus.Main), not on this function.
//
// Every error it returns is merr.ErrServiceInternal: a failed installation is
// a wiring bug in the binary, never a request or a transient condition.
func SetProvider(p Provider) error {
	if p == nil {
		return merr.WrapErrServiceInternal("extension: nil provider")
	}
	// The same typed-nil trap the capability fields get below: a nil
	// *concreteProvider stored in the interface passes the check above and
	// then panics on p.Capabilities(). Catch it here, before the first call.
	if v := reflect.ValueOf(p); v.Kind() == reflect.Ptr && v.IsNil() {
		return merr.WrapErrServiceInternalMsg("extension: provider is a typed nil (%T)", p)
	}
	c := p.Capabilities()
	for _, id := range p.Requires() {
		v, known := c.lookup(id)
		if !known {
			return merr.WrapErrServiceInternalMsg("extension: provider %q requires unknown capability %q", p.Name(), id)
		}
		if v == nil {
			return merr.WrapErrServiceInternalMsg("extension: provider %q requires capability %q but did not supply it", p.Name(), id)
		}
	}
	// A typed-nil capability - a nil *concrete stored in an interface field -
	// would pass has() and then panic at the first seam that calls it. Catch
	// the wiring mistake here, once, where the provider's name is at hand.
	if id, ok := c.typedNilCapability(); ok {
		return merr.WrapErrServiceInternalMsg("extension: provider %q supplies capability %q as a typed nil, which would panic at its first use", p.Name(), id)
	}
	if !installed.CompareAndSwap(nil, &box{provider: p, caps: c}) {
		// Load again rather than trusting the CAS: under the test tag the
		// slot can be cleared between the two, and a nil here must not turn
		// a clear error into a panic.
		name := "<unknown>"
		if prev := installed.Load(); prev != nil && prev.provider != nil {
			name = prev.provider.Name()
		}
		return merr.WrapErrServiceInternalMsg("extension: provider %q already installed, refusing %q", name, p.Name())
	}
	return nil
}

// zeroCaps backs Caps() when no provider is installed, so the nil-provider
// path is the same single pointer load as the installed one.
var zeroCaps = &Capabilities{}

// Caps returns the installed capability table, or the zero table when no
// provider was installed.
//
// The pointer is READ-ONLY by contract, and the contract is all there is: the
// table is written once by SetProvider and shared by every caller, and it is
// returned by pointer precisely so the hot paths (Search, Query, Insert,
// per-channel routing) pay one atomic load and one nil comparison rather than
// copying the whole struct per call. A caller that assigns through it
// bypasses SetProvider's checks and races every other reader; nothing stops
// that, deliberately, because the alternatives - a copy per call, or a
// getter per capability - would put the cost on every request to guard
// against a misuse no seam has a reason to commit.
func Caps() *Capabilities {
	if b := installed.Load(); b != nil {
		return &b.caps
	}
	return zeroCaps
}
