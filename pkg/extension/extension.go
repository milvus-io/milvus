// Package extension is milvus's deployment-form extension framework.
//
// A distribution links its own implementation of the capability interfaces and
// installs one Provider at boot. A stock milvus binary installs none, so every
// capability is nil and each seam falls through to the native path unchanged.
//
// Relation to hookutil: milvus's older extension point (internal/util/hookutil)
// loads a .so at start-up and covers request observation and API-key
// verification for the dedicated cloud form. The two coexist deliberately:
// hookutil stays the surface for binary plug-ins built out of tree, while
// this package is for forms compiled into the binary, whose capabilities need
// typed interfaces and places hookutil never reaches (coordinator internals,
// listeners, load semantics). A capability is added here, never to hookutil;
// hookutil is frozen at what its existing users need.
package extension

import (
	"reflect"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CapabilityID names one entry of the capability table.
type CapabilityID string

const (
	// CapProxyExtension is the proxy-side behavior takeover.
	CapProxyExtension CapabilityID = "proxy_extension"

	// CapAPIKey is API key verification.
	CapAPIKey CapabilityID = "api_key"

	// CapRBACBootstrap is account and role seeding at rootcoord startup.
	CapRBACBootstrap CapabilityID = "rbac_bootstrap"

	// CapAdmission is per-instance admission checking on DDL such as
	// collection and database creation.
	CapAdmission CapabilityID = "admission"

	// CapCoordinatorEngine is control-plane machinery hosted in the
	// coordinator process, with its own gRPC services on the coordinator's
	// server.
	CapCoordinatorEngine CapabilityID = "coordinator_engine"

	// CapResourceGroupInterceptor is interception of the resource-group
	// requests the coordinator receives.
	CapResourceGroupInterceptor CapabilityID = "resource_group_interceptor"

	// CapIndexDrain is the graceful drop of a vector index on a loaded
	// collection.
	CapIndexDrain CapabilityID = "index_drain"

	// CapLoadPlacementScope is whether a load request states the
	// collection's whole desired placement or only the placement of the
	// resource groups it names.
	CapLoadPlacementScope CapabilityID = "load_placement_scope"

	// CapInternalSurfaces is the unauthenticated internal-domain listeners a
	// form serves its control plane on.
	CapInternalSurfaces CapabilityID = "internal_surfaces"
)

// Capabilities is the table a Provider fills in. A nil field means the
// capability is not taken over and the native path applies.
//
// Fields are only ever added, never removed or renamed: adding a field is
// additive and keeps existing implementations compiling, whereas adding a
// method to an interface would break every one of them.
type Capabilities struct {
	ProxyExt          ProxyExtension
	APIKey            APIKeyVerifier
	RBACBootstrap     RBACBootstrapper
	Admission         AdmissionChecker
	CoordinatorEngine CoordinatorEngine
	ResourceGroups    ResourceGroupInterceptor
	IndexDrain        IndexDrainer
	LoadPlacement     LoadPlacementScope
	InternalSurfaces  InternalSurfaces
}

func (c Capabilities) has(id CapabilityID) bool {
	switch id {
	case CapProxyExtension:
		return c.ProxyExt != nil
	case CapAPIKey:
		return c.APIKey != nil
	case CapRBACBootstrap:
		return c.RBACBootstrap != nil
	case CapAdmission:
		return c.Admission != nil
	case CapCoordinatorEngine:
		return c.CoordinatorEngine != nil
	case CapResourceGroupInterceptor:
		return c.ResourceGroups != nil
	case CapIndexDrain:
		return c.IndexDrain != nil
	case CapLoadPlacementScope:
		return c.LoadPlacement != nil
	case CapInternalSurfaces:
		return c.InternalSurfaces != nil
	default:
		return false
	}
}

// typedNilCapability reports the first capability field holding a typed nil:
// an interface that is non-nil (so has() counts it as supplied) wrapping a nil
// pointer (so its first method call panics). reflect runs once, at install
// time, never on a hot path.
func (c Capabilities) typedNilCapability() (CapabilityID, bool) {
	fields := []struct {
		id  CapabilityID
		val any
	}{
		{CapProxyExtension, c.ProxyExt},
		{CapAPIKey, c.APIKey},
		{CapRBACBootstrap, c.RBACBootstrap},
		{CapAdmission, c.Admission},
		{CapCoordinatorEngine, c.CoordinatorEngine},
		{CapResourceGroupInterceptor, c.ResourceGroups},
		{CapIndexDrain, c.IndexDrain},
		{CapLoadPlacementScope, c.LoadPlacement},
		{CapInternalSurfaces, c.InternalSurfaces},
	}
	for _, f := range fields {
		if f.val == nil {
			continue
		}
		v := reflect.ValueOf(f.val)
		switch v.Kind() {
		case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			if v.IsNil() {
				return f.id, true
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
	// Capabilities returns the table.
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
// on the wiring (cmd/milvus installs before roles start), not on this
// function.
func SetProvider(p Provider) error {
	if p == nil {
		return merr.WrapErrServiceInternal("extension: nil provider")
	}
	c := p.Capabilities()
	for _, id := range p.Requires() {
		if !c.has(id) {
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
		return merr.WrapErrServiceInternalMsg("extension: provider %q already installed", installed.Load().provider.Name())
	}
	return nil
}

// zeroCaps backs Caps() when no provider is installed, so the nil-provider
// path is the same single pointer load as the installed one.
var zeroCaps = &Capabilities{}

// Caps returns the installed capability table, or the zero table when no
// provider was installed. The pointer is READ-ONLY by contract: the table is
// written once by SetProvider and shared by every caller, and it is returned
// by pointer precisely so the hot paths (Search, Query, Insert, per-channel
// routing) pay one atomic load and one nil comparison rather than copying an
// eight-field struct per call.
func Caps() *Capabilities {
	if b := installed.Load(); b != nil {
		return &b.caps
	}
	return zeroCaps
}
