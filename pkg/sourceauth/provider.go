// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sourceauth

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CredentialStatus carries a safe classification across the native callback
// boundary. HTTP bodies, transport errors and tokens must not cross as text.
type CredentialStatus uint32

const (
	CredentialOK CredentialStatus = iota
	CredentialDenied
	CredentialExpired
	CredentialTransient
	CredentialCancelled
	CredentialInvalidResponse
)

type ReadOperation uint32

const (
	OperationRead ReadOperation = iota
	OperationList
)

type FetchRequest struct {
	ResourceURI       string
	ScopeRootURI      string
	Operation         ReadOperation
	RequestDeadline   time.Time
	OperationDeadline time.Time // optional signed-authority deadline; not an RPC timeout
}

// Credential is transient; its JSON/debug representations are redacted.
// The storage adapter copies it into native-owned memory for one callback.
// Providers must recheck the bound owner's current READ on every issuance.
type Credential struct {
	Status       CredentialStatus
	AccountName  string
	ScopeRootURI string
	SASToken     string
	ExpiresAt    time.Time
}

func (Credential) String() string               { return "external source credential (redacted)" }
func (c Credential) GoString() string           { return c.String() }
func (Credential) MarshalJSON() ([]byte, error) { return []byte(`"<redacted>"`), nil }

// Session owns one reader's provider. Native readers retain it until their
// final lazy read and in-flight callbacks finish. Implementations must support
// concurrent Fetch calls and honor their context and request deadlines.
// Close runs exactly once after all owners and callbacks have drained.
type Session interface {
	AccountName() string
	AllowedRootURIs() []string
	Fetch(context.Context, FetchRequest) Credential
	Close()
}

// Factory opens a fresh session from trusted coordinator state. It must not
// trust a reference from request Properties or reuse another owner's session.
// The context lasts for the reader operation, not the original Proxy RPC.
type Factory func(context.Context, Binding) (Session, error)

// Registry is populated by the distribution at process startup. A missing
// factory is an unsupported capability, never permission to use ambient
// credentials. Entries cannot be replaced while readers retain sessions.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func (r *Registry) Register(name string, factory Factory) error {
	if name == "" || factory == nil {
		return merr.WrapErrServiceInternalMsg("invalid external source provider registration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.factories == nil {
		r.factories = make(map[string]Factory)
	}
	if _, exists := r.factories[name]; exists {
		return merr.WrapErrServiceInternalMsg("external source provider is already registered")
	}
	r.factories[name] = factory
	return nil
}

// Open returns nil only for an unbound source. Each bound source gets a fresh
// session; caches belong to the native context created for this session.
func (r *Registry) Open(ctx context.Context, source, spec string) (Session, error) {
	binding, err := Read(source, spec)
	if err != nil || binding == nil {
		return nil, err
	}
	r.mu.RLock()
	factory := r.factories[binding.Provider]
	r.mu.RUnlock()
	if factory == nil {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "external source provider is not installed")
	}
	session, err := factory(ctx, *binding)
	if err != nil {
		if session != nil {
			session.Close()
		}
		return nil, err
	}
	if session == nil {
		return nil, merr.WrapErrServiceInternalMsg("external source provider returned no session")
	}
	if session.AccountName() == "" || len(session.AllowedRootURIs()) == 0 {
		session.Close()
		return nil, merr.WrapErrServiceInternalMsg("external source provider returned an unscoped session")
	}
	return session, nil
}
