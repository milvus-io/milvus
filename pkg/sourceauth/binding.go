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

// Package sourceauth separates a durable external-source authorization reference
// from the credentials used by a particular reader. References are created by a
// trusted request hook, not supplied by an external collection client.
package sourceauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const BindingKey = "credential_binding"

// Binding is durable identity metadata, not a bearer capability. Reference must
// identify a server-owned authorization record or contain non-secret identity
// metadata understood by Provider. It must never contain a user token or SAS.
// The provider reauthorizes this binding whenever it issues storage credentials.
type Binding struct {
	Version   int    `json:"version"`
	Provider  string `json:"provider"`
	Reference string `json:"reference"`
	// Generation identifies this authorization attempt, not a Catalog table
	// incarnation. Same-name table deletion/recreation is not detected.
	Generation string `json:"generation"`
	Source     string `json:"source"`
	SpecSHA256 string `json:"spec_sha256"`
}

// String deliberately omits provider-specific identity metadata.
func (b Binding) String() string { return "external source credential binding (redacted)" }

type Operation struct {
	Database     string
	Collection   string
	CollectionID int64 // zero during create, before the coordinator allocates it
	Source       string
	Spec         string   // canonical JSON with no credential_binding
	Previous     *Binding // trusted stored binding for an omitted refresh; never authority for the new caller
}

// Reference is returned only after the hook verifies the initiating user and
// source-table read permission. It contains no personal or storage credentials.
type Reference struct {
	Provider string
	Value    string
}

type Authorizer func(context.Context, Operation) (Reference, error)

type authorizerKey struct{}

// WithAuthorizer lets an existing request hook authorize the fully resolved
// source. Refresh can omit source/spec, so this must run after metadata lookup.
// The callback and any caller token it captures never cross the Proxy RPC.
func WithAuthorizer(ctx context.Context, authorize Authorizer) context.Context {
	return context.WithValue(ctx, authorizerKey{}, authorize)
}

func HasAuthorizer(ctx context.Context) bool {
	authorize, _ := ctx.Value(authorizerKey{}).(Authorizer)
	return authorize != nil
}

// RejectClientBinding guards entry points that accept an external specification
// from outside trusted coordinator state, including snapshot restore. Even a
// null or escaped binding key is reserved; a valid digest is not provenance.
func RejectClientBinding(spec string) error {
	object, err := decodeObject(spec)
	if err != nil {
		return err
	}
	if _, present := object[BindingKey]; present {
		return merr.WrapErrParameterInvalidMsg("credential_binding is server-owned")
	}
	return nil
}

// PrepareRefresh resolves an omitted source against trusted collection state.
// Explicit replacement of an already bound source also requires the initiating
// user's authorizer: replacing the JSON must not disable source authorization.
// With no authorizer and no binding, omitted fields stay omitted so DataCoord
// retains the legacy behavior of resolving its current schema at admission.
func PrepareRefresh(ctx context.Context, stored Operation, source, spec string) (string, string, error) {
	previous, err := Read(stored.Source, stored.Spec)
	if err != nil {
		return "", "", err
	}
	if previous != nil && !HasAuthorizer(ctx) {
		return "", "", merr.WrapErrPrivilegeNotPermitted("source authorization is required to refresh this collection")
	}
	explicit := source != "" || spec != ""
	if explicit && (source == "" || spec == "") {
		return "", "", merr.WrapErrParameterInvalidMsg("external source and specification must be both provided or both omitted")
	}
	if !explicit && !HasAuthorizer(ctx) {
		return "", "", nil
	}
	operation := stored
	if explicit {
		operation.Source, operation.Spec = source, spec
	}
	authorized, err := Prepare(ctx, operation, explicit)
	if err != nil {
		return "", "", err
	}
	return operation.Source, authorized, nil
}

// Prepare creates a fresh reference for this operation. clientSpec distinguishes
// public request input from the stored spec resolved for an omitted refresh.
// Existing bindings never authorize a new caller by themselves.
func Prepare(ctx context.Context, operation Operation, clientSpec bool) (string, error) {
	object, err := decodeObject(operation.Spec)
	if err != nil {
		return "", err
	}
	_, bound := object[BindingKey]
	if bound && clientSpec {
		return "", merr.WrapErrParameterInvalidMsg("credential_binding is server-owned")
	}
	if bound {
		operation.Previous, err = Read(operation.Source, operation.Spec)
		if err != nil {
			return "", err
		}
	}
	delete(object, BindingKey)
	authorize, _ := ctx.Value(authorizerKey{}).(Authorizer)
	if authorize == nil {
		if bound {
			return "", merr.WrapErrPrivilegeNotPermitted("source authorization is required to refresh this collection")
		}
		return operation.Spec, nil
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return "", merr.WrapErrServiceInternalMsg("cannot encode external source specification")
	}
	operation.Spec = string(canonical)
	reference, err := authorize(ctx, operation)
	if err != nil {
		return "", err
	}
	if reference.Provider == "" || len(reference.Provider) > 128 ||
		reference.Value == "" || len(reference.Value) > 16*1024 ||
		strings.ContainsAny(reference.Provider, "\r\n\x00") {
		return "", merr.WrapErrServiceInternalMsg("source authorizer returned an invalid reference")
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", merr.WrapErrServiceInternalMsg("cannot allocate source authorization generation")
	}
	digest := sha256.Sum256(canonical)
	binding := Binding{
		Version: 1, Provider: reference.Provider, Reference: reference.Value,
		Generation: id.String(), Source: operation.Source,
		SpecSHA256: hex.EncodeToString(digest[:]),
	}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return "", merr.WrapErrServiceInternalMsg("cannot encode source authorization reference")
	}
	object[BindingKey] = encoded
	result, err := json.Marshal(object)
	if err != nil {
		return "", merr.WrapErrServiceInternalMsg("cannot encode authorized external specification")
	}
	return string(result), nil
}

// Read verifies that a persisted reference still names the source/spec it was
// issued for. It does not authenticate provenance: callers must obtain this spec
// from trusted coordinator state, and the provider still checks current access.
func Read(source, spec string) (*Binding, error) {
	object, err := decodeObject(spec)
	if err != nil {
		return nil, err
	}
	raw, present := object[BindingKey]
	if !present {
		return nil, nil
	}
	var binding Binding
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&binding); err != nil || binding.Version != 1 ||
		binding.Provider == "" || len(binding.Provider) > 128 ||
		binding.Reference == "" || len(binding.Reference) > 16*1024 ||
		binding.Source != source || source == "" {
		return nil, merr.WrapErrPrivilegeNotPermitted("invalid external source authorization reference")
	}
	id, err := uuid.Parse(binding.Generation)
	if err != nil || id == uuid.Nil || id.String() != binding.Generation {
		return nil, merr.WrapErrPrivilegeNotPermitted("invalid source authorization generation")
	}
	delete(object, BindingKey)
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot encode external source specification")
	}
	digest := sha256.Sum256(canonical)
	if binding.SpecSHA256 != hex.EncodeToString(digest[:]) {
		return nil, merr.WrapErrPrivilegeNotPermitted("external source specification changed after authorization")
	}
	return &binding, nil
}

// decodeObject rejects duplicate top-level keys, including alternate JSON
// spellings of credential_binding, so the public and native parsers cannot
// disagree about which authorization reference was supplied.
func decodeObject(spec string) (map[string]json.RawMessage, error) {
	if spec == "" {
		return map[string]json.RawMessage{}, nil
	}
	d := json.NewDecoder(strings.NewReader(spec))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, merr.WrapErrParameterInvalidMsg("external specification must be a JSON object")
	}
	object := map[string]json.RawMessage{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("invalid external specification")
		}
		name, ok := key.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("invalid external specification key")
		}
		// Go's typed JSON decoder matches field names case-insensitively.
		// Reject alternate casing before normalization can turn a public key
		// into the server-owned binding expected by the native reader.
		if name != BindingKey && strings.EqualFold(name, BindingKey) {
			return nil, merr.WrapErrParameterInvalidMsg("credential_binding requires its canonical spelling")
		}
		if _, present := object[name]; present {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate external specification key")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("invalid external specification value")
		}
		// Normalize nested object ordering too. The resulting spec is what is
		// persisted and passed to native readers, avoiding parser differences.
		nested := json.NewDecoder(strings.NewReader(string(value)))
		nested.UseNumber()
		var normalized any
		if err := nested.Decode(&normalized); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("invalid external specification value")
		}
		value, err = json.Marshal(normalized)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("invalid external specification value")
		}
		object[name] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid external specification")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, merr.WrapErrParameterInvalidMsg("invalid trailing external specification data")
	}
	return object, nil
}
