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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func fixtureOperation() Operation {
	return Operation{Database: "default", Collection: "vectors", Source: "azure://account/container/table", Spec: `{"format":"lance-table","extfs":{"cloud_provider":"azure","region":"test"}}`}
}

func callerContext(owner string) context.Context {
	return WithAuthorizer(context.Background(), func(_ context.Context, operation Operation) (Reference, error) {
		return Reference{Provider: "test-catalog", Value: "verified-owner:" + owner}, nil
	})
}

func TestAuthorizationReferenceSurvivesPersistenceWithoutCallerCredential(t *testing.T) {
	op := fixtureOperation()
	const personalToken = "PERSONAL_TOKEN_MUST_NOT_BE_PERSISTED"
	ctx := WithAuthorizer(context.Background(), func(_ context.Context, resolved Operation) (Reference, error) {
		require.Equal(t, op.Source, resolved.Source)
		require.NotContains(t, resolved.Spec, BindingKey)
		// A deployment's hook uses this credential only for admission.
		require.NotEmpty(t, personalToken)
		return Reference{Provider: "test-catalog", Value: "verified-owner:alice/table:42"}, nil
	})
	spec, err := Prepare(ctx, op, true)
	require.NoError(t, err)
	require.NotContains(t, spec, personalToken)

	// This is the durable string passed through existing job/task/schema RPCs.
	serialized, err := json.Marshal(map[string]string{"external_spec": spec})
	require.NoError(t, err)
	var restored map[string]string
	require.NoError(t, json.Unmarshal(serialized, &restored))
	binding, err := Read(op.Source, restored["external_spec"])
	require.NoError(t, err)
	require.Equal(t, "verified-owner:alice/table:42", binding.Reference)
	require.NotContains(t, binding.String(), "alice")
}

func TestPublicClientsCannotSupplyBindings(t *testing.T) {
	for _, spec := range []string{
		`{"format":"lance-table","credential_binding":null}`,
		`{"format":"lance-table","Credential_Binding":null}`,
		`{"format":"lance-table","CREDENTIAL_BINDING":{}}`,
		`{"format":"lance-table","credential_binding":{}}`,
		`{"format":"lance-table","credential_\u0062inding":{}}`,
		`{"credential_binding":null,"credential_\u0062inding":{}}`,
	} {
		require.ErrorIs(t, RejectClientBinding(spec), merr.ErrParameterInvalid)
		op := fixtureOperation()
		op.Spec = spec
		_, err := Prepare(callerContext("alice"), op, true)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	require.NoError(t, RejectClientBinding(""))
	require.NoError(t, RejectClientBinding(fixtureOperation().Spec))
}

func TestRefreshReauthorizesTheNewCaller(t *testing.T) {
	op := fixtureOperation()
	first, err := Prepare(callerContext("alice"), op, true)
	require.NoError(t, err)
	alice, err := Read(op.Source, first)
	require.NoError(t, err)
	op.Spec = first
	_, err = Prepare(context.Background(), op, false)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	denied := WithAuthorizer(context.Background(), func(context.Context, Operation) (Reference, error) {
		return Reference{}, merr.WrapErrPrivilegeNotPermitted("source read denied")
	})
	_, err = Prepare(denied, op, false)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	second, err := Prepare(callerContext("bob"), op, false)
	require.NoError(t, err)
	bob, err := Read(op.Source, second)
	require.NoError(t, err)
	require.Equal(t, "verified-owner:bob", bob.Reference)
	require.NotEqual(t, alice.Generation, bob.Generation)
	require.Equal(t, alice.SpecSHA256, bob.SpecSHA256)
}

func TestReferenceCannotBeReusedForChangedSourceOrSpec(t *testing.T) {
	op := fixtureOperation()
	spec, err := Prepare(callerContext("alice"), op, true)
	require.NoError(t, err)
	_, err = Read(op.Source+"-other", spec)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	_, err = Read(op.Source, strings.Replace(spec, `"region":"test"`, `"region":"other"`, 1))
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestMalformedReferencesFailClosed(t *testing.T) {
	op := fixtureOperation()
	spec, err := Prepare(callerContext("alice"), op, true)
	require.NoError(t, err)
	for _, mutate := range []func(map[string]any){
		func(b map[string]any) { b["version"] = 2 },
		func(b map[string]any) { b["generation"] = "" },
		func(b map[string]any) { b["reference"] = "" },
		func(b map[string]any) { b["provider"] = "" },
		func(b map[string]any) { b["unexpected"] = "ignored-authority" },
	} {
		var object map[string]any
		require.NoError(t, json.Unmarshal([]byte(spec), &object))
		mutate(object[BindingKey].(map[string]any))
		changed, err := json.Marshal(object)
		require.NoError(t, err)
		_, err = Read(op.Source, string(changed))
		require.Error(t, err)
	}
}

func TestUnconfiguredSourcesKeepExistingBehavior(t *testing.T) {
	op := fixtureOperation()
	spec, err := Prepare(context.Background(), op, true)
	require.NoError(t, err)
	require.Equal(t, op.Spec, spec)
	binding, err := Read(op.Source, spec)
	require.NoError(t, err)
	require.Nil(t, binding)
	for _, malformed := range []string{`null`, `[]`, `{} {}`, `{"format":"lance-table","format":"iceberg-table"}`} {
		op.Spec = malformed
		_, err := Prepare(context.Background(), op, true)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
}

func TestOmittedRefreshPreservesLegacyCoordinatorResolution(t *testing.T) {
	stored := fixtureOperation()
	source, spec, err := PrepareRefresh(context.Background(), stored, "", "")
	require.NoError(t, err)
	require.Empty(t, source)
	require.Empty(t, spec)
}

func TestExplicitRefreshCannotRemoveAuthorization(t *testing.T) {
	stored := fixtureOperation()
	bound, err := Prepare(callerContext("alice"), stored, true)
	require.NoError(t, err)
	stored.Spec = bound
	_, _, err = PrepareRefresh(context.Background(), stored, stored.Source, fixtureOperation().Spec)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)

	source, spec, err := PrepareRefresh(callerContext("bob"), stored, stored.Source, fixtureOperation().Spec)
	require.NoError(t, err)
	binding, err := Read(source, spec)
	require.NoError(t, err)
	require.Equal(t, "verified-owner:bob", binding.Reference)
}

func TestOmittedRefreshPassesPreviousIdentityButReauthorizesCaller(t *testing.T) {
	stored := fixtureOperation()
	var err error
	stored.Spec, err = Prepare(callerContext("alice"), stored, true)
	require.NoError(t, err)
	ctx := WithAuthorizer(context.Background(), func(_ context.Context, operation Operation) (Reference, error) {
		require.NotNil(t, operation.Previous)
		require.Equal(t, "verified-owner:alice", operation.Previous.Reference)
		require.NotContains(t, operation.Spec, BindingKey)
		return Reference{Provider: "test-catalog", Value: "verified-owner:bob"}, nil
	})
	source, spec, err := PrepareRefresh(ctx, stored, "", "")
	require.NoError(t, err)
	binding, err := Read(source, spec)
	require.NoError(t, err)
	require.Equal(t, "verified-owner:bob", binding.Reference)
}
