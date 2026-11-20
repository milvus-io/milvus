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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testSession struct {
	owner   string
	account string
	closed  bool
}

func (s *testSession) AccountName() string       { return s.account }
func (s *testSession) AllowedRootURIs() []string { return []string{"az://container/table"} }
func (*testSession) Fetch(context.Context, FetchRequest) Credential {
	return Credential{Status: CredentialDenied}
}
func (s *testSession) Close() { s.closed = true }

func TestProviderMissingOrFailingNeverFallsBack(t *testing.T) {
	op := fixtureOperation()
	op.Spec, _ = Prepare(callerContext("alice"), op, true)
	var registry Registry
	session, err := registry.Open(context.Background(), op.Source, op.Spec)
	require.Nil(t, session)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.NoError(t, registry.Register("test-catalog", func(context.Context, Binding) (Session, error) {
		return nil, merr.WrapErrPrivilegeNotPermitted("READ revoked")
	}))
	session, err = registry.Open(context.Background(), op.Source, op.Spec)
	require.Nil(t, session)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	_, err = registry.Open(context.Background(), op.Source+"-other", op.Spec)
	require.Error(t, err)
}

func TestRegistryCreatesFreshSessionsFromEachTrustedBinding(t *testing.T) {
	op := fixtureOperation()
	alice, err := Prepare(callerContext("alice"), op, true)
	require.NoError(t, err)
	bob, err := Prepare(callerContext("bob"), op, true)
	require.NoError(t, err)
	var registry Registry
	factory := func(_ context.Context, b Binding) (Session, error) {
		return &testSession{owner: b.Reference, account: "account"}, nil
	}
	require.NoError(t, registry.Register("test-catalog", factory))
	require.Error(t, registry.Register("test-catalog", factory))
	a, err := registry.Open(context.Background(), op.Source, alice)
	require.NoError(t, err)
	b, err := registry.Open(context.Background(), op.Source, bob)
	require.NoError(t, err)
	a2, err := registry.Open(context.Background(), op.Source, alice)
	require.NoError(t, err)
	require.NotSame(t, a, b)
	require.NotSame(t, a, a2)
	require.Equal(t, "verified-owner:alice", a.(*testSession).owner)
	require.Equal(t, "verified-owner:bob", b.(*testSession).owner)
	a.Close()
	b.Close()
	a2.Close()
}

func TestUnscopedSessionIsClosedAndRejected(t *testing.T) {
	op := fixtureOperation()
	op.Spec, _ = Prepare(callerContext("alice"), op, true)
	var registry Registry
	invalid := &testSession{}
	require.NoError(t, registry.Register("test-catalog", func(context.Context, Binding) (Session, error) {
		return invalid, nil
	}))
	session, err := registry.Open(context.Background(), op.Source, op.Spec)
	require.Nil(t, session)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.True(t, invalid.closed)
}

func TestCredentialDiagnosticsAreRedacted(t *testing.T) {
	credential := Credential{SASToken: "do-not-log-this-token"}
	encoded, err := json.Marshal(credential)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), credential.SASToken)
	require.NotContains(t, fmt.Sprintf("%v %+v %#v", credential, credential, credential), credential.SASToken)
}
