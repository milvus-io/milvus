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

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// fakeCredentialStore is a no-op CredentialStore used only to prove that the
// store handed to Bootstrap is the same instance the seam passed in.
type fakeCredentialStore struct{}

func (fakeCredentialStore) HasCredential(context.Context, string) (bool, error)   { return false, nil }
func (fakeCredentialStore) AlterCredential(context.Context, string, string) error { return nil }
func (fakeCredentialStore) CreateRole(context.Context, string, *milvuspb.RoleEntity) error {
	return nil
}

func (fakeCredentialStore) OperateUserRole(context.Context, string, *milvuspb.UserEntity, *milvuspb.RoleEntity, milvuspb.OperateUserRoleType) error {
	return nil
}

func (fakeCredentialStore) SelectUser(context.Context, string, *milvuspb.UserEntity, bool) ([]*milvuspb.UserResult, error) {
	return nil, nil
}

func (fakeCredentialStore) OperatePrivilege(context.Context, string, *milvuspb.GrantEntity, milvuspb.OperatePrivilegeType) error {
	return nil
}

// fakeBootstrapper records the store it was called with and returns a
// preconfigured error.
type fakeBootstrapper struct {
	err    error
	seen   CredentialStore
	called bool
}

func (f *fakeBootstrapper) Bootstrap(ctx context.Context, store CredentialStore) error {
	f.called = true
	f.seen = store
	return f.err
}

func TestCapabilitiesReportsRBACBootstrapPresence(t *testing.T) {
	assert.False(t, Capabilities{}.has(CapRBACBootstrap),
		"an empty table must not claim to supply the rbac bootstrap capability")
	assert.True(t, Capabilities{RBACBootstrap: &fakeBootstrapper{}}.has(CapRBACBootstrap))
}

func TestSetProviderRejectsMissingRBACBootstrapCapability(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapRBACBootstrap},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapRBACBootstrap))
}

func TestInstalledRBACBootstrapperIsReachableThroughCaps(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	b := &fakeBootstrapper{}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{RBACBootstrap: b}}))

	got := Caps().RBACBootstrap
	assert.NotNil(t, got)

	store := fakeCredentialStore{}
	assert.NoError(t, got.Bootstrap(context.Background(), store))
	assert.True(t, b.called)
	assert.Equal(t, store, b.seen, "the store passed to Bootstrap must be the one the seam handed over")
}

func TestBootstrapErrorIsPropagated(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	want := errors.New("seeding failed")
	b := &fakeBootstrapper{err: want}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{RBACBootstrap: b}}))

	err := Caps().RBACBootstrap.Bootstrap(context.Background(), fakeCredentialStore{})
	assert.ErrorIs(t, err, want, "an error from Bootstrap must survive install, Caps, and the call unwrapped and unreplaced")
}

// NoopRBACBootstrapper seeds nothing and succeeds, so a form that embeds it
// and has no accounts to seed does not fail rootcoord's start-up.
func TestNoopRBACBootstrapperSeedsNothing(t *testing.T) {
	type embedder struct{ NoopRBACBootstrapper }
	var b RBACBootstrapper = embedder{}
	assert.NoError(t, b.Bootstrap(context.Background(), fakeCredentialStore{}))
}
