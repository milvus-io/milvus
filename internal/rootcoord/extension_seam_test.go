package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// fakeRBACProvider installs a preset RBACBootstrap capability and nothing
// else, mirroring the shape used by internal/proxy/extension_seam_test.go.
type fakeRBACProvider struct {
	bootstrap extension.RBACBootstrapper
}

func (fakeRBACProvider) Name() string                       { return "test" }
func (fakeRBACProvider) Requires() []extension.CapabilityID { return nil }
func (p fakeRBACProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{RBACBootstrap: p.bootstrap}
}

// recordingBootstrapper records how many times Bootstrap ran and the store it
// was given, and returns a preconfigured error.
type recordingBootstrapper struct {
	err       error
	callCount int
	seenStore extension.CredentialStore
}

func (b *recordingBootstrapper) Bootstrap(ctx context.Context, store extension.CredentialStore) error {
	b.callCount++
	b.seenStore = store
	return b.err
}

func installRBACBootstrapper(t *testing.T, b extension.RBACBootstrapper) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, extension.SetProvider(fakeRBACProvider{bootstrap: b}))
}

func TestBootstrapExtensionRBACNoProviderDoesNotTouchCatalog(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	catalog := mocks.NewRootCoordCatalog(t)

	err := bootstrapExtensionRBAC(context.Background(), catalog)
	assert.NoError(t, err)
	assert.Empty(t, catalog.Calls, "with no provider installed the seam must not construct the adapter or call the catalog")
}

func TestBootstrapExtensionRBACCallsBootstrapperWithAdaptedStore(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("GetCredential", mock.Anything, "someone").
		Return(&model.Credential{Username: "someone"}, nil)

	b := &recordingBootstrapper{}
	installRBACBootstrapper(t, b)

	err := bootstrapExtensionRBAC(context.Background(), catalog)
	assert.NoError(t, err)
	assert.Equal(t, 1, b.callCount, "Bootstrap must run exactly once")

	// Bootstrap looks up a credential through the store it was given, proving
	// the store handed to it is wired all the way through to the real catalog
	// rather than a disconnected stub.
	ok, err := b.seenStore.HasCredential(context.Background(), "someone")
	assert.NoError(t, err)
	assert.True(t, ok, "the store passed to Bootstrap must forward to the underlying catalog")
}

func TestBootstrapExtensionRBACPropagatesBootstrapError(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	want := errors.New("seeding failed")
	installRBACBootstrapper(t, &recordingBootstrapper{err: want})

	err := bootstrapExtensionRBAC(context.Background(), catalog)
	assert.ErrorIs(t, err, want, "a Bootstrap error must propagate out of the seam so rootcoord startup fails")
}

func TestCatalogCredentialStoreHasCredentialReturnsFalseWhenNotFound(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("GetCredential", mock.Anything, "missing").
		Return(nil, merr.WrapErrIoKeyNotFound("missing"))
	store := catalogCredentialStore{catalog: catalog}

	ok, err := store.HasCredential(context.Background(), "missing")
	assert.NoError(t, err, "a not-found credential must not be reported as an error")
	assert.False(t, ok)
}

func TestCatalogCredentialStoreHasCredentialPropagatesOtherErrors(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	want := errors.New("store is unavailable")
	catalog.On("GetCredential", mock.Anything, "someone").
		Return(nil, want)
	store := catalogCredentialStore{catalog: catalog}

	ok, err := store.HasCredential(context.Background(), "someone")
	assert.ErrorIs(t, err, want, "a genuine catalog failure must not be swallowed as not-found")
	assert.False(t, ok)
}

func TestCatalogCredentialStoreHasCredentialReturnsTrueWhenCredentialExists(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("GetCredential", mock.Anything, "someone").
		Return(&model.Credential{Username: "someone"}, nil)
	store := catalogCredentialStore{catalog: catalog}

	ok, err := store.HasCredential(context.Background(), "someone")
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestCatalogCredentialStoreCreateCredentialForwardsToAlterCredential(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("AlterCredential", mock.Anything, &model.Credential{
		Username:          "someone",
		EncryptedPassword: "encrypted",
	}).Return(nil)
	store := catalogCredentialStore{catalog: catalog}

	err := store.CreateCredential(context.Background(), "someone", "encrypted")
	assert.NoError(t, err)
}

func TestCatalogCredentialStoreForwardsRemainingMethods(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	roleEntity := &milvuspb.RoleEntity{Name: "role1"}
	userEntity := &milvuspb.UserEntity{Name: "user1"}
	grantEntity := &milvuspb.GrantEntity{Role: roleEntity}

	catalog.On("CreateRole", mock.Anything, "tenant1", roleEntity).Return(nil)
	catalog.On("AlterUserRole", mock.Anything, "tenant1", userEntity, roleEntity, milvuspb.OperateUserRoleType_AddUserToRole).Return(nil)
	catalog.On("ListUser", mock.Anything, "tenant1", userEntity, true).Return([]*milvuspb.UserResult{{User: userEntity}}, nil)
	catalog.On("AlterGrant", mock.Anything, "tenant1", grantEntity, milvuspb.OperatePrivilegeType_Grant).Return(nil)

	store := catalogCredentialStore{catalog: catalog}
	ctx := context.Background()

	assert.NoError(t, store.CreateRole(ctx, "tenant1", roleEntity))
	assert.NoError(t, store.AlterUserRole(ctx, "tenant1", userEntity, roleEntity, milvuspb.OperateUserRoleType_AddUserToRole))

	users, err := store.ListUser(ctx, "tenant1", userEntity, true)
	assert.NoError(t, err)
	assert.Equal(t, []*milvuspb.UserResult{{User: userEntity}}, users)

	assert.NoError(t, store.AlterGrant(ctx, "tenant1", grantEntity, milvuspb.OperatePrivilegeType_Grant))
}
