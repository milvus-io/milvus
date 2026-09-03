package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

	err := bootstrapExtensionRBAC(context.Background(), mockrootcoord.NewIMetaTable(t), catalog)
	assert.NoError(t, err)
	assert.Empty(t, catalog.Calls, "with no provider installed the seam must not construct the adapter or call the catalog")
}

func TestBootstrapExtensionRBACCallsBootstrapperWithAdaptedStore(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("GetCredential", mock.Anything, "someone").
		Return(&model.Credential{Username: "someone"}, nil)

	b := &recordingBootstrapper{}
	installRBACBootstrapper(t, b)

	err := bootstrapExtensionRBAC(context.Background(), mockrootcoord.NewIMetaTable(t), catalog)
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

	err := bootstrapExtensionRBAC(context.Background(), mockrootcoord.NewIMetaTable(t), catalog)
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

	err := store.AlterCredential(context.Background(), "someone", "encrypted")
	assert.NoError(t, err)
}

// The role binding goes through the MetaTable, not the raw catalog: its
// methods carry the validation the RPC path has and take the permission lock,
// so a bootstrapped binding is exactly what the same binding over RPC would
// write.
func TestCatalogCredentialStoreForwardsRemainingMethods(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	roleEntity := &milvuspb.RoleEntity{Name: "role1"}
	userEntity := &milvuspb.UserEntity{Name: "user1"}

	meta.On("OperateUserRole", mock.Anything, "tenant1", userEntity, roleEntity, milvuspb.OperateUserRoleType_AddUserToRole).Return(nil)
	meta.On("SelectUser", mock.Anything, "tenant1", userEntity, true).Return([]*milvuspb.UserResult{{User: userEntity}}, nil)

	store := catalogCredentialStore{meta: meta}
	ctx := context.Background()

	assert.NoError(t, store.OperateUserRole(ctx, "tenant1", userEntity, roleEntity, milvuspb.OperateUserRoleType_AddUserToRole))

	users, err := store.SelectUser(ctx, "tenant1", userEntity, true)
	assert.NoError(t, err)
	assert.Equal(t, []*milvuspb.UserResult{{User: userEntity}}, users)
}

// A meta implementation that is not the real MetaTable cannot store
// credentials, so with the bootstrap capability installed initRbac refuses to
// continue. The refusal must be classified: it is reported to the operator as
// a start-up failure, and a raw errors.New reaches them with no code on it.
func TestInitRbacRefusesANonMetaTableThroughMerr(t *testing.T) {
	paramtable.Init()
	installRBACBootstrapper(t, &recordingBootstrapper{})

	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))
	meta.EXPECT().CreateRole(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(len(util.DefaultRoles))

	Params.Save(Params.RoleCfg.Enabled.Key, "false")
	Params.Save(Params.CommonCfg.EnablePublicPrivilege.Key, "false")
	defer func() {
		Params.Reset(Params.RoleCfg.Enabled.Key)
		Params.Reset(Params.CommonCfg.EnablePublicPrivilege.Key)
	}()

	err := c.initRbac(context.TODO())
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

// The bootstrap runs AFTER the builtin roles, and the ordering is a contract,
// not an accident: a form declares its roles in builtinRoles.roles and binds
// its accounts to them here, so a bootstrap that ran first would bind to a
// role that does not exist yet.
func TestTheBootstrapRunsAfterTheBuiltinRoles(t *testing.T) {
	paramtable.Init()

	var order []string
	bootstrapper := &orderRecordingBootstrapper{order: &order}
	installRBACBootstrapper(t, bootstrapper)

	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))
	meta.EXPECT().CreateRole(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, entity *milvuspb.RoleEntity) error {
			order = append(order, "role:"+entity.GetName())
			return nil
		}).Maybe()

	Params.Save(Params.RoleCfg.Enabled.Key, "true")
	Params.Save(Params.RoleCfg.Roles.Key, `{"db_ro": {"privileges": []}}`)
	Params.Save(Params.CommonCfg.EnablePublicPrivilege.Key, "false")
	defer func() {
		Params.Reset(Params.RoleCfg.Enabled.Key)
		Params.Reset(Params.RoleCfg.Roles.Key)
		Params.Reset(Params.CommonCfg.EnablePublicPrivilege.Key)
	}()

	// initRbac refuses a non-MetaTable meta once it reaches the bootstrap, and
	// that refusal is exactly the point this test needs: it proves the
	// bootstrap was reached, and the recorded order says what ran before it.
	err := c.initRbac(context.TODO())
	require.Error(t, err)

	require.Contains(t, order, "role:db_ro", "the form's builtin role must be created")
	assert.Less(t, indexOfOrder(order, "role:db_ro"), indexOfOrder(order, "bootstrap"),
		"the roles a bootstrap binds to must exist before it runs")
}

type orderRecordingBootstrapper struct {
	order *[]string
}

func (b *orderRecordingBootstrapper) Bootstrap(context.Context, extension.CredentialStore) error {
	*b.order = append(*b.order, "bootstrap")
	return nil
}

func indexOfOrder(order []string, want string) int {
	for i, got := range order {
		if got == want {
			return i
		}
	}
	return len(order)
}
