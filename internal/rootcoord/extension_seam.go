package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file is the rootcoord-side seam for rbac bootstrap: it declares WHERE
// rootcoord consults the installed extension and adapts rootcoord's own
// catalog to the narrow store the capability declares. The implementations
// live outside this tree. With no provider installed the seam resolves to a
// no-op and the catalog is never touched, so a stock binary's startup is
// unchanged.

// catalogCredentialStore adapts metastore.RootCoordCatalog to
// extension.CredentialStore, the deliberately narrow slice of credential and
// privilege metadata a bootstrapper is allowed to touch.
type catalogCredentialStore struct {
	catalog metastore.RootCoordCatalog
}

// HasCredential reports whether a credential already exists for username. The
// catalog signals "not found" by returning an error that wraps
// merr.ErrIoKeyNotFound rather than a nil credential, so that case is folded
// into (false, nil); any other error is propagated unchanged.
func (s catalogCredentialStore) HasCredential(ctx context.Context, username string) (bool, error) {
	_, err := s.catalog.GetCredential(ctx, username)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CreateCredential stores an already-encrypted password for username.
func (s catalogCredentialStore) CreateCredential(ctx context.Context, username, encryptedPassword string) error {
	return s.catalog.AlterCredential(ctx, &model.Credential{
		Username:          username,
		EncryptedPassword: encryptedPassword,
	})
}

// CreateRole forwards directly to the catalog.
func (s catalogCredentialStore) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	return s.catalog.CreateRole(ctx, tenant, entity)
}

// AlterUserRole forwards directly to the catalog.
func (s catalogCredentialStore) AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType) error {
	return s.catalog.AlterUserRole(ctx, tenant, userEntity, roleEntity, op)
}

// ListUser forwards directly to the catalog.
func (s catalogCredentialStore) ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	return s.catalog.ListUser(ctx, tenant, entity, includeRoleInfo)
}

// AlterGrant forwards directly to the catalog.
func (s catalogCredentialStore) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, op milvuspb.OperatePrivilegeType) error {
	return s.catalog.AlterGrant(ctx, tenant, entity, op)
}

// bootstrapExtensionRBAC seeds provider-managed accounts and roles once during
// rootcoord initialisation, if a provider installed the rbac bootstrap
// capability. With no provider installed it returns nil without constructing
// the adapter or touching the catalog. A non-nil error must fail rootcoord
// startup: a form whose accounts are missing would otherwise serve requests
// with no identity.
func bootstrapExtensionRBAC(ctx context.Context, catalog metastore.RootCoordCatalog) error {
	bootstrapper := extension.Caps().RBACBootstrap
	if bootstrapper == nil {
		return nil
	}
	return bootstrapper.Bootstrap(ctx, catalogCredentialStore{catalog: catalog})
}
