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
	meta    IMetaTable
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

// AlterCredential stores an already-encrypted password for username. It
// writes through the catalog on purpose: MetaTable has no credential-write
// short of the RPC task pipeline, and rootcoord's own InitCredential seeds
// the root account through this same catalog call at this same point in
// initialization - the bootstrap's credentials take exactly the path the
// native root credential takes.
func (s catalogCredentialStore) AlterCredential(ctx context.Context, username, encryptedPassword string) error {
	return s.catalog.AlterCredential(ctx, &model.Credential{
		Username:          username,
		EncryptedPassword: encryptedPassword,
	})
}

// The role binding below goes through the MetaTable, not the raw catalog: its
// methods carry the validation the RPC path has and take the permission lock,
// so a bootstrapped binding is byte-for-byte what the same binding issued over
// RPC would have written.

func (s catalogCredentialStore) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType) error {
	return s.meta.OperateUserRole(ctx, tenant, userEntity, roleEntity, op)
}

func (s catalogCredentialStore) SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	return s.meta.SelectUser(ctx, tenant, entity, includeRoleInfo)
}

// bootstrapExtensionRBAC seeds provider-managed accounts once during rootcoord
// initialization, and binds them to their roles, if a provider installed the
// rbac bootstrap capability. It runs after initBuiltinRoles, because the roles
// it binds to are the ones that creates. With no provider installed it returns nil without constructing
// the adapter or touching the catalog. A non-nil error must fail rootcoord
// startup: a form whose accounts are missing would otherwise serve requests
// with no identity.
func bootstrapExtensionRBAC(ctx context.Context, meta IMetaTable, catalog metastore.RootCoordCatalog) error {
	bootstrapper := extension.Caps().RBACBootstrap
	if bootstrapper == nil {
		return nil
	}
	return bootstrapper.Bootstrap(ctx, catalogCredentialStore{catalog: catalog, meta: meta})
}
