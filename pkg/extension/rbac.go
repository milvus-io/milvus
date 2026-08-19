package extension

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// CredentialStore is the slice of milvus's credential and privilege metadata a
// bootstrapper is allowed to touch. It is deliberately narrower than milvus's
// own catalog: a distribution seeding accounts needs to create them and grant
// roles, not to read back stored credentials.
type CredentialStore interface {
	// HasCredential reports whether a credential already exists for username.
	// A missing credential is not an error.
	HasCredential(ctx context.Context, username string) (bool, error)
	// CreateCredential stores an already-encrypted password for username.
	// The caller does the encryption; milvus does not see the plaintext.
	CreateCredential(ctx context.Context, username, encryptedPassword string) error

	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType) error
	ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, op milvuspb.OperatePrivilegeType) error
}

// RBACBootstrapper seeds the accounts and roles a deployment form needs before
// it serves traffic.
//
// It runs once during rootcoord initialisation, single-threaded and before any
// request is accepted, so an implementation needs no locking of its own. It must
// be idempotent: rootcoord initializes on every restart.
type RBACBootstrapper interface {
	// Bootstrap seeds accounts and roles. A non-nil error fails rootcoord
	// startup, because a form whose accounts are missing cannot serve.
	Bootstrap(ctx context.Context, store CredentialStore) error
}
