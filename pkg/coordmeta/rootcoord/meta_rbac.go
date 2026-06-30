package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")

	ErrRoleAlreadyExists = errors.New("role already exists")
	ErrRoleNotExists     = errors.New("role not exists")

	ErrEmptyRBACMeta           = errors.New("rbac meta is empty")
	ErrNotCustomPrivilegeGroup = errors.New("not a custom privilege group")
)

type RBACChecker interface {
	// CheckIfAddCredential checks if the credential can be added.
	// if the credential already exists, it will return ErrUserAlreadyExists.
	CheckIfAddCredential(ctx context.Context, req *internalpb.CredentialInfo) error

	// CheckIfUpdateCredential checks if the credential can be updated.
	// if the credential not exists, it will return ErrUserNotFound.
	CheckIfUpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) error

	// CheckIfDeleteCredential checks if the credential can be deleted.
	// if the credential not exists, it will return ErrUserNotFound.
	CheckIfDeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) error

	// CheckIfCreateRole checks if the role can be created.
	// if the role already exists, it will return ErrRoleAlreadyExists.
	CheckIfCreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) error

	// CheckIfAlterRole checks if the role can be altered.
	// if the role not exists, it will return ErrRoleNotExists.
	CheckIfAlterRole(ctx context.Context, req *milvuspb.AlterRoleRequest) error

	// CheckIfDropRole checks if the role can be dropped.
	// if the role not exists, it will return ErrRoleNotExists.
	CheckIfDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error

	// CheckIfOperateUserRole checks if the user role can be operated.
	CheckIfOperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) error

	// CheckIfPrivilegeGroupCreatable checks if the privilege group can be created.
	CheckIfPrivilegeGroupCreatable(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) error

	// CheckIfPrivilegeGroupAlterable checks if the privilege group can be altered.
	CheckIfPrivilegeGroupAlterable(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) error

	// CheckIfPrivilegeGroupDropable checks if the privilege group can be dropped.
	CheckIfPrivilegeGroupDropable(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) error

	// CheckIfRBACRestorable checks if the rbac meta data can be restored.
	CheckIfRBACRestorable(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) error
}
