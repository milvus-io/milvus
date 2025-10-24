package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

var (
	errEmptyUsername     = errors.New("username is empty")
	errUserNotFound      = errors.New("user not found")
	errUserAlreadyExists = errors.New("user already exists")

	errEmptyRoleName     = errors.New("role name is empty")
	errRoleAlreadyExists = errors.New("role already exists")
	errRoleNotExists     = errors.New("role not exists")

	errEmptyRBACMeta           = errors.New("rbac meta is empty")
	errNotCustomPrivilegeGroup = errors.New("not a custom privilege group")

	errEmptyPrivilegeGroupName = errors.New("privilege group name is empty")
)

type RBACChecker interface {
	// CheckIfAddCredential checks if the credential can be added.
	// if the credential already exists, it will return errUserAlreadyExists.
	CheckIfAddCredential(ctx context.Context, req *internalpb.CredentialInfo) error

	// CheckIfUpdateCredential checks if the credential can be updated.
	// if the credential not exists, it will return errUserNotFound.
	CheckIfUpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) error

	// CheckIfDeleteCredential checks if the credential can be deleted.
	// if the credential not exists, it will return errUserNotFound.
	CheckIfDeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) error

	// CheckIfCreateRole checks if the role can be created.
	// if the role already exists, it will return errRoleAlreadyExists.
	CheckIfCreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) error

	// CheckIfDropRole checks if the role can be dropped.
	// if the role not exists, it will return errRoleNotExists.
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
