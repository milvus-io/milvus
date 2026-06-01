package catalog

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
)

type AccessControlCatalog interface {
	Credentials() CredentialCatalog
	Roles() RoleCatalog
	Grants() GrantCatalog
	PrivilegeGroups() PrivilegeGroupCatalog
}

type CredentialCatalog interface {
	Get(ctx context.Context, username string, opts ReadOptions) (*model.Credential, error)
	Alter(ctx context.Context, credential *model.Credential, opts WriteOptions) error
	Drop(ctx context.Context, username string, opts WriteOptions) error
	List(ctx context.Context, opts ReadOptions) ([]string, error)
	ListWithPasswords(ctx context.Context, opts ReadOptions) (map[string]string, error)
}

type RoleCatalog interface {
	Create(ctx context.Context, tenant string, role *milvuspb.RoleEntity, opts WriteOptions) error
	Drop(ctx context.Context, tenant string, roleName string, opts WriteOptions) error
	List(ctx context.Context, tenant string, role *milvuspb.RoleEntity, includeUserInfo bool, opts ReadOptions) ([]*milvuspb.RoleResult, error)
	AlterUserRole(ctx context.Context, tenant string, user *milvuspb.UserEntity, role *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType, opts WriteOptions) error
	ListUser(ctx context.Context, tenant string, user *milvuspb.UserEntity, includeRoleInfo bool, opts ReadOptions) ([]*milvuspb.UserResult, error)
	ListUserRole(ctx context.Context, tenant string, opts ReadOptions) ([]string, error)
}

type GrantCatalog interface {
	Alter(ctx context.Context, tenant string, grant *milvuspb.GrantEntity, op milvuspb.OperatePrivilegeType, opts WriteOptions) error
	DeleteByRole(ctx context.Context, tenant string, role *milvuspb.RoleEntity, opts WriteOptions) error
	List(ctx context.Context, tenant string, grant *milvuspb.GrantEntity, opts ReadOptions) ([]*milvuspb.GrantEntity, error)
	ListPolicy(ctx context.Context, tenant string, opts ReadOptions) ([]*milvuspb.GrantEntity, error)
	DeleteByCollectionName(ctx context.Context, tenant string, dbName string, collectionName string, opts WriteOptions) error
	MigrateCollectionName(ctx context.Context, tenant string, oldDBName string, oldName string, newDBName string, newName string, opts WriteOptions) error
	Backup(ctx context.Context, tenant string, opts ReadOptions) (*milvuspb.RBACMeta, error)
	Restore(ctx context.Context, tenant string, meta *milvuspb.RBACMeta, opts WriteOptions) error
}

type PrivilegeGroupCatalog interface {
	Get(ctx context.Context, groupName string, opts ReadOptions) (*milvuspb.PrivilegeGroupInfo, error)
	Drop(ctx context.Context, groupName string, opts WriteOptions) error
	Save(ctx context.Context, group *milvuspb.PrivilegeGroupInfo, opts WriteOptions) error
	List(ctx context.Context, opts ReadOptions) ([]*milvuspb.PrivilegeGroupInfo, error)
}
