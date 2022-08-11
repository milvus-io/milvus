package metastore

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error)
	GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error)
	ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error)
	CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool
	DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error

	CreatePartition(ctx context.Context, partition *model.Partition, ts typeutil.Timestamp) error
	DropPartition(ctx context.Context, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error

	CreateIndex(ctx context.Context, index *model.Index) error
	CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error
	AlterIndex(ctx context.Context, indexes []*model.Index) error
	AlterSegmentIndex(ctx context.Context, segIdxes []*model.SegmentIndex) error
	ListIndexes(ctx context.Context) ([]*model.Index, error)
	ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error)
	DropIndex(ctx context.Context, collID typeutil.UniqueID, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error
	DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error

	CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	DropAlias(ctx context.Context, alias string, ts typeutil.Timestamp) error
	AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error)

	GetCredential(ctx context.Context, username string) (*model.Credential, error)
	CreateCredential(ctx context.Context, credential *model.Credential) error
	DropCredential(ctx context.Context, username string) error
	ListCredentials(ctx context.Context) ([]string, error)

	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	DropRole(ctx context.Context, tenant string, roleName string) error
	OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error
	SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error)
	SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error
	SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error)
	ListPolicy(ctx context.Context, tenant string) ([]string, error)
	ListUserRole(ctx context.Context, tenant string) ([]string, error)

	Close()
}

type AlterType int32

const (
	ADD AlterType = iota
	DELETE
	MODIFY
)
