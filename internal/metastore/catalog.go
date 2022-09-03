package metastore

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type RootCoordCatalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error)
	GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error)
	ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error)
	CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool
	DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType AlterType, ts typeutil.Timestamp) error

	CreatePartition(ctx context.Context, partition *model.Partition, ts typeutil.Timestamp) error
	DropPartition(ctx context.Context, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error
	AlterPartition(ctx context.Context, oldPart *model.Partition, newPart *model.Partition, alterType AlterType, ts typeutil.Timestamp) error

	CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	DropAlias(ctx context.Context, alias string, ts typeutil.Timestamp) error
	AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error)

	GetCredential(ctx context.Context, username string) (*model.Credential, error)
	CreateCredential(ctx context.Context, credential *model.Credential) error
	AlterCredential(ctx context.Context, credential *model.Credential) error
	DropCredential(ctx context.Context, username string) error
	ListCredentials(ctx context.Context) ([]string, error)

	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	DropRole(ctx context.Context, tenant string, roleName string) error
	AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error
	ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error)
	ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error
	DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error
	ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error)
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

func (t AlterType) String() string {
	switch t {
	case ADD:
		return "ADD"
	case DELETE:
		return "DELETE"
	case MODIFY:
		return "MODIFY"
	}
	return ""
}

type DataCoordCatalog interface {
	ListSegments(ctx context.Context) ([]*datapb.SegmentInfo, error)
	AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error
	AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo) error
	// AlterSegmentsAndAddNewSegment for transaction
	AlterSegmentsAndAddNewSegment(ctx context.Context, segments []*datapb.SegmentInfo, newSegment *datapb.SegmentInfo) error
	SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error
	DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error
	MarkChannelDeleted(ctx context.Context, channel string) error
	IsChannelDropped(ctx context.Context, channel string) bool
	DropChannel(ctx context.Context, channel string) error
}

type IndexCoordCatalog interface {
	CreateIndex(ctx context.Context, index *model.Index) error
	ListIndexes(ctx context.Context) ([]*model.Index, error)
	AlterIndex(ctx context.Context, newIndex *model.Index) error
	AlterIndexes(ctx context.Context, newIndexes []*model.Index) error
	DropIndex(ctx context.Context, collID, dropIdxID typeutil.UniqueID) error

	CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error
	ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error)
	AlterSegmentIndex(ctx context.Context, newSegIndex *model.SegmentIndex) error
	AlterSegmentIndexes(ctx context.Context, newSegIdxes []*model.SegmentIndex) error
	DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error
}
