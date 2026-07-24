package metastore

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

//go:generate mockery --name=RootCoordCatalog
type RootCoordCatalog interface {
	CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error
	DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error
	ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error)
	AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error

	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error)
	GetCollectionByName(ctx context.Context, dbID int64, dbName string, collectionName string, ts typeutil.Timestamp) (*model.Collection, error)
	ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error)
	CollectionExists(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool
	DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType AlterType, ts typeutil.Timestamp, fieldModify bool) error
	AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error

	CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error
	DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error
	AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType AlterType, ts typeutil.Timestamp) error

	CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error
	AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error)

	// GetCredential gets the credential info for the username, returns error if no credential exists for this username.
	GetCredential(ctx context.Context, username string) (*model.Credential, error)
	// AlterCredential does exactly the same as CreateCredential
	AlterCredential(ctx context.Context, credential *model.Credential) error
	// DropCredential removes the credential of this username
	DropCredential(ctx context.Context, username string) error
	// ListCredentials gets all usernames.
	ListCredentials(ctx context.Context) ([]string, error)

	// CreateRole creates role by the entity for the tenant. Please make sure the tenent and entity.Name aren't empty. Empty entity.Name may end up with deleting all roles
	// Returns common.IgnorableError if the role already existes
	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	// AlterRole updates mutable role metadata by role name.
	AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	// DropRole removes a role by name
	DropRole(ctx context.Context, tenant string, roleName string) error
	// AlterUserRole changes the role of a user for the tenant. Please make sure the userEntity.Name and roleEntity.Name aren't empty before calling this API.
	// Returns common.IgnorableError
	// - if user has the role when AddUserToRole
	// - if user doen't have the role when RemoveUserFromRole
	AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error
	// ListRole returns lists of RoleResults for the tenant
	// Returns all role results if entity is nill
	// Returns only role results if entity.Name is provided
	// Returns UserInfo inside each RoleResult if includeUserInfo is True
	ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error)
	// ListUser returns list of UserResults for the tenant
	// Returns all users if entity is nill
	// Returns the specific user if enitity is provided
	// Returns RoleInfo inside each UserResult if includeRoleInfo is True
	ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	// AlterGrant  grants or revokes a grant of a role to an object, according to the operateType.
	// Please make sure entity and operateType are valid before calling this API
	AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error
	// DeleteGrant deletes all the grant for a role.
	// Please make sure the role.Name isn't empty before call this API.
	DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error
	// ListGrant lists all grant infos accoording to entity for the tenant
	// Please make sure entity valid before calling this API
	ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error)
	ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error)
	// List all user role pair in string for the tenant
	// For example []string{"user1/role1"}
	ListUserRole(ctx context.Context, tenant string) ([]string, error)

	// DeleteGrantByCollectionName deletes all grants for a specific collection.
	DeleteGrantByCollectionName(ctx context.Context, tenant string, dbName string, collectionName string) error
	// MigrateGrantCollectionName migrates all grants from oldName to newName when a collection is renamed.
	MigrateGrantCollectionName(ctx context.Context, tenant string, oldDBName string, oldName string, newDBName string, newName string) error

	BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error)
	RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error

	GetPrivilegeGroup(ctx context.Context, groupName string) (*milvuspb.PrivilegeGroupInfo, error)
	DropPrivilegeGroup(ctx context.Context, groupName string) error
	SavePrivilegeGroup(ctx context.Context, data *milvuspb.PrivilegeGroupInfo) error
	ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error)

	// File resource related
	SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error
	RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error
	ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error)

	// Compound DDL operations
	//
	// Each method below collapses a multi-step catalog write sequence, which
	// callers previously issued as separate calls inside one critical section,
	// into a single catalog call. The etcd-based implementation replays the
	// original stepwise sequence with identical ordering and error semantics;
	// an atomic (remote) implementation may execute the whole compound
	// operation in a single round-trip.

	// AlterCollectionAndDeleteGrants alters the collection (MODIFY, no field
	// modification) and then deletes all grants referencing it. It returns an
	// error if and only if the collection write fails; the grant deletion is
	// best-effort in non-atomic implementations (failure is logged and
	// swallowed, leftover grants are swept later) while in atomic
	// implementations it takes effect atomically with the collection write.
	AlterCollectionAndDeleteGrants(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp, tenant string, dbName string, collectionName string) error
	// DropCollectionAndDeleteGrants drops the collection and then deletes all
	// grants referencing it. It returns an error if and only if the collection
	// drop fails; the grant deletion is best-effort in non-atomic
	// implementations while in atomic implementations it takes effect
	// atomically with the collection drop.
	DropCollectionAndDeleteGrants(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp, tenant string, dbName string, collectionName string) error
	// AlterCollectionAndMigrateGrants alters the collection (in-place MODIFY
	// when dbChanged is false, cross-database move otherwise) and, when the
	// collection name or database name changes, migrates all grants from the
	// old name to the new one. It returns an error if and only if the
	// collection write fails; the grant migration is best-effort in non-atomic
	// implementations while in atomic implementations it takes effect
	// atomically with the collection write.
	AlterCollectionAndMigrateGrants(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp, fieldModify bool, dbChanged bool, tenant string) error

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
