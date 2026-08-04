package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type Catalog struct {
	client    catalogpb.RootCatalogServiceClient
	namespace string
}

func NewCatalog(client catalogpb.RootCatalogServiceClient, namespace string) metastore.RootCoordCatalog {
	return &Catalog{client: client, namespace: namespace}
}

func (c *Catalog) header() *catalogpb.CatalogRequestHeader {
	return &catalogpb.CatalogRequestHeader{Namespace: c.namespace}
}

func checkHeader(header *catalogpb.CatalogResponseHeader) error {
	if header == nil {
		return merr.WrapErrServiceInternalMsg("missing catalog response header")
	}
	return merr.Error(header.GetStatus())
}

func unsupported(method string) error {
	return merr.WrapErrServiceInternalMsg("catalogservice rootcoord catalog does not support %s yet", method)
}

func collectionFromModel(coll *model.Collection) *catalogpb.RootCatalogCollection {
	if coll == nil {
		return nil
	}
	resp := &catalogpb.RootCatalogCollection{
		Collection: model.MarshalCollectionModelWithOption(
			coll,
			model.WithFields(),
			model.WithStructArrayFields(),
			model.WithFunctions(),
		),
		Partitions: make([]*etcdpb.PartitionInfo, 0, len(coll.Partitions)),
	}
	for _, partition := range coll.Partitions {
		resp.Partitions = append(resp.Partitions, model.MarshalPartitionModel(partition))
	}
	return resp
}

func collectionToModel(coll *catalogpb.RootCatalogCollection) *model.Collection {
	if coll == nil {
		return nil
	}
	resp := model.UnmarshalCollectionModel(coll.GetCollection())
	if resp == nil {
		return nil
	}
	resp.Partitions = make([]*model.Partition, 0, len(coll.GetPartitions()))
	for _, partition := range coll.GetPartitions() {
		resp.Partitions = append(resp.Partitions, model.UnmarshalPartitionModel(partition))
	}
	return resp
}

func (c *Catalog) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	resp, err := c.client.CreateDatabase(ctx, &catalogpb.CreateDatabaseRequest{
		Header:   c.header(),
		Database: model.MarshalDatabaseModel(db),
		Ts:       uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error {
	resp, err := c.client.DropDatabase(ctx, &catalogpb.DropDatabaseRequest{
		Header: c.header(),
		DbId:   dbID,
		Ts:     uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	resp, err := c.client.ListDatabases(ctx, &catalogpb.ListDatabasesRequest{
		Header: c.header(),
		Ts:     uint64(ts),
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	dbs := make([]*model.Database, 0, len(resp.GetDatabases()))
	for _, db := range resp.GetDatabases() {
		dbs = append(dbs, model.UnmarshalDatabaseModel(db))
	}
	return dbs, nil
}

func (c *Catalog) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	resp, err := c.client.AlterDatabase(ctx, &catalogpb.AlterDatabaseRequest{
		Header:   c.header(),
		Database: model.MarshalDatabaseModel(newDB),
		Ts:       uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	resp, err := c.client.CreateCollection(ctx, &catalogpb.CreateCollectionRequest{
		Header:     c.header(),
		Collection: collectionFromModel(collectionInfo),
		Ts:         uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error) {
	resp, err := c.client.GetCollectionByID(ctx, &catalogpb.GetCollectionByIDRequest{
		Header:       c.header(),
		DbId:         dbID,
		Ts:           uint64(ts),
		CollectionId: collectionID,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return collectionToModel(resp.GetCollection()), nil
}

func (c *Catalog) GetCollectionByName(ctx context.Context, dbID int64, dbName string, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	resp, err := c.client.GetCollectionByName(ctx, &catalogpb.GetCollectionByNameRequest{
		Header:         c.header(),
		DbId:           dbID,
		DbName:         dbName,
		CollectionName: collectionName,
		Ts:             uint64(ts),
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return collectionToModel(resp.GetCollection()), nil
}

func (c *Catalog) ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error) {
	resp, err := c.client.ListCollections(ctx, &catalogpb.ListCollectionsRequest{
		Header: c.header(),
		DbId:   dbID,
		Ts:     uint64(ts),
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	collections := make([]*model.Collection, 0, len(resp.GetCollections()))
	for _, coll := range resp.GetCollections() {
		collections = append(collections, collectionToModel(coll))
	}
	return collections, nil
}

func (c *Catalog) CollectionExists(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	resp, err := c.client.CollectionExists(ctx, &catalogpb.CollectionExistsRequest{
		Header:       c.header(),
		DbId:         dbID,
		CollectionId: collectionID,
		Ts:           uint64(ts),
	})
	return err == nil && merr.Ok(resp.GetHeader().GetStatus()) && resp.GetExists()
}

func (c *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	resp, err := c.client.DropCollection(ctx, &catalogpb.DropCollectionRequest{
		Header:     c.header(),
		Collection: collectionFromModel(collectionInfo),
		Ts:         uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp, fieldModify bool) error {
	resp, err := c.client.AlterCollection(ctx, &catalogpb.AlterCollectionRequest{
		Header:        c.header(),
		OldCollection: collectionFromModel(oldColl),
		NewCollection: collectionFromModel(newColl),
		AlterType:     int32(alterType),
		Ts:            uint64(ts),
		FieldModify:   fieldModify,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error {
	resp, err := c.client.AlterCollectionDB(ctx, &catalogpb.AlterCollectionDBRequest{
		Header:        c.header(),
		OldCollection: collectionFromModel(oldColl),
		NewCollection: collectionFromModel(newColl),
		Ts:            uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) Update(ctx context.Context, ts typeutil.Timestamp, actions ...metastore.UpdateAction) error {
	req := &catalogpb.UpdateRootCatalogRequest{Header: c.header(), Ts: uint64(ts)}
	for _, action := range actions {
		protoAction := &catalogpb.RootCatalogUpdateAction{}
		switch action.Type {
		case metastore.ActionAdd:
			protoAction.Type = catalogpb.RootCatalogActionType_ROOT_CATALOG_ACTION_TYPE_ADD
		case metastore.ActionUpdate:
			protoAction.Type = catalogpb.RootCatalogActionType_ROOT_CATALOG_ACTION_TYPE_UPDATE
		case metastore.ActionDelete:
			protoAction.Type = catalogpb.RootCatalogActionType_ROOT_CATALOG_ACTION_TYPE_DELETE
		default:
			return unsupported("Update action type")
		}
		entry, ok := action.Entry.(metastore.CollectionEntry)
		if !ok {
			return unsupported("Update non-collection entry")
		}
		protoAction.Entry = &catalogpb.RootCatalogUpdateAction_Collection{Collection: collectionFromModel(entry.Collection)}
		req.Actions = append(req.Actions, protoAction)
	}
	resp, err := c.client.UpdateRootCatalog(ctx, req)
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error {
	resp, err := c.client.CreatePartition(ctx, &catalogpb.CreatePartitionRequest{
		Header:    c.header(),
		DbId:      dbID,
		Partition: model.MarshalPartitionModel(partition),
		Ts:        uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	resp, err := c.client.DropPartition(ctx, &catalogpb.DropPartitionRequest{
		Header:       c.header(),
		DbId:         dbID,
		CollectionId: collectionID,
		PartitionId:  partitionID,
		Ts:           uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	resp, err := c.client.AlterPartition(ctx, &catalogpb.AlterPartitionRequest{
		Header:       c.header(),
		DbId:         dbID,
		OldPartition: model.MarshalPartitionModel(oldPart),
		NewPartition: model.MarshalPartitionModel(newPart),
		AlterType:    int32(alterType),
		Ts:           uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	resp, err := c.client.CreateAlias(ctx, &catalogpb.CreateAliasRequest{
		Header: c.header(),
		Alias:  model.MarshalAliasModel(alias),
		Ts:     uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error {
	resp, err := c.client.DropAlias(ctx, &catalogpb.DropAliasRequest{
		Header: c.header(),
		DbId:   dbID,
		Alias:  alias,
		Ts:     uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	resp, err := c.client.AlterAlias(ctx, &catalogpb.AlterAliasRequest{
		Header: c.header(),
		Alias:  model.MarshalAliasModel(alias),
		Ts:     uint64(ts),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error) {
	resp, err := c.client.ListAliases(ctx, &catalogpb.ListAliasesRequest{
		Header: c.header(),
		DbId:   dbID,
		Ts:     uint64(ts),
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	aliases := make([]*model.Alias, 0, len(resp.GetAliases()))
	for _, alias := range resp.GetAliases() {
		aliases = append(aliases, model.UnmarshalAliasModel(alias))
	}
	return aliases, nil
}

func (c *Catalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	resp, err := c.client.SaveFileResource(ctx, &catalogpb.SaveFileResourceRequest{
		Header:   c.header(),
		Resource: resource,
		Version:  version,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	resp, err := c.client.RemoveFileResource(ctx, &catalogpb.RemoveFileResourceRequest{
		Header:     c.header(),
		ResourceId: resourceID,
		Version:    version,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	resp, err := c.client.ListFileResource(ctx, &catalogpb.ListFileResourceRequest{Header: c.header()})
	if err != nil {
		return nil, 0, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, 0, err
	}
	return resp.GetResources(), resp.GetVersion(), nil
}

func (c *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	resp, err := c.client.GetCredential(ctx, &catalogpb.GetCredentialRequest{
		Header:   c.header(),
		Username: username,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return model.UnmarshalCredentialModel(resp.GetCredential()), nil
}

func (c *Catalog) AlterCredential(ctx context.Context, credential *model.Credential) error {
	resp, err := c.client.AlterCredential(ctx, &catalogpb.AlterCredentialRequest{
		Header:     c.header(),
		Credential: model.MarshalCredentialModel(credential),
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) DropCredential(ctx context.Context, username string) error {
	resp, err := c.client.DropCredential(ctx, &catalogpb.DropCredentialRequest{
		Header:   c.header(),
		Username: username,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListCredentials(ctx context.Context) ([]string, error) {
	resp, err := c.client.ListCredentials(ctx, &catalogpb.ListCredentialsRequest{Header: c.header()})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetUsernames(), nil
}

func (c *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	resp, err := c.client.CreateRole(ctx, &catalogpb.CreateRoleRequest{
		Header: c.header(),
		Tenant: tenant,
		Role:   entity,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	resp, err := c.client.AlterRole(ctx, &catalogpb.AlterRoleRequest{
		Header: c.header(),
		Tenant: tenant,
		Role:   entity,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	resp, err := c.client.DropRole(ctx, &catalogpb.DropRoleRequest{
		Header:   c.header(),
		Tenant:   tenant,
		RoleName: roleName,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	resp, err := c.client.AlterUserRole(ctx, &catalogpb.AlterUserRoleRequest{
		Header:      c.header(),
		Tenant:      tenant,
		User:        userEntity,
		Role:        roleEntity,
		OperateType: operateType,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	resp, err := c.client.ListRole(ctx, &catalogpb.ListRoleRequest{
		Header:          c.header(),
		Tenant:          tenant,
		Role:            entity,
		IncludeUserInfo: includeUserInfo,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetRoles(), nil
}

func (c *Catalog) ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	resp, err := c.client.ListUser(ctx, &catalogpb.ListUserRequest{
		Header:          c.header(),
		Tenant:          tenant,
		User:            entity,
		IncludeRoleInfo: includeRoleInfo,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetUsers(), nil
}

func (c *Catalog) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	resp, err := c.client.AlterGrant(ctx, &catalogpb.AlterGrantRequest{
		Header:      c.header(),
		Tenant:      tenant,
		Grant:       entity,
		OperateType: operateType,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	resp, err := c.client.DeleteGrant(ctx, &catalogpb.DeleteGrantRequest{
		Header: c.header(),
		Tenant: tenant,
		Role:   role,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	resp, err := c.client.ListGrant(ctx, &catalogpb.ListGrantRequest{
		Header: c.header(),
		Tenant: tenant,
		Grant:  entity,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetGrants(), nil
}

func (c *Catalog) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	resp, err := c.client.ListPolicy(ctx, &catalogpb.ListPolicyRequest{
		Header: c.header(),
		Tenant: tenant,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetGrants(), nil
}

func (c *Catalog) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	resp, err := c.client.ListUserRole(ctx, &catalogpb.ListUserRoleRequest{
		Header: c.header(),
		Tenant: tenant,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetUserRoles(), nil
}

func (c *Catalog) DeleteGrantByCollectionName(ctx context.Context, tenant string, dbName string, collectionName string) error {
	resp, err := c.client.DeleteGrantByCollectionName(ctx, &catalogpb.DeleteGrantByCollectionNameRequest{
		Header:         c.header(),
		Tenant:         tenant,
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) MigrateGrantCollectionName(ctx context.Context, tenant string, oldDBName string, oldName string, newDBName string, newName string) error {
	resp, err := c.client.MigrateGrantCollectionName(ctx, &catalogpb.MigrateGrantCollectionNameRequest{
		Header:    c.header(),
		Tenant:    tenant,
		OldDbName: oldDBName,
		OldName:   oldName,
		NewDbName: newDBName,
		NewName:   newName,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	resp, err := c.client.BackupRBAC(ctx, &catalogpb.BackupRBACRequest{
		Header: c.header(),
		Tenant: tenant,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetRbacMeta(), nil
}

func (c *Catalog) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	resp, err := c.client.RestoreRBAC(ctx, &catalogpb.RestoreRBACRequest{
		Header:   c.header(),
		Tenant:   tenant,
		RbacMeta: meta,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) GetPrivilegeGroup(ctx context.Context, groupName string) (*milvuspb.PrivilegeGroupInfo, error) {
	resp, err := c.client.GetPrivilegeGroup(ctx, &catalogpb.GetPrivilegeGroupRequest{
		Header:    c.header(),
		GroupName: groupName,
	})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetPrivilegeGroup(), nil
}

func (c *Catalog) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	resp, err := c.client.DropPrivilegeGroup(ctx, &catalogpb.DropPrivilegeGroupRequest{
		Header:    c.header(),
		GroupName: groupName,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) SavePrivilegeGroup(ctx context.Context, data *milvuspb.PrivilegeGroupInfo) error {
	resp, err := c.client.SavePrivilegeGroup(ctx, &catalogpb.SavePrivilegeGroupRequest{
		Header:         c.header(),
		PrivilegeGroup: data,
	})
	if err != nil {
		return err
	}
	return checkHeader(resp.GetHeader())
}

func (c *Catalog) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	resp, err := c.client.ListPrivilegeGroups(ctx, &catalogpb.ListPrivilegeGroupsRequest{Header: c.header()})
	if err != nil {
		return nil, err
	}
	if err := checkHeader(resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetPrivilegeGroups(), nil
}

func (c *Catalog) Close() {}

var _ metastore.RootCoordCatalog = (*Catalog)(nil)
