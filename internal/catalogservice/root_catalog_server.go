package catalogservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type RootCatalogServer struct {
	catalogpb.UnimplementedRootCatalogServiceServer
	catalogs RootCoordCatalogResolver
}

func NewRootCatalogServer(catalogs RootCoordCatalogResolver) *RootCatalogServer {
	return &RootCatalogServer{catalogs: catalogs}
}

func (s *RootCatalogServer) catalog(req *catalogpb.CatalogRequestHeader) (metastore.RootCoordCatalog, error) {
	namespace := req.GetNamespace()
	if namespace == "" {
		return nil, merr.WrapErrParameterInvalidMsg("catalog namespace is required")
	}
	return s.catalogs.RootCoordCatalog(namespace)
}

func catalogOK() *catalogpb.CatalogResponseHeader {
	return &catalogpb.CatalogResponseHeader{Status: merr.Success()}
}

func catalogStatus(err error) *catalogpb.CatalogResponseHeader {
	if err == nil {
		return catalogOK()
	}
	return &catalogpb.CatalogResponseHeader{Status: merr.Status(err)}
}

func rootCatalogCollectionFromModel(coll *model.Collection) *catalogpb.RootCatalogCollection {
	if coll == nil {
		return nil
	}
	ret := &catalogpb.RootCatalogCollection{
		Collection: model.MarshalCollectionModelWithOption(
			coll,
			model.WithFields(),
			model.WithStructArrayFields(),
		),
		Partitions: make([]*etcdpb.PartitionInfo, 0, len(coll.Partitions)),
	}
	for _, partition := range coll.Partitions {
		ret.Partitions = append(ret.Partitions, model.MarshalPartitionModel(partition))
	}
	return ret
}

func rootCatalogCollectionToModel(coll *catalogpb.RootCatalogCollection) *model.Collection {
	if coll == nil {
		return nil
	}
	ret := model.UnmarshalCollectionModel(coll.GetCollection())
	if ret == nil {
		return nil
	}
	ret.Partitions = make([]*model.Partition, 0, len(coll.GetPartitions()))
	for _, partition := range coll.GetPartitions() {
		ret.Partitions = append(ret.Partitions, model.UnmarshalPartitionModel(partition))
	}
	return ret
}

func (s *RootCatalogServer) CreateDatabase(ctx context.Context, req *catalogpb.CreateDatabaseRequest) (*catalogpb.CreateDatabaseResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.CreateDatabaseResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.CreateDatabase(ctx, model.UnmarshalDatabaseModel(req.GetDatabase()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.CreateDatabaseResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DropDatabase(ctx context.Context, req *catalogpb.DropDatabaseRequest) (*catalogpb.DropDatabaseResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropDatabaseResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropDatabase(ctx, req.GetDbId(), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.DropDatabaseResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterDatabase(ctx context.Context, req *catalogpb.AlterDatabaseRequest) (*catalogpb.AlterDatabaseResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterDatabaseResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterDatabase(ctx, model.UnmarshalDatabaseModel(req.GetDatabase()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.AlterDatabaseResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListDatabases(ctx context.Context, req *catalogpb.ListDatabasesRequest) (*catalogpb.ListDatabasesResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListDatabasesResponse{Header: catalogStatus(err)}, nil
	}
	dbs, err := catalog.ListDatabases(ctx, typeutil.Timestamp(req.GetTs()))
	resp := &catalogpb.ListDatabasesResponse{Header: catalogStatus(err)}
	for _, db := range dbs {
		resp.Databases = append(resp.Databases, model.MarshalDatabaseModel(db))
	}
	return resp, nil
}

func (s *RootCatalogServer) CreateCollection(ctx context.Context, req *catalogpb.CreateCollectionRequest) (*catalogpb.CreateCollectionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.CreateCollectionResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.CreateCollection(ctx, rootCatalogCollectionToModel(req.GetCollection()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.CreateCollectionResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DropCollection(ctx context.Context, req *catalogpb.DropCollectionRequest) (*catalogpb.DropCollectionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropCollectionResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropCollection(ctx, rootCatalogCollectionToModel(req.GetCollection()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.DropCollectionResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterCollection(ctx context.Context, req *catalogpb.AlterCollectionRequest) (*catalogpb.AlterCollectionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterCollectionResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterCollection(ctx, rootCatalogCollectionToModel(req.GetOldCollection()), rootCatalogCollectionToModel(req.GetNewCollection()), metastore.AlterType(req.GetAlterType()), typeutil.Timestamp(req.GetTs()), req.GetFieldModify())
	return &catalogpb.AlterCollectionResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterCollectionDB(ctx context.Context, req *catalogpb.AlterCollectionDBRequest) (*catalogpb.AlterCollectionDBResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterCollectionDBResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterCollectionDB(ctx, rootCatalogCollectionToModel(req.GetOldCollection()), rootCatalogCollectionToModel(req.GetNewCollection()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.AlterCollectionDBResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) GetCollectionByID(ctx context.Context, req *catalogpb.GetCollectionByIDRequest) (*catalogpb.GetCollectionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.GetCollectionResponse{Header: catalogStatus(err)}, nil
	}
	coll, err := catalog.GetCollectionByID(ctx, req.GetDbId(), typeutil.Timestamp(req.GetTs()), req.GetCollectionId())
	return &catalogpb.GetCollectionResponse{Header: catalogStatus(err), Collection: rootCatalogCollectionFromModel(coll)}, nil
}

func (s *RootCatalogServer) GetCollectionByName(ctx context.Context, req *catalogpb.GetCollectionByNameRequest) (*catalogpb.GetCollectionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.GetCollectionResponse{Header: catalogStatus(err)}, nil
	}
	coll, err := catalog.GetCollectionByName(ctx, req.GetDbId(), req.GetDbName(), req.GetCollectionName(), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.GetCollectionResponse{Header: catalogStatus(err), Collection: rootCatalogCollectionFromModel(coll)}, nil
}

func (s *RootCatalogServer) ListCollections(ctx context.Context, req *catalogpb.ListCollectionsRequest) (*catalogpb.ListCollectionsResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListCollectionsResponse{Header: catalogStatus(err)}, nil
	}
	colls, err := catalog.ListCollections(ctx, req.GetDbId(), typeutil.Timestamp(req.GetTs()))
	resp := &catalogpb.ListCollectionsResponse{Header: catalogStatus(err)}
	for _, coll := range colls {
		resp.Collections = append(resp.Collections, rootCatalogCollectionFromModel(coll))
	}
	return resp, nil
}

func (s *RootCatalogServer) CollectionExists(ctx context.Context, req *catalogpb.CollectionExistsRequest) (*catalogpb.CollectionExistsResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.CollectionExistsResponse{Header: catalogStatus(err)}, nil
	}
	return &catalogpb.CollectionExistsResponse{
		Header: catalogOK(),
		Exists: catalog.CollectionExists(ctx, req.GetDbId(), req.GetCollectionId(), typeutil.Timestamp(req.GetTs())),
	}, nil
}

func (s *RootCatalogServer) UpdateRootCatalog(ctx context.Context, req *catalogpb.UpdateRootCatalogRequest) (*catalogpb.UpdateRootCatalogResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.UpdateRootCatalogResponse{Header: catalogStatus(err)}, nil
	}
	actions := make([]metastore.UpdateAction, 0, len(req.GetActions()))
	for _, action := range req.GetActions() {
		switch action.GetType() {
		case catalogpb.RootCatalogActionType_ROOT_CATALOG_ACTION_TYPE_ADD:
			actions = append(actions, metastore.UpdateAction{Type: metastore.ActionAdd, Entry: metastore.CollectionEntry{Collection: rootCatalogCollectionToModel(action.GetCollection())}})
		case catalogpb.RootCatalogActionType_ROOT_CATALOG_ACTION_TYPE_UPDATE:
			actions = append(actions, metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.CollectionEntry{Collection: rootCatalogCollectionToModel(action.GetCollection())}})
		case catalogpb.RootCatalogActionType_ROOT_CATALOG_ACTION_TYPE_DELETE:
			actions = append(actions, metastore.UpdateAction{Type: metastore.ActionDelete, Entry: metastore.CollectionEntry{Collection: rootCatalogCollectionToModel(action.GetCollection())}})
		default:
			return &catalogpb.UpdateRootCatalogResponse{Header: catalogStatus(merr.WrapErrParameterInvalidMsg("unsupported root catalog action type: %s", action.GetType().String()))}, nil
		}
	}
	err = catalog.Update(ctx, typeutil.Timestamp(req.GetTs()), actions...)
	return &catalogpb.UpdateRootCatalogResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) CreatePartition(ctx context.Context, req *catalogpb.CreatePartitionRequest) (*catalogpb.CreatePartitionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.CreatePartitionResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.CreatePartition(ctx, req.GetDbId(), model.UnmarshalPartitionModel(req.GetPartition()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.CreatePartitionResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DropPartition(ctx context.Context, req *catalogpb.DropPartitionRequest) (*catalogpb.DropPartitionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropPartitionResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropPartition(ctx, req.GetDbId(), req.GetCollectionId(), req.GetPartitionId(), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.DropPartitionResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterPartition(ctx context.Context, req *catalogpb.AlterPartitionRequest) (*catalogpb.AlterPartitionResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterPartitionResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterPartition(ctx, req.GetDbId(), model.UnmarshalPartitionModel(req.GetOldPartition()), model.UnmarshalPartitionModel(req.GetNewPartition()), metastore.AlterType(req.GetAlterType()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.AlterPartitionResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) CreateAlias(ctx context.Context, req *catalogpb.CreateAliasRequest) (*catalogpb.CreateAliasResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.CreateAliasResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.CreateAlias(ctx, model.UnmarshalAliasModel(req.GetAlias()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.CreateAliasResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DropAlias(ctx context.Context, req *catalogpb.DropAliasRequest) (*catalogpb.DropAliasResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropAliasResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropAlias(ctx, req.GetDbId(), req.GetAlias(), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.DropAliasResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterAlias(ctx context.Context, req *catalogpb.AlterAliasRequest) (*catalogpb.AlterAliasResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterAliasResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterAlias(ctx, model.UnmarshalAliasModel(req.GetAlias()), typeutil.Timestamp(req.GetTs()))
	return &catalogpb.AlterAliasResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListAliases(ctx context.Context, req *catalogpb.ListAliasesRequest) (*catalogpb.ListAliasesResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListAliasesResponse{Header: catalogStatus(err)}, nil
	}
	aliases, err := catalog.ListAliases(ctx, req.GetDbId(), typeutil.Timestamp(req.GetTs()))
	resp := &catalogpb.ListAliasesResponse{Header: catalogStatus(err)}
	for _, alias := range aliases {
		resp.Aliases = append(resp.Aliases, model.MarshalAliasModel(alias))
	}
	return resp, nil
}

func (s *RootCatalogServer) SaveFileResource(ctx context.Context, req *catalogpb.SaveFileResourceRequest) (*catalogpb.SaveFileResourceResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.SaveFileResourceResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.SaveFileResource(ctx, req.GetResource(), req.GetVersion())
	return &catalogpb.SaveFileResourceResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) RemoveFileResource(ctx context.Context, req *catalogpb.RemoveFileResourceRequest) (*catalogpb.RemoveFileResourceResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.RemoveFileResourceResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.RemoveFileResource(ctx, req.GetResourceId(), req.GetVersion())
	return &catalogpb.RemoveFileResourceResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListFileResource(ctx context.Context, req *catalogpb.ListFileResourceRequest) (*catalogpb.ListFileResourceResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListFileResourceResponse{Header: catalogStatus(err)}, nil
	}
	resources, version, err := catalog.ListFileResource(ctx)
	return &catalogpb.ListFileResourceResponse{
		Header:    catalogStatus(err),
		Resources: resources,
		Version:   version,
	}, nil
}

func (s *RootCatalogServer) GetCredential(ctx context.Context, req *catalogpb.GetCredentialRequest) (*catalogpb.GetCredentialResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.GetCredentialResponse{Header: catalogStatus(err)}, nil
	}
	credential, err := catalog.GetCredential(ctx, req.GetUsername())
	return &catalogpb.GetCredentialResponse{Header: catalogStatus(err), Credential: model.MarshalCredentialModel(credential)}, nil
}

func (s *RootCatalogServer) AlterCredential(ctx context.Context, req *catalogpb.AlterCredentialRequest) (*catalogpb.AlterCredentialResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterCredentialResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterCredential(ctx, model.UnmarshalCredentialModel(req.GetCredential()))
	return &catalogpb.AlterCredentialResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DropCredential(ctx context.Context, req *catalogpb.DropCredentialRequest) (*catalogpb.DropCredentialResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropCredentialResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropCredential(ctx, req.GetUsername())
	return &catalogpb.DropCredentialResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListCredentials(ctx context.Context, req *catalogpb.ListCredentialsRequest) (*catalogpb.ListCredentialsResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListCredentialsResponse{Header: catalogStatus(err)}, nil
	}
	usernames, err := catalog.ListCredentials(ctx)
	return &catalogpb.ListCredentialsResponse{Header: catalogStatus(err), Usernames: usernames}, nil
}

func (s *RootCatalogServer) CreateRole(ctx context.Context, req *catalogpb.CreateRoleRequest) (*catalogpb.CreateRoleResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.CreateRoleResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.CreateRole(ctx, req.GetTenant(), req.GetRole())
	return &catalogpb.CreateRoleResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterRole(ctx context.Context, req *catalogpb.AlterRoleRequest) (*catalogpb.AlterRoleResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterRoleResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterRole(ctx, req.GetTenant(), req.GetRole())
	return &catalogpb.AlterRoleResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DropRole(ctx context.Context, req *catalogpb.DropRoleRequest) (*catalogpb.DropRoleResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropRoleResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropRole(ctx, req.GetTenant(), req.GetRoleName())
	return &catalogpb.DropRoleResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) AlterUserRole(ctx context.Context, req *catalogpb.AlterUserRoleRequest) (*catalogpb.AlterUserRoleResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterUserRoleResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterUserRole(ctx, req.GetTenant(), req.GetUser(), req.GetRole(), req.GetOperateType())
	return &catalogpb.AlterUserRoleResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListRole(ctx context.Context, req *catalogpb.ListRoleRequest) (*catalogpb.ListRoleResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListRoleResponse{Header: catalogStatus(err)}, nil
	}
	roles, err := catalog.ListRole(ctx, req.GetTenant(), req.GetRole(), req.GetIncludeUserInfo())
	return &catalogpb.ListRoleResponse{Header: catalogStatus(err), Roles: roles}, nil
}

func (s *RootCatalogServer) ListUser(ctx context.Context, req *catalogpb.ListUserRequest) (*catalogpb.ListUserResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListUserResponse{Header: catalogStatus(err)}, nil
	}
	users, err := catalog.ListUser(ctx, req.GetTenant(), req.GetUser(), req.GetIncludeRoleInfo())
	return &catalogpb.ListUserResponse{Header: catalogStatus(err), Users: users}, nil
}

func (s *RootCatalogServer) AlterGrant(ctx context.Context, req *catalogpb.AlterGrantRequest) (*catalogpb.AlterGrantResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.AlterGrantResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.AlterGrant(ctx, req.GetTenant(), req.GetGrant(), req.GetOperateType())
	return &catalogpb.AlterGrantResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) DeleteGrant(ctx context.Context, req *catalogpb.DeleteGrantRequest) (*catalogpb.DeleteGrantResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DeleteGrantResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DeleteGrant(ctx, req.GetTenant(), req.GetRole())
	return &catalogpb.DeleteGrantResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListGrant(ctx context.Context, req *catalogpb.ListGrantRequest) (*catalogpb.ListGrantResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListGrantResponse{Header: catalogStatus(err)}, nil
	}
	grants, err := catalog.ListGrant(ctx, req.GetTenant(), req.GetGrant())
	return &catalogpb.ListGrantResponse{Header: catalogStatus(err), Grants: grants}, nil
}

func (s *RootCatalogServer) ListPolicy(ctx context.Context, req *catalogpb.ListPolicyRequest) (*catalogpb.ListPolicyResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListPolicyResponse{Header: catalogStatus(err)}, nil
	}
	grants, err := catalog.ListPolicy(ctx, req.GetTenant())
	return &catalogpb.ListPolicyResponse{Header: catalogStatus(err), Grants: grants}, nil
}

func (s *RootCatalogServer) ListUserRole(ctx context.Context, req *catalogpb.ListUserRoleRequest) (*catalogpb.ListUserRoleResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListUserRoleResponse{Header: catalogStatus(err)}, nil
	}
	userRoles, err := catalog.ListUserRole(ctx, req.GetTenant())
	return &catalogpb.ListUserRoleResponse{Header: catalogStatus(err), UserRoles: userRoles}, nil
}

func (s *RootCatalogServer) DeleteGrantByCollectionName(ctx context.Context, req *catalogpb.DeleteGrantByCollectionNameRequest) (*catalogpb.DeleteGrantByCollectionNameResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DeleteGrantByCollectionNameResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DeleteGrantByCollectionName(ctx, req.GetTenant(), req.GetDbName(), req.GetCollectionName())
	return &catalogpb.DeleteGrantByCollectionNameResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) MigrateGrantCollectionName(ctx context.Context, req *catalogpb.MigrateGrantCollectionNameRequest) (*catalogpb.MigrateGrantCollectionNameResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.MigrateGrantCollectionNameResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.MigrateGrantCollectionName(ctx, req.GetTenant(), req.GetOldDbName(), req.GetOldName(), req.GetNewDbName(), req.GetNewName())
	return &catalogpb.MigrateGrantCollectionNameResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) BackupRBAC(ctx context.Context, req *catalogpb.BackupRBACRequest) (*catalogpb.BackupRBACResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.BackupRBACResponse{Header: catalogStatus(err)}, nil
	}
	meta, err := catalog.BackupRBAC(ctx, req.GetTenant())
	return &catalogpb.BackupRBACResponse{Header: catalogStatus(err), RbacMeta: meta}, nil
}

func (s *RootCatalogServer) RestoreRBAC(ctx context.Context, req *catalogpb.RestoreRBACRequest) (*catalogpb.RestoreRBACResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.RestoreRBACResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.RestoreRBAC(ctx, req.GetTenant(), req.GetRbacMeta())
	return &catalogpb.RestoreRBACResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) GetPrivilegeGroup(ctx context.Context, req *catalogpb.GetPrivilegeGroupRequest) (*catalogpb.GetPrivilegeGroupResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.GetPrivilegeGroupResponse{Header: catalogStatus(err)}, nil
	}
	group, err := catalog.GetPrivilegeGroup(ctx, req.GetGroupName())
	return &catalogpb.GetPrivilegeGroupResponse{Header: catalogStatus(err), PrivilegeGroup: group}, nil
}

func (s *RootCatalogServer) DropPrivilegeGroup(ctx context.Context, req *catalogpb.DropPrivilegeGroupRequest) (*catalogpb.DropPrivilegeGroupResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.DropPrivilegeGroupResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.DropPrivilegeGroup(ctx, req.GetGroupName())
	return &catalogpb.DropPrivilegeGroupResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) SavePrivilegeGroup(ctx context.Context, req *catalogpb.SavePrivilegeGroupRequest) (*catalogpb.SavePrivilegeGroupResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.SavePrivilegeGroupResponse{Header: catalogStatus(err)}, nil
	}
	err = catalog.SavePrivilegeGroup(ctx, req.GetPrivilegeGroup())
	return &catalogpb.SavePrivilegeGroupResponse{Header: catalogStatus(err)}, nil
}

func (s *RootCatalogServer) ListPrivilegeGroups(ctx context.Context, req *catalogpb.ListPrivilegeGroupsRequest) (*catalogpb.ListPrivilegeGroupsResponse, error) {
	catalog, err := s.catalog(req.GetHeader())
	if err != nil {
		return &catalogpb.ListPrivilegeGroupsResponse{Header: catalogStatus(err)}, nil
	}
	groups, err := catalog.ListPrivilegeGroups(ctx)
	return &catalogpb.ListPrivilegeGroupsResponse{Header: catalogStatus(err), PrivilegeGroups: groups}, nil
}

var _ *internalpb.FileResourceInfo
