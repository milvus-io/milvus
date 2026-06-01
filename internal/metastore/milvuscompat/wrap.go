package milvuscompat

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/catalog"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func Wrap(c Catalogs) Catalog {
	return wrappedCatalog{catalogs: c}
}

type wrappedCatalog struct{ catalogs Catalogs }

var (
	_ catalog.Catalog = wrappedCatalog{}
	_ Catalog         = wrappedCatalog{}
)

func (w wrappedCatalog) Metadata() catalog.MetadataCatalog {
	return wrappedMetadata(w)
}

func (w wrappedCatalog) MetadataInternal() MetadataCatalog {
	return wrappedMetadata(w)
}

func (w wrappedCatalog) AccessControl() catalog.AccessControlCatalog {
	return wrappedAccessControl{root: w.catalogs.RootCoord}
}

func (w wrappedCatalog) InternalState() MilvusStateCatalog {
	return wrappedState(w)
}

func (w wrappedCatalog) Migration() catalog.MigrationCatalog {
	return unsupportedMigration{}
}

func (w wrappedCatalog) Close(ctx context.Context) error {
	_ = ctx
	if w.catalogs.RootCoord != nil {
		w.catalogs.RootCoord.Close()
	}
	return nil
}

func readTS(opts catalog.ReadOptions) uint64 {
	return opts.At
}

func writeTS(opts catalog.WriteOptions) uint64 {
	return opts.Timestamp
}

func missing(name string) error {
	return fmt.Errorf("%w: %s catalog is not configured", catalog.ErrNotWired, name)
}

func unsupported(name string) error {
	return fmt.Errorf("%w: %s is not available in Milvus compatibility catalogs", catalog.ErrUnsupportedImplementation, name)
}

type wrappedMetadata struct{ catalogs Catalogs }

func (w wrappedMetadata) Databases() catalog.DatabaseCatalog {
	return wrappedDatabases{root: w.catalogs.RootCoord}
}

func (w wrappedMetadata) Collections() catalog.CollectionCatalog {
	return wrappedCollections{root: w.catalogs.RootCoord}
}

func (w wrappedMetadata) Partitions() catalog.PartitionCatalog {
	return wrappedPartitions{root: w.catalogs.RootCoord}
}

func (w wrappedMetadata) Aliases() catalog.AliasCatalog {
	return wrappedAliases{root: w.catalogs.RootCoord}
}

func (w wrappedMetadata) Segments() SegmentCatalog {
	return wrappedSegments{data: w.catalogs.DataCoord}
}

func (w wrappedMetadata) Indexes() catalog.IndexCatalog {
	return wrappedIndexes{data: w.catalogs.DataCoord}
}

func (w wrappedMetadata) Files() FileResourceCatalog {
	return wrappedFiles{root: w.catalogs.RootCoord, data: w.catalogs.DataCoord}
}

func (w wrappedMetadata) Snapshots() SnapshotCatalog {
	return wrappedSnapshots{data: w.catalogs.DataCoord}
}

type wrappedDatabases struct{ root metastore.RootCoordCatalog }

func (w wrappedDatabases) Create(ctx context.Context, db *model.Database, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.CreateDatabase(ctx, db, writeTS(opts))
}

func (w wrappedDatabases) Get(ctx context.Context, ref catalog.DatabaseRef, opts catalog.ReadOptions) (*model.Database, error) {
	dbs, err := w.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if ref.ID != 0 && db.ID == ref.ID {
			return db, nil
		}
		if ref.Name != "" && db.Name == ref.Name {
			return db, nil
		}
	}
	return nil, catalog.ErrNotFound
}

func (w wrappedDatabases) List(ctx context.Context, opts catalog.ReadOptions) ([]*model.Database, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListDatabases(ctx, readTS(opts))
}

func (w wrappedDatabases) Alter(ctx context.Context, db *model.Database, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.AlterDatabase(ctx, db, writeTS(opts))
}

func (w wrappedDatabases) Drop(ctx context.Context, ref catalog.DatabaseRef, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropDatabase(ctx, ref.ID, writeTS(opts))
}

type wrappedCollections struct{ root metastore.RootCoordCatalog }

func (w wrappedCollections) Create(ctx context.Context, collection *model.Collection, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.CreateCollection(ctx, collection, writeTS(opts))
}

func (w wrappedCollections) Get(ctx context.Context, ref catalog.CollectionRef, opts catalog.ReadOptions) (*model.Collection, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	if ref.ID != 0 {
		return w.root.GetCollectionByID(ctx, ref.Database.ID, readTS(opts), ref.ID)
	}
	return w.root.GetCollectionByName(ctx, ref.Database.ID, ref.Database.Name, ref.Name, readTS(opts))
}

func (w wrappedCollections) List(ctx context.Context, db catalog.DatabaseRef, opts catalog.ReadOptions) ([]*model.Collection, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListCollections(ctx, db.ID, readTS(opts))
}

func (w wrappedCollections) Exists(ctx context.Context, ref catalog.CollectionRef, opts catalog.ReadOptions) (bool, error) {
	if w.root == nil {
		return false, missing("rootcoord")
	}
	exists := w.root.CollectionExists(ctx, ref.Database.ID, ref.ID, readTS(opts))
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return exists, nil
}

func (w wrappedCollections) Alter(ctx context.Context, req catalog.AlterCollectionRequest, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	if req.Old != nil && req.New != nil && req.Old.DBID != req.New.DBID {
		return w.root.AlterCollectionDB(ctx, req.Old, req.New, writeTS(opts))
	}
	return w.root.AlterCollection(ctx, req.Old, req.New, metastore.AlterType(req.AlterType), writeTS(opts), req.FieldModify)
}

func (w wrappedCollections) Drop(ctx context.Context, collection *model.Collection, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropCollection(ctx, collection, writeTS(opts))
}

type wrappedPartitions struct{ root metastore.RootCoordCatalog }

func (w wrappedPartitions) Create(ctx context.Context, db catalog.DatabaseRef, partition *model.Partition, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.CreatePartition(ctx, db.ID, partition, writeTS(opts))
}

func (w wrappedPartitions) List(ctx context.Context, collection catalog.CollectionRef, opts catalog.ReadOptions) ([]*model.Partition, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	coll, err := w.root.GetCollectionByID(ctx, collection.Database.ID, readTS(opts), collection.ID)
	if err != nil {
		return nil, err
	}
	return coll.Partitions, nil
}

func (w wrappedPartitions) Alter(ctx context.Context, req catalog.AlterPartitionRequest, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.AlterPartition(ctx, req.DatabaseID, req.Old, req.New, metastore.AlterType(req.AlterType), writeTS(opts))
}

func (w wrappedPartitions) Drop(ctx context.Context, ref catalog.PartitionRef, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropPartition(ctx, ref.Collection.Database.ID, ref.Collection.ID, ref.ID, writeTS(opts))
}

type wrappedAliases struct{ root metastore.RootCoordCatalog }

func (w wrappedAliases) Create(ctx context.Context, alias *model.Alias, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.CreateAlias(ctx, alias, writeTS(opts))
}

func (w wrappedAliases) List(ctx context.Context, db catalog.DatabaseRef, opts catalog.ReadOptions) ([]*model.Alias, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListAliases(ctx, db.ID, readTS(opts))
}

func (w wrappedAliases) Alter(ctx context.Context, alias *model.Alias, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.AlterAlias(ctx, alias, writeTS(opts))
}

func (w wrappedAliases) Drop(ctx context.Context, ref catalog.AliasRef, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropAlias(ctx, ref.Database.ID, ref.Name, writeTS(opts))
}

type wrappedSegments struct{ data metastore.DataCoordCatalog }

func (w wrappedSegments) Save(ctx context.Context, segment *datapb.SegmentInfo, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.AddSegment(ctx, segment)
}

func (w wrappedSegments) Get(ctx context.Context, ref catalog.SegmentRef, opts catalog.ReadOptions) (*datapb.SegmentInfo, error) {
	segments, err := w.List(ctx, ListSegmentsRequest{CollectionID: ref.CollectionID, SegmentIDs: []int64{ref.SegmentID}}, opts)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, catalog.ErrNotFound
	}
	return segments[0], nil
}

func (w wrappedSegments) List(ctx context.Context, req ListSegmentsRequest, opts catalog.ReadOptions) ([]*datapb.SegmentInfo, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	segments, err := w.data.ListSegments(ctx, req.CollectionID)
	if err != nil {
		return segments, err
	}
	if req.PartitionID == 0 && len(req.SegmentIDs) == 0 {
		return segments, nil
	}
	var ids map[int64]struct{}
	if len(req.SegmentIDs) > 0 {
		ids = make(map[int64]struct{}, len(req.SegmentIDs))
		for _, id := range req.SegmentIDs {
			ids[id] = struct{}{}
		}
	}
	filtered := make([]*datapb.SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		if req.PartitionID != 0 && segment.GetPartitionID() != req.PartitionID {
			continue
		}
		if ids != nil {
			if _, ok := ids[segment.GetID()]; !ok {
				continue
			}
		}
		filtered = append(filtered, segment)
	}
	return filtered, nil
}

func (w wrappedSegments) UpdateBatch(ctx context.Context, req UpdateSegmentsRequest, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.AlterSegments(ctx, req.Segments, req.Binlogs...)
}

func (w wrappedSegments) MarkDropped(ctx context.Context, segments []*datapb.SegmentInfo, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveDroppedSegmentsInBatch(ctx, segments)
}

func (w wrappedSegments) Drop(ctx context.Context, segment *datapb.SegmentInfo, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropSegment(ctx, segment)
}

type wrappedIndexes struct{ data metastore.DataCoordCatalog }

func (w wrappedIndexes) CreateIndex(ctx context.Context, index *model.Index, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.CreateIndex(ctx, index)
}

func (w wrappedIndexes) ListIndexes(ctx context.Context, req catalog.ListIndexesRequest, opts catalog.ReadOptions) ([]*model.Index, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	indexes, err := w.data.ListIndexes(ctx)
	if err != nil {
		return nil, err
	}
	if req.CollectionID == 0 && len(req.IndexIDs) == 0 {
		return indexes, nil
	}
	ids := make(map[int64]struct{}, len(req.IndexIDs))
	for _, id := range req.IndexIDs {
		ids[id] = struct{}{}
	}
	filtered := make([]*model.Index, 0, len(indexes))
	for _, index := range indexes {
		if req.CollectionID != 0 && index.CollectionID != req.CollectionID {
			continue
		}
		if len(ids) != 0 {
			if _, ok := ids[index.IndexID]; !ok {
				continue
			}
		}
		filtered = append(filtered, index)
	}
	return filtered, nil
}

func (w wrappedIndexes) AlterIndexes(ctx context.Context, indexes []*model.Index, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.AlterIndexes(ctx, indexes)
}

func (w wrappedIndexes) DropIndex(ctx context.Context, ref catalog.IndexRef, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropIndex(ctx, ref.CollectionID, ref.IndexID)
}

func (w wrappedIndexes) SaveSegmentIndex(ctx context.Context, index *model.SegmentIndex, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.CreateSegmentIndex(ctx, index)
}

func (w wrappedIndexes) ListSegmentIndexes(ctx context.Context, req catalog.ListSegmentIndexesRequest, opts catalog.ReadOptions) ([]*model.SegmentIndex, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	indexes, err := w.data.ListSegmentIndexes(ctx, req.CollectionID)
	if err != nil || req.SegmentID == 0 {
		return indexes, err
	}
	filtered := make([]*model.SegmentIndex, 0, len(indexes))
	for _, index := range indexes {
		if index.SegmentID == req.SegmentID {
			filtered = append(filtered, index)
		}
	}
	return filtered, nil
}

func (w wrappedIndexes) AlterSegmentIndexes(ctx context.Context, indexes []*model.SegmentIndex, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.AlterSegmentIndexes(ctx, indexes)
}

func (w wrappedIndexes) DropSegmentIndex(ctx context.Context, ref catalog.SegmentIndexRef, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropSegmentIndex(ctx, ref.CollectionID, ref.PartitionID, ref.SegmentID, ref.BuildID)
}

type wrappedFiles struct {
	root metastore.RootCoordCatalog
	data metastore.DataCoordCatalog
}

func (w wrappedFiles) Save(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64, opts catalog.WriteOptions) error {
	if w.root != nil {
		return w.root.SaveFileResource(ctx, resource, version)
	}
	if w.data != nil {
		return w.data.SaveFileResource(ctx, resource, version)
	}
	return missing("file resource")
}

func (w wrappedFiles) Remove(ctx context.Context, resourceID int64, version uint64, opts catalog.WriteOptions) error {
	if w.root != nil {
		return w.root.RemoveFileResource(ctx, resourceID, version)
	}
	if w.data != nil {
		return w.data.RemoveFileResource(ctx, resourceID, version)
	}
	return missing("file resource")
}

func (w wrappedFiles) List(ctx context.Context, opts catalog.ReadOptions) ([]*internalpb.FileResourceInfo, uint64, error) {
	if w.root != nil {
		return w.root.ListFileResource(ctx)
	}
	if w.data != nil {
		return w.data.ListFileResource(ctx)
	}
	return nil, 0, missing("file resource")
}

type wrappedSnapshots struct{ data metastore.DataCoordCatalog }

func (w wrappedSnapshots) Get(ctx context.Context, req GetSnapshotRequest, opts catalog.ReadOptions) (*Snapshot, error) {
	return nil, unsupported("snapshot get")
}

func (w wrappedSnapshots) ListManifests(ctx context.Context, req ListManifestsRequest, opts catalog.ReadOptions) ([]*Manifest, error) {
	return nil, unsupported("snapshot manifests")
}

func (w wrappedSnapshots) Save(ctx context.Context, snapshot *datapb.SnapshotInfo, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveSnapshot(ctx, snapshot)
}

func (w wrappedSnapshots) Drop(ctx context.Context, collectionID int64, snapshotID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropSnapshot(ctx, collectionID, snapshotID)
}

func (w wrappedSnapshots) List(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.SnapshotInfo, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListSnapshots(ctx)
}

type wrappedAccessControl struct{ root metastore.RootCoordCatalog }

func (w wrappedAccessControl) Credentials() catalog.CredentialCatalog {
	return wrappedCredentials(w)
}

func (w wrappedAccessControl) Roles() catalog.RoleCatalog {
	return wrappedRoles(w)
}

func (w wrappedAccessControl) Grants() catalog.GrantCatalog {
	return wrappedGrants(w)
}

func (w wrappedAccessControl) PrivilegeGroups() catalog.PrivilegeGroupCatalog {
	return wrappedPrivilegeGroups(w)
}

type wrappedCredentials struct{ root metastore.RootCoordCatalog }

func (w wrappedCredentials) Get(ctx context.Context, username string, opts catalog.ReadOptions) (*model.Credential, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.GetCredential(ctx, username)
}

func (w wrappedCredentials) Alter(ctx context.Context, credential *model.Credential, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.AlterCredential(ctx, credential)
}

func (w wrappedCredentials) Drop(ctx context.Context, username string, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropCredential(ctx, username)
}

func (w wrappedCredentials) List(ctx context.Context, opts catalog.ReadOptions) ([]string, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListCredentials(ctx)
}

func (w wrappedCredentials) ListWithPasswords(ctx context.Context, opts catalog.ReadOptions) (map[string]string, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListCredentialsWithPasswd(ctx)
}

type wrappedRoles struct{ root metastore.RootCoordCatalog }

func (w wrappedRoles) Create(ctx context.Context, tenant string, role *milvuspb.RoleEntity, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.CreateRole(ctx, tenant, role)
}

func (w wrappedRoles) Drop(ctx context.Context, tenant string, roleName string, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropRole(ctx, tenant, roleName)
}

func (w wrappedRoles) List(ctx context.Context, tenant string, role *milvuspb.RoleEntity, includeUserInfo bool, opts catalog.ReadOptions) ([]*milvuspb.RoleResult, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListRole(ctx, tenant, role, includeUserInfo)
}

func (w wrappedRoles) AlterUserRole(ctx context.Context, tenant string, user *milvuspb.UserEntity, role *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.AlterUserRole(ctx, tenant, user, role, op)
}

func (w wrappedRoles) ListUser(ctx context.Context, tenant string, user *milvuspb.UserEntity, includeRoleInfo bool, opts catalog.ReadOptions) ([]*milvuspb.UserResult, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListUser(ctx, tenant, user, includeRoleInfo)
}

func (w wrappedRoles) ListUserRole(ctx context.Context, tenant string, opts catalog.ReadOptions) ([]string, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListUserRole(ctx, tenant)
}

type wrappedGrants struct{ root metastore.RootCoordCatalog }

func (w wrappedGrants) Alter(ctx context.Context, tenant string, grant *milvuspb.GrantEntity, op milvuspb.OperatePrivilegeType, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.AlterGrant(ctx, tenant, grant, op)
}

func (w wrappedGrants) DeleteByRole(ctx context.Context, tenant string, role *milvuspb.RoleEntity, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DeleteGrant(ctx, tenant, role)
}

func (w wrappedGrants) List(ctx context.Context, tenant string, grant *milvuspb.GrantEntity, opts catalog.ReadOptions) ([]*milvuspb.GrantEntity, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListGrant(ctx, tenant, grant)
}

func (w wrappedGrants) ListPolicy(ctx context.Context, tenant string, opts catalog.ReadOptions) ([]*milvuspb.GrantEntity, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListPolicy(ctx, tenant)
}

func (w wrappedGrants) DeleteByCollectionName(ctx context.Context, tenant string, dbName string, collectionName string, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DeleteGrantByCollectionName(ctx, tenant, dbName, collectionName)
}

func (w wrappedGrants) MigrateCollectionName(ctx context.Context, tenant string, oldDBName string, oldName string, newDBName string, newName string, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.MigrateGrantCollectionName(ctx, tenant, oldDBName, oldName, newDBName, newName)
}

func (w wrappedGrants) Backup(ctx context.Context, tenant string, opts catalog.ReadOptions) (*milvuspb.RBACMeta, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.BackupRBAC(ctx, tenant)
}

func (w wrappedGrants) Restore(ctx context.Context, tenant string, meta *milvuspb.RBACMeta, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.RestoreRBAC(ctx, tenant, meta)
}

type wrappedPrivilegeGroups struct{ root metastore.RootCoordCatalog }

func (w wrappedPrivilegeGroups) Get(ctx context.Context, groupName string, opts catalog.ReadOptions) (*milvuspb.PrivilegeGroupInfo, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.GetPrivilegeGroup(ctx, groupName)
}

func (w wrappedPrivilegeGroups) Drop(ctx context.Context, groupName string, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.DropPrivilegeGroup(ctx, groupName)
}

func (w wrappedPrivilegeGroups) Save(ctx context.Context, group *milvuspb.PrivilegeGroupInfo, opts catalog.WriteOptions) error {
	if w.root == nil {
		return missing("rootcoord")
	}
	return w.root.SavePrivilegeGroup(ctx, group)
}

func (w wrappedPrivilegeGroups) List(ctx context.Context, opts catalog.ReadOptions) ([]*milvuspb.PrivilegeGroupInfo, error) {
	if w.root == nil {
		return nil, missing("rootcoord")
	}
	return w.root.ListPrivilegeGroups(ctx)
}

type wrappedState struct{ catalogs Catalogs }

func (w wrappedState) DataCoord() DataCoordStateCatalog {
	return wrappedDataState{data: w.catalogs.DataCoord}
}

func (w wrappedState) QueryCoord() QueryCoordStateCatalog {
	return wrappedQueryState{query: w.catalogs.QueryCoord}
}

func (w wrappedState) Streaming() StreamingStateCatalog {
	return wrappedStreamingState{streamingCoord: w.catalogs.StreamingCoord, streamingNode: w.catalogs.StreamingNode}
}

type wrappedDataState struct{ data metastore.DataCoordCatalog }

func (w wrappedDataState) MarkChannelAdded(ctx context.Context, channel string, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.MarkChannelAdded(ctx, channel)
}

func (w wrappedDataState) MarkChannelDeleted(ctx context.Context, channel string, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.MarkChannelDeleted(ctx, channel)
}

func (w wrappedDataState) ShouldDropChannel(ctx context.Context, channel string, opts catalog.ReadOptions) (bool, error) {
	if w.data == nil {
		return false, missing("datacoord")
	}
	result := w.data.ShouldDropChannel(ctx, channel)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return result, nil
}

func (w wrappedDataState) ChannelExists(ctx context.Context, channel string, opts catalog.ReadOptions) (bool, error) {
	if w.data == nil {
		return false, missing("datacoord")
	}
	result := w.data.ChannelExists(ctx, channel)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return result, nil
}

func (w wrappedDataState) DropChannel(ctx context.Context, channel string, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropChannel(ctx, channel)
}

func (w wrappedDataState) SaveImportJob(ctx context.Context, job *datapb.ImportJob, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveImportJob(ctx, job)
}

func (w wrappedDataState) ListImportJobs(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ImportJob, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListImportJobs(ctx)
}

func (w wrappedDataState) DropImportJob(ctx context.Context, jobID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropImportJob(ctx, jobID)
}

func (w wrappedDataState) SavePreImportTask(ctx context.Context, task *datapb.PreImportTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SavePreImportTask(ctx, task)
}

func (w wrappedDataState) ListPreImportTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.PreImportTask, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListPreImportTasks(ctx)
}

func (w wrappedDataState) DropPreImportTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropPreImportTask(ctx, taskID)
}

func (w wrappedDataState) SaveImportTask(ctx context.Context, task *datapb.ImportTaskV2, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveImportTask(ctx, task)
}

func (w wrappedDataState) ListImportTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ImportTaskV2, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListImportTasks(ctx)
}

func (w wrappedDataState) DropImportTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropImportTask(ctx, taskID)
}

func (w wrappedDataState) SaveCopySegmentJob(ctx context.Context, job *datapb.CopySegmentJob, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveCopySegmentJob(ctx, job)
}

func (w wrappedDataState) ListCopySegmentJobs(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.CopySegmentJob, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListCopySegmentJobs(ctx)
}

func (w wrappedDataState) DropCopySegmentJob(ctx context.Context, jobID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropCopySegmentJob(ctx, jobID)
}

func (w wrappedDataState) SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveCopySegmentTask(ctx, task)
}

func (w wrappedDataState) SaveCopySegmentTasksBatch(ctx context.Context, tasks []*datapb.CopySegmentTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveCopySegmentTasksBatch(ctx, tasks)
}

func (w wrappedDataState) ListCopySegmentTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.CopySegmentTask, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListCopySegmentTasks(ctx)
}

func (w wrappedDataState) DropCopySegmentTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropCopySegmentTask(ctx, taskID)
}

func (w wrappedDataState) GcConfirm(ctx context.Context, collectionID int64, partitionID int64, opts catalog.ReadOptions) (bool, error) {
	if w.data == nil {
		return false, missing("datacoord")
	}
	result := w.data.GcConfirm(ctx, collectionID, partitionID)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return result, nil
}

func (w wrappedDataState) SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveCompactionTask(ctx, task)
}

func (w wrappedDataState) ListCompactionTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.CompactionTask, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListCompactionTask(ctx)
}

func (w wrappedDataState) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropCompactionTask(ctx, task)
}

func (w wrappedDataState) SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveAnalyzeTask(ctx, task)
}

func (w wrappedDataState) ListAnalyzeTasks(ctx context.Context, opts catalog.ReadOptions) ([]*indexpb.AnalyzeTask, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListAnalyzeTasks(ctx)
}

func (w wrappedDataState) DropAnalyzeTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropAnalyzeTask(ctx, taskID)
}

func (w wrappedDataState) SaveStatsTask(ctx context.Context, task *indexpb.StatsTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveStatsTask(ctx, task)
}

func (w wrappedDataState) ListStatsTasks(ctx context.Context, opts catalog.ReadOptions) ([]*indexpb.StatsTask, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListStatsTasks(ctx)
}

func (w wrappedDataState) DropStatsTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropStatsTask(ctx, taskID)
}

func (w wrappedDataState) SaveChannelCheckpoint(ctx context.Context, vchannel string, position *msgpb.MsgPosition, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveChannelCheckpoint(ctx, vchannel, position)
}

func (w wrappedDataState) SaveChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveChannelCheckpoints(ctx, positions)
}

func (w wrappedDataState) ListChannelCheckpoints(ctx context.Context, opts catalog.ReadOptions) (map[string]*msgpb.MsgPosition, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListChannelCheckpoint(ctx)
}

func (w wrappedDataState) DropChannelCheckpoint(ctx context.Context, vchannel string, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropChannelCheckpoint(ctx, vchannel)
}

func (w wrappedDataState) ListPartitionStatsInfos(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.PartitionStatsInfo, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListPartitionStatsInfos(ctx)
}

func (w wrappedDataState) SavePartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SavePartitionStatsInfo(ctx, info)
}

func (w wrappedDataState) DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropPartitionStatsInfo(ctx, info)
}

func (w wrappedDataState) SaveCurrentPartitionStatsVersion(ctx context.Context, collID int64, partID int64, vchannel string, currentVersion int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveCurrentPartitionStatsVersion(ctx, collID, partID, vchannel, currentVersion)
}

func (w wrappedDataState) GetCurrentPartitionStatsVersion(ctx context.Context, collID int64, partID int64, vchannel string, opts catalog.ReadOptions) (int64, error) {
	if w.data == nil {
		return 0, missing("datacoord")
	}
	return w.data.GetCurrentPartitionStatsVersion(ctx, collID, partID, vchannel)
}

func (w wrappedDataState) DropCurrentPartitionStatsVersion(ctx context.Context, collID int64, partID int64, vchannel string, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropCurrentPartitionStatsVersion(ctx, collID, partID, vchannel)
}

func (w wrappedDataState) ListExternalCollectionRefreshJobs(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ExternalCollectionRefreshJob, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListExternalCollectionRefreshJobs(ctx)
}

func (w wrappedDataState) SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveExternalCollectionRefreshJob(ctx, job)
}

func (w wrappedDataState) DropExternalCollectionRefreshJob(ctx context.Context, jobID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropExternalCollectionRefreshJob(ctx, jobID)
}

func (w wrappedDataState) ListExternalCollectionRefreshTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ExternalCollectionRefreshTask, error) {
	if w.data == nil {
		return nil, missing("datacoord")
	}
	return w.data.ListExternalCollectionRefreshTasks(ctx)
}

func (w wrappedDataState) SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.SaveExternalCollectionRefreshTask(ctx, task)
}

func (w wrappedDataState) DropExternalCollectionRefreshTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error {
	if w.data == nil {
		return missing("datacoord")
	}
	return w.data.DropExternalCollectionRefreshTask(ctx, taskID)
}

type wrappedQueryState struct{ query metastore.QueryCoordCatalog }

func (w wrappedQueryState) SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions []*querypb.PartitionLoadInfo, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.SaveCollection(ctx, collection, partitions...)
}

func (w wrappedQueryState) SavePartition(ctx context.Context, partitions []*querypb.PartitionLoadInfo, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.SavePartition(ctx, partitions...)
}

func (w wrappedQueryState) SaveReplica(ctx context.Context, replicas []*querypb.Replica, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.SaveReplica(ctx, replicas...)
}

func (w wrappedQueryState) GetCollections(ctx context.Context, opts catalog.ReadOptions) ([]*querypb.CollectionLoadInfo, error) {
	if w.query == nil {
		return nil, missing("querycoord")
	}
	return w.query.GetCollections(ctx)
}

func (w wrappedQueryState) GetPartitions(ctx context.Context, collectionIDs []int64, opts catalog.ReadOptions) (map[int64][]*querypb.PartitionLoadInfo, error) {
	if w.query == nil {
		return nil, missing("querycoord")
	}
	return w.query.GetPartitions(ctx, collectionIDs)
}

func (w wrappedQueryState) GetReplicas(ctx context.Context, opts catalog.ReadOptions) ([]*querypb.Replica, error) {
	if w.query == nil {
		return nil, missing("querycoord")
	}
	return w.query.GetReplicas(ctx)
}

func (w wrappedQueryState) ReleaseCollection(ctx context.Context, collectionID int64, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.ReleaseCollection(ctx, collectionID)
}

func (w wrappedQueryState) ReleasePartition(ctx context.Context, collectionID int64, partitionIDs []int64, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.ReleasePartition(ctx, collectionID, partitionIDs...)
}

func (w wrappedQueryState) ReleaseReplicas(ctx context.Context, collectionID int64, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.ReleaseReplicas(ctx, collectionID)
}

func (w wrappedQueryState) ReleaseReplica(ctx context.Context, collectionID int64, replicaIDs []int64, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.ReleaseReplica(ctx, collectionID, replicaIDs...)
}

func (w wrappedQueryState) SaveResourceGroup(ctx context.Context, groups []*querypb.ResourceGroup, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.SaveResourceGroup(ctx, groups...)
}

func (w wrappedQueryState) RemoveResourceGroup(ctx context.Context, rgName string, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.RemoveResourceGroup(ctx, rgName)
}

func (w wrappedQueryState) GetResourceGroups(ctx context.Context, opts catalog.ReadOptions) ([]*querypb.ResourceGroup, error) {
	if w.query == nil {
		return nil, missing("querycoord")
	}
	return w.query.GetResourceGroups(ctx)
}

func (w wrappedQueryState) SaveCollectionTargets(ctx context.Context, targets []*querypb.CollectionTarget, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.SaveCollectionTargets(ctx, targets...)
}

func (w wrappedQueryState) RemoveCollectionTarget(ctx context.Context, collectionID int64, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.RemoveCollectionTarget(ctx, collectionID)
}

func (w wrappedQueryState) RemoveCollectionTargets(ctx context.Context, opts catalog.WriteOptions) error {
	if w.query == nil {
		return missing("querycoord")
	}
	return w.query.RemoveCollectionTargets(ctx)
}

func (w wrappedQueryState) GetCollectionTargets(ctx context.Context, opts catalog.ReadOptions) (map[int64]*querypb.CollectionTarget, error) {
	if w.query == nil {
		return nil, missing("querycoord")
	}
	return w.query.GetCollectionTargets(ctx)
}

type wrappedStreamingState struct {
	streamingCoord metastore.StreamingCoordCatalog
	streamingNode  metastore.StreamingNodeCatalog
}

func (w wrappedStreamingState) GetCChannel(ctx context.Context, opts catalog.ReadOptions) (*streamingpb.CChannelMeta, error) {
	if w.streamingCoord == nil {
		return nil, missing("streamingcoord")
	}
	return w.streamingCoord.GetCChannel(ctx)
}

func (w wrappedStreamingState) SaveCChannel(ctx context.Context, channel *streamingpb.CChannelMeta, opts catalog.WriteOptions) error {
	if w.streamingCoord == nil {
		return missing("streamingcoord")
	}
	return w.streamingCoord.SaveCChannel(ctx, channel)
}

func (w wrappedStreamingState) GetVersion(ctx context.Context, opts catalog.ReadOptions) (*streamingpb.StreamingVersion, error) {
	if w.streamingCoord == nil {
		return nil, missing("streamingcoord")
	}
	return w.streamingCoord.GetVersion(ctx)
}

func (w wrappedStreamingState) SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion, opts catalog.WriteOptions) error {
	if w.streamingCoord == nil {
		return missing("streamingcoord")
	}
	return w.streamingCoord.SaveVersion(ctx, version)
}

func (w wrappedStreamingState) ListPChannels(ctx context.Context, opts catalog.ReadOptions) ([]*streamingpb.PChannelMeta, error) {
	if w.streamingCoord == nil {
		return nil, missing("streamingcoord")
	}
	return w.streamingCoord.ListPChannel(ctx)
}

func (w wrappedStreamingState) SavePChannels(ctx context.Context, channels []*streamingpb.PChannelMeta, opts catalog.WriteOptions) error {
	if w.streamingCoord == nil {
		return missing("streamingcoord")
	}
	return w.streamingCoord.SavePChannels(ctx, channels)
}

func (w wrappedStreamingState) ListBroadcastTasks(ctx context.Context, opts catalog.ReadOptions) ([]*streamingpb.BroadcastTask, error) {
	if w.streamingCoord == nil {
		return nil, missing("streamingcoord")
	}
	return w.streamingCoord.ListBroadcastTask(ctx)
}

func (w wrappedStreamingState) SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask, opts catalog.WriteOptions) error {
	if w.streamingCoord == nil {
		return missing("streamingcoord")
	}
	return w.streamingCoord.SaveBroadcastTask(ctx, broadcastID, task)
}

func (w wrappedStreamingState) SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta, opts catalog.WriteOptions) error {
	if w.streamingCoord == nil {
		return missing("streamingcoord")
	}
	return w.streamingCoord.SaveReplicateConfiguration(ctx, config, replicatingTasks)
}

func (w wrappedStreamingState) GetReplicateConfiguration(ctx context.Context, opts catalog.ReadOptions) (*streamingpb.ReplicateConfigurationMeta, error) {
	if w.streamingCoord == nil {
		return nil, missing("streamingcoord")
	}
	return w.streamingCoord.GetReplicateConfiguration(ctx)
}

func (w wrappedStreamingState) ListVChannels(ctx context.Context, pchannel string, opts catalog.ReadOptions) ([]*streamingpb.VChannelMeta, error) {
	if w.streamingNode == nil {
		return nil, missing("streamingnode")
	}
	return w.streamingNode.ListVChannel(ctx, pchannel)
}

func (w wrappedStreamingState) SaveVChannels(ctx context.Context, pchannel string, channels map[string]*streamingpb.VChannelMeta, opts catalog.WriteOptions) error {
	if w.streamingNode == nil {
		return missing("streamingnode")
	}
	return w.streamingNode.SaveVChannels(ctx, pchannel, channels)
}

func (w wrappedStreamingState) ListSegmentAssignments(ctx context.Context, pchannel string, opts catalog.ReadOptions) ([]*streamingpb.SegmentAssignmentMeta, error) {
	if w.streamingNode == nil {
		return nil, missing("streamingnode")
	}
	return w.streamingNode.ListSegmentAssignment(ctx, pchannel)
}

func (w wrappedStreamingState) SaveSegmentAssignments(ctx context.Context, pchannel string, assignments map[int64]*streamingpb.SegmentAssignmentMeta, opts catalog.WriteOptions) error {
	if w.streamingNode == nil {
		return missing("streamingnode")
	}
	return w.streamingNode.SaveSegmentAssignments(ctx, pchannel, assignments)
}

func (w wrappedStreamingState) GetConsumeCheckpoint(ctx context.Context, pchannel string, opts catalog.ReadOptions) (*streamingpb.WALCheckpoint, error) {
	if w.streamingNode == nil {
		return nil, missing("streamingnode")
	}
	return w.streamingNode.GetConsumeCheckpoint(ctx, pchannel)
}

func (w wrappedStreamingState) SaveConsumeCheckpoint(ctx context.Context, pchannel string, checkpoint *streamingpb.WALCheckpoint, opts catalog.WriteOptions) error {
	if w.streamingNode == nil {
		return missing("streamingnode")
	}
	return w.streamingNode.SaveConsumeCheckpoint(ctx, pchannel, checkpoint)
}

func (w wrappedStreamingState) SaveSalvageCheckpoint(ctx context.Context, pchannel string, checkpoint *commonpb.ReplicateCheckpoint, opts catalog.WriteOptions) error {
	if w.streamingNode == nil {
		return missing("streamingnode")
	}
	return w.streamingNode.SaveSalvageCheckpoint(ctx, pchannel, checkpoint)
}

func (w wrappedStreamingState) GetSalvageCheckpoint(ctx context.Context, pchannel string, opts catalog.ReadOptions) ([]*commonpb.ReplicateCheckpoint, error) {
	if w.streamingNode == nil {
		return nil, missing("streamingnode")
	}
	return w.streamingNode.GetSalvageCheckpoint(ctx, pchannel)
}

type unsupportedMigration struct{}

func (unsupportedMigration) State(ctx context.Context) (*catalog.MigrationState, error) {
	return nil, unsupported("migration state")
}

func (unsupportedMigration) SetState(ctx context.Context, state catalog.MigrationState) error {
	return unsupported("migration set state")
}

func (unsupportedMigration) Backfill(ctx context.Context, req catalog.BackfillRequest) (*catalog.BackfillResult, error) {
	return nil, unsupported("migration backfill")
}

func (unsupportedMigration) Compare(ctx context.Context, req catalog.CompareRequest) (*catalog.CompareResult, error) {
	return nil, unsupported("migration compare")
}

func (unsupportedMigration) Cutover(ctx context.Context, req catalog.CutoverRequest) error {
	return unsupported("migration cutover")
}

func (unsupportedMigration) Rollback(ctx context.Context, req catalog.RollbackRequest) error {
	return unsupported("migration rollback")
}
