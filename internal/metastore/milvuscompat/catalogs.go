package milvuscompat

import (
	"context"

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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type Catalogs struct {
	RootCoord      metastore.RootCoordCatalog
	DataCoord      metastore.DataCoordCatalog
	QueryCoord     metastore.QueryCoordCatalog
	StreamingCoord metastore.StreamingCoordCatalog
	StreamingNode  metastore.StreamingNodeCatalog
}

func New(c Catalog) Catalogs {
	return Catalogs{
		RootCoord:      rootCoordCatalog{c: c},
		DataCoord:      dataCoordCatalog{c: c},
		QueryCoord:     queryCoordCatalog{c: c},
		StreamingCoord: streamingCoordCatalog{c: c},
		StreamingNode:  streamingNodeCatalog{c: c},
	}
}

type (
	rootCoordCatalog      struct{ c Catalog }
	dataCoordCatalog      struct{ c Catalog }
	queryCoordCatalog     struct{ c Catalog }
	streamingCoordCatalog struct{ c Catalog }
	streamingNodeCatalog  struct{ c Catalog }
)

var (
	_ metastore.RootCoordCatalog      = rootCoordCatalog{}
	_ metastore.DataCoordCatalog      = dataCoordCatalog{}
	_ metastore.QueryCoordCatalog     = queryCoordCatalog{}
	_ metastore.StreamingCoordCatalog = streamingCoordCatalog{}
	_ metastore.StreamingNodeCatalog  = streamingNodeCatalog{}
)

func ro(ts typeutil.Timestamp) catalog.ReadOptions {
	return catalog.ReadOptions{At: ts}
}

func wo(ts typeutil.Timestamp) catalog.WriteOptions {
	return catalog.WriteOptions{Timestamp: ts}
}

// collapseBool maps the canonical (bool, error) catalog API to a legacy bool
// return. Errors silently become false because the legacy interface has no
// error channel. Callers that need to distinguish "definitively false" from
// "backend unavailable" must migrate to the (bool, error) API directly.
func collapseBool(result bool, err error) bool {
	return err == nil && result
}

func (r rootCoordCatalog) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	return r.c.Metadata().Databases().Create(ctx, db, wo(ts))
}

func (r rootCoordCatalog) DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error {
	return r.c.Metadata().Databases().Drop(ctx, catalog.DatabaseRef{ID: dbID}, wo(ts))
}

func (r rootCoordCatalog) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	return r.c.Metadata().Databases().List(ctx, ro(ts))
}

func (r rootCoordCatalog) AlterDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	return r.c.Metadata().Databases().Alter(ctx, db, wo(ts))
}

func (r rootCoordCatalog) CreateCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return r.c.Metadata().Collections().Create(ctx, collection, wo(ts))
}

func (r rootCoordCatalog) GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error) {
	return r.c.Metadata().Collections().Get(ctx, catalog.CollectionRef{Database: catalog.DatabaseRef{ID: dbID}, ID: collectionID}, ro(ts))
}

func (r rootCoordCatalog) GetCollectionByName(ctx context.Context, dbID int64, dbName string, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	return r.c.Metadata().Collections().Get(ctx, catalog.CollectionRef{Database: catalog.DatabaseRef{ID: dbID, Name: dbName}, Name: collectionName}, ro(ts))
}

func (r rootCoordCatalog) ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error) {
	return r.c.Metadata().Collections().List(ctx, catalog.DatabaseRef{ID: dbID}, ro(ts))
}

func (r rootCoordCatalog) CollectionExists(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	return collapseBool(r.c.Metadata().Collections().Exists(ctx, catalog.CollectionRef{Database: catalog.DatabaseRef{ID: dbID}, ID: collectionID}, ro(ts)))
}

func (r rootCoordCatalog) DropCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return r.c.Metadata().Collections().Drop(ctx, collection, wo(ts))
}

func (r rootCoordCatalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp, fieldModify bool) error {
	return r.c.Metadata().Collections().Alter(ctx, catalog.AlterCollectionRequest{
		Old:         oldColl,
		New:         newColl,
		AlterType:   catalog.AlterType(alterType),
		FieldModify: fieldModify,
	}, wo(ts))
}

func (r rootCoordCatalog) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error {
	return r.c.Metadata().Collections().Alter(ctx, catalog.AlterCollectionRequest{
		Old:       oldColl,
		New:       newColl,
		AlterType: catalog.MODIFY,
	}, wo(ts))
}

func (r rootCoordCatalog) CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error {
	return r.c.Metadata().Partitions().Create(ctx, catalog.DatabaseRef{ID: dbID}, partition, wo(ts))
}

func (r rootCoordCatalog) DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	return r.c.Metadata().Partitions().Drop(ctx, catalog.PartitionRef{Collection: catalog.CollectionRef{Database: catalog.DatabaseRef{ID: dbID}, ID: collectionID}, ID: partitionID}, wo(ts))
}

func (r rootCoordCatalog) AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	return r.c.Metadata().Partitions().Alter(ctx, catalog.AlterPartitionRequest{DatabaseID: dbID, Old: oldPart, New: newPart, AlterType: catalog.AlterType(alterType)}, wo(ts))
}

func (r rootCoordCatalog) CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return r.c.Metadata().Aliases().Create(ctx, alias, wo(ts))
}

func (r rootCoordCatalog) DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error {
	return r.c.Metadata().Aliases().Drop(ctx, catalog.AliasRef{Database: catalog.DatabaseRef{ID: dbID}, Name: alias}, wo(ts))
}

func (r rootCoordCatalog) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return r.c.Metadata().Aliases().Alter(ctx, alias, wo(ts))
}

func (r rootCoordCatalog) ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error) {
	return r.c.Metadata().Aliases().List(ctx, catalog.DatabaseRef{ID: dbID}, ro(ts))
}

func (r rootCoordCatalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	return r.c.AccessControl().Credentials().Get(ctx, username, catalog.ReadOptions{})
}

func (r rootCoordCatalog) AlterCredential(ctx context.Context, credential *model.Credential) error {
	return r.c.AccessControl().Credentials().Alter(ctx, credential, catalog.WriteOptions{})
}

func (r rootCoordCatalog) DropCredential(ctx context.Context, username string) error {
	return r.c.AccessControl().Credentials().Drop(ctx, username, catalog.WriteOptions{})
}

func (r rootCoordCatalog) ListCredentials(ctx context.Context) ([]string, error) {
	return r.c.AccessControl().Credentials().List(ctx, catalog.ReadOptions{})
}

func (r rootCoordCatalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	return r.c.AccessControl().Roles().Create(ctx, tenant, entity, catalog.WriteOptions{})
}

func (r rootCoordCatalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	return r.c.AccessControl().Roles().Drop(ctx, tenant, roleName, catalog.WriteOptions{})
}

func (r rootCoordCatalog) AlterUserRole(ctx context.Context, tenant string, user *milvuspb.UserEntity, role *milvuspb.RoleEntity, op milvuspb.OperateUserRoleType) error {
	return r.c.AccessControl().Roles().AlterUserRole(ctx, tenant, user, role, op, catalog.WriteOptions{})
}

func (r rootCoordCatalog) ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	return r.c.AccessControl().Roles().List(ctx, tenant, entity, includeUserInfo, catalog.ReadOptions{})
}

func (r rootCoordCatalog) ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	return r.c.AccessControl().Roles().ListUser(ctx, tenant, entity, includeRoleInfo, catalog.ReadOptions{})
}

func (r rootCoordCatalog) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, op milvuspb.OperatePrivilegeType) error {
	return r.c.AccessControl().Grants().Alter(ctx, tenant, entity, op, catalog.WriteOptions{})
}

func (r rootCoordCatalog) DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	return r.c.AccessControl().Grants().DeleteByRole(ctx, tenant, role, catalog.WriteOptions{})
}

func (r rootCoordCatalog) ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	return r.c.AccessControl().Grants().List(ctx, tenant, entity, catalog.ReadOptions{})
}

func (r rootCoordCatalog) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	return r.c.AccessControl().Grants().ListPolicy(ctx, tenant, catalog.ReadOptions{})
}

func (r rootCoordCatalog) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	return r.c.AccessControl().Roles().ListUserRole(ctx, tenant, catalog.ReadOptions{})
}

func (r rootCoordCatalog) DeleteGrantByCollectionName(ctx context.Context, tenant string, dbName string, collectionName string) error {
	return r.c.AccessControl().Grants().DeleteByCollectionName(ctx, tenant, dbName, collectionName, catalog.WriteOptions{})
}

func (r rootCoordCatalog) MigrateGrantCollectionName(ctx context.Context, tenant string, oldDBName string, oldName string, newDBName string, newName string) error {
	return r.c.AccessControl().Grants().MigrateCollectionName(ctx, tenant, oldDBName, oldName, newDBName, newName, catalog.WriteOptions{})
}

func (r rootCoordCatalog) ListCredentialsWithPasswd(ctx context.Context) (map[string]string, error) {
	return r.c.AccessControl().Credentials().ListWithPasswords(ctx, catalog.ReadOptions{})
}

func (r rootCoordCatalog) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	return r.c.AccessControl().Grants().Backup(ctx, tenant, catalog.ReadOptions{})
}

func (r rootCoordCatalog) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	return r.c.AccessControl().Grants().Restore(ctx, tenant, meta, catalog.WriteOptions{})
}

func (r rootCoordCatalog) GetPrivilegeGroup(ctx context.Context, groupName string) (*milvuspb.PrivilegeGroupInfo, error) {
	return r.c.AccessControl().PrivilegeGroups().Get(ctx, groupName, catalog.ReadOptions{})
}

func (r rootCoordCatalog) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	return r.c.AccessControl().PrivilegeGroups().Drop(ctx, groupName, catalog.WriteOptions{})
}

func (r rootCoordCatalog) SavePrivilegeGroup(ctx context.Context, data *milvuspb.PrivilegeGroupInfo) error {
	return r.c.AccessControl().PrivilegeGroups().Save(ctx, data, catalog.WriteOptions{})
}

func (r rootCoordCatalog) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	return r.c.AccessControl().PrivilegeGroups().List(ctx, catalog.ReadOptions{})
}

func (r rootCoordCatalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	return r.c.MetadataInternal().Files().Save(ctx, resource, version, catalog.WriteOptions{})
}

func (r rootCoordCatalog) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	return r.c.MetadataInternal().Files().Remove(ctx, resourceID, version, catalog.WriteOptions{})
}

func (r rootCoordCatalog) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	return r.c.MetadataInternal().Files().List(ctx, catalog.ReadOptions{})
}

func (r rootCoordCatalog) Close() {
	_ = r.c.Close(context.Background())
}

func (d dataCoordCatalog) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
	return d.c.MetadataInternal().Segments().List(ctx, ListSegmentsRequest{CollectionID: collectionID}, catalog.ReadOptions{})
}

func (d dataCoordCatalog) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	return d.c.MetadataInternal().Segments().Save(ctx, segment, catalog.WriteOptions{})
}

func (d dataCoordCatalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	return d.c.MetadataInternal().Segments().UpdateBatch(ctx, UpdateSegmentsRequest{Segments: segments, Binlogs: binlogs}, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	return d.c.MetadataInternal().Segments().MarkDropped(ctx, segments, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	return d.c.MetadataInternal().Segments().Drop(ctx, segment, catalog.WriteOptions{})
}

func (d dataCoordCatalog) MarkChannelAdded(ctx context.Context, channel string) error {
	return d.c.InternalState().DataCoord().MarkChannelAdded(ctx, channel, catalog.WriteOptions{})
}

func (d dataCoordCatalog) MarkChannelDeleted(ctx context.Context, channel string) error {
	return d.c.InternalState().DataCoord().MarkChannelDeleted(ctx, channel, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ShouldDropChannel(ctx context.Context, channel string) bool {
	return collapseBool(d.c.InternalState().DataCoord().ShouldDropChannel(ctx, channel, catalog.ReadOptions{}))
}

func (d dataCoordCatalog) ChannelExists(ctx context.Context, channel string) bool {
	return collapseBool(d.c.InternalState().DataCoord().ChannelExists(ctx, channel, catalog.ReadOptions{}))
}

func (d dataCoordCatalog) DropChannel(ctx context.Context, channel string) error {
	return d.c.InternalState().DataCoord().DropChannel(ctx, channel, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListChannelCheckpoint(ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
	return d.c.InternalState().DataCoord().ListChannelCheckpoints(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	return d.c.InternalState().DataCoord().SaveChannelCheckpoint(ctx, vChannel, pos, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error {
	return d.c.InternalState().DataCoord().SaveChannelCheckpoints(ctx, positions, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropChannelCheckpoint(ctx context.Context, vChannel string) error {
	return d.c.InternalState().DataCoord().DropChannelCheckpoint(ctx, vChannel, catalog.WriteOptions{})
}

func (d dataCoordCatalog) CreateIndex(ctx context.Context, index *model.Index) error {
	return d.c.Metadata().Indexes().CreateIndex(ctx, index, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	return d.c.Metadata().Indexes().ListIndexes(ctx, catalog.ListIndexesRequest{}, catalog.ReadOptions{})
}

func (d dataCoordCatalog) AlterIndexes(ctx context.Context, indexes []*model.Index) error {
	return d.c.Metadata().Indexes().AlterIndexes(ctx, indexes, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropIndex(ctx context.Context, collID typeutil.UniqueID, dropIdxID typeutil.UniqueID) error {
	return d.c.Metadata().Indexes().DropIndex(ctx, catalog.IndexRef{CollectionID: collID, IndexID: dropIdxID}, catalog.WriteOptions{})
}

func (d dataCoordCatalog) CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error {
	return d.c.Metadata().Indexes().SaveSegmentIndex(ctx, segIdx, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListSegmentIndexes(ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
	return d.c.Metadata().Indexes().ListSegmentIndexes(ctx, catalog.ListSegmentIndexesRequest{CollectionID: collectionID}, catalog.ReadOptions{})
}

func (d dataCoordCatalog) AlterSegmentIndexes(ctx context.Context, indexes []*model.SegmentIndex) error {
	return d.c.Metadata().Indexes().AlterSegmentIndexes(ctx, indexes, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error {
	return d.c.Metadata().Indexes().DropSegmentIndex(ctx, catalog.SegmentIndexRef{CollectionID: collID, PartitionID: partID, SegmentID: segID, BuildID: buildID}, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveImportJob(ctx context.Context, job *datapb.ImportJob) error {
	return d.c.InternalState().DataCoord().SaveImportJob(ctx, job, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListImportJobs(ctx context.Context) ([]*datapb.ImportJob, error) {
	return d.c.InternalState().DataCoord().ListImportJobs(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) DropImportJob(ctx context.Context, jobID int64) error {
	return d.c.InternalState().DataCoord().DropImportJob(ctx, jobID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SavePreImportTask(ctx context.Context, task *datapb.PreImportTask) error {
	return d.c.InternalState().DataCoord().SavePreImportTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListPreImportTasks(ctx context.Context) ([]*datapb.PreImportTask, error) {
	return d.c.InternalState().DataCoord().ListPreImportTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) DropPreImportTask(ctx context.Context, taskID int64) error {
	return d.c.InternalState().DataCoord().DropPreImportTask(ctx, taskID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveImportTask(ctx context.Context, task *datapb.ImportTaskV2) error {
	return d.c.InternalState().DataCoord().SaveImportTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListImportTasks(ctx context.Context) ([]*datapb.ImportTaskV2, error) {
	return d.c.InternalState().DataCoord().ListImportTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) DropImportTask(ctx context.Context, taskID int64) error {
	return d.c.InternalState().DataCoord().DropImportTask(ctx, taskID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveCopySegmentJob(ctx context.Context, job *datapb.CopySegmentJob) error {
	return d.c.InternalState().DataCoord().SaveCopySegmentJob(ctx, job, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListCopySegmentJobs(ctx context.Context) ([]*datapb.CopySegmentJob, error) {
	return d.c.InternalState().DataCoord().ListCopySegmentJobs(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) DropCopySegmentJob(ctx context.Context, jobID int64) error {
	return d.c.InternalState().DataCoord().DropCopySegmentJob(ctx, jobID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask) error {
	return d.c.InternalState().DataCoord().SaveCopySegmentTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveCopySegmentTasksBatch(ctx context.Context, tasks []*datapb.CopySegmentTask) error {
	return d.c.InternalState().DataCoord().SaveCopySegmentTasksBatch(ctx, tasks, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListCopySegmentTasks(ctx context.Context) ([]*datapb.CopySegmentTask, error) {
	return d.c.InternalState().DataCoord().ListCopySegmentTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) DropCopySegmentTask(ctx context.Context, taskID int64) error {
	return d.c.InternalState().DataCoord().DropCopySegmentTask(ctx, taskID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	return collapseBool(d.c.InternalState().DataCoord().GcConfirm(ctx, collectionID, partitionID, catalog.ReadOptions{}))
}

func (d dataCoordCatalog) ListCompactionTask(ctx context.Context) ([]*datapb.CompactionTask, error) {
	return d.c.InternalState().DataCoord().ListCompactionTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	return d.c.InternalState().DataCoord().SaveCompactionTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	return d.c.InternalState().DataCoord().DropCompactionTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListAnalyzeTasks(ctx context.Context) ([]*indexpb.AnalyzeTask, error) {
	return d.c.InternalState().DataCoord().ListAnalyzeTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask) error {
	return d.c.InternalState().DataCoord().SaveAnalyzeTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropAnalyzeTask(ctx context.Context, taskID typeutil.UniqueID) error {
	return d.c.InternalState().DataCoord().DropAnalyzeTask(ctx, taskID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListPartitionStatsInfos(ctx context.Context) ([]*datapb.PartitionStatsInfo, error) {
	return d.c.InternalState().DataCoord().ListPartitionStatsInfos(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SavePartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	return d.c.InternalState().DataCoord().SavePartitionStatsInfo(ctx, info, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	return d.c.InternalState().DataCoord().DropPartitionStatsInfo(ctx, info, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string, currentVersion int64) error {
	return d.c.InternalState().DataCoord().SaveCurrentPartitionStatsVersion(ctx, collID, partID, vChannel, currentVersion, catalog.WriteOptions{})
}

func (d dataCoordCatalog) GetCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) (int64, error) {
	return d.c.InternalState().DataCoord().GetCurrentPartitionStatsVersion(ctx, collID, partID, vChannel, catalog.ReadOptions{})
}

func (d dataCoordCatalog) DropCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) error {
	return d.c.InternalState().DataCoord().DropCurrentPartitionStatsVersion(ctx, collID, partID, vChannel, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListStatsTasks(ctx context.Context) ([]*indexpb.StatsTask, error) {
	return d.c.InternalState().DataCoord().ListStatsTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveStatsTask(ctx context.Context, task *indexpb.StatsTask) error {
	return d.c.InternalState().DataCoord().SaveStatsTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropStatsTask(ctx context.Context, taskID typeutil.UniqueID) error {
	return d.c.InternalState().DataCoord().DropStatsTask(ctx, taskID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListExternalCollectionRefreshJobs(ctx context.Context) ([]*datapb.ExternalCollectionRefreshJob, error) {
	return d.c.InternalState().DataCoord().ListExternalCollectionRefreshJobs(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	return d.c.InternalState().DataCoord().SaveExternalCollectionRefreshJob(ctx, job, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropExternalCollectionRefreshJob(ctx context.Context, jobID typeutil.UniqueID) error {
	return d.c.InternalState().DataCoord().DropExternalCollectionRefreshJob(ctx, jobID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListExternalCollectionRefreshTasks(ctx context.Context) ([]*datapb.ExternalCollectionRefreshTask, error) {
	return d.c.InternalState().DataCoord().ListExternalCollectionRefreshTasks(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask) error {
	return d.c.InternalState().DataCoord().SaveExternalCollectionRefreshTask(ctx, task, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropExternalCollectionRefreshTask(ctx context.Context, taskID typeutil.UniqueID) error {
	return d.c.InternalState().DataCoord().DropExternalCollectionRefreshTask(ctx, taskID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	return d.c.MetadataInternal().Files().Save(ctx, resource, version, catalog.WriteOptions{})
}

func (d dataCoordCatalog) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	return d.c.MetadataInternal().Files().Remove(ctx, resourceID, version, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	return d.c.MetadataInternal().Files().List(ctx, catalog.ReadOptions{})
}

func (d dataCoordCatalog) SaveSnapshot(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
	return d.c.MetadataInternal().Snapshots().Save(ctx, snapshot, catalog.WriteOptions{})
}

func (d dataCoordCatalog) DropSnapshot(ctx context.Context, collectionID int64, snapshotID int64) error {
	return d.c.MetadataInternal().Snapshots().Drop(ctx, collectionID, snapshotID, catalog.WriteOptions{})
}

func (d dataCoordCatalog) ListSnapshots(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
	return d.c.MetadataInternal().Snapshots().List(ctx, catalog.ReadOptions{})
}

func (q queryCoordCatalog) SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error {
	return q.c.InternalState().QueryCoord().SaveCollection(ctx, collection, partitions, catalog.WriteOptions{})
}

func (q queryCoordCatalog) SavePartition(ctx context.Context, info ...*querypb.PartitionLoadInfo) error {
	return q.c.InternalState().QueryCoord().SavePartition(ctx, info, catalog.WriteOptions{})
}

func (q queryCoordCatalog) SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error {
	return q.c.InternalState().QueryCoord().SaveReplica(ctx, replicas, catalog.WriteOptions{})
}

func (q queryCoordCatalog) GetCollections(ctx context.Context) ([]*querypb.CollectionLoadInfo, error) {
	return q.c.InternalState().QueryCoord().GetCollections(ctx, catalog.ReadOptions{})
}

func (q queryCoordCatalog) GetPartitions(ctx context.Context, collectionIDs []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
	return q.c.InternalState().QueryCoord().GetPartitions(ctx, collectionIDs, catalog.ReadOptions{})
}

func (q queryCoordCatalog) GetReplicas(ctx context.Context) ([]*querypb.Replica, error) {
	return q.c.InternalState().QueryCoord().GetReplicas(ctx, catalog.ReadOptions{})
}

func (q queryCoordCatalog) ReleaseCollection(ctx context.Context, collection int64) error {
	return q.c.InternalState().QueryCoord().ReleaseCollection(ctx, collection, catalog.WriteOptions{})
}

func (q queryCoordCatalog) ReleasePartition(ctx context.Context, collection int64, partitions ...int64) error {
	return q.c.InternalState().QueryCoord().ReleasePartition(ctx, collection, partitions, catalog.WriteOptions{})
}

func (q queryCoordCatalog) ReleaseReplicas(ctx context.Context, collectionID int64) error {
	return q.c.InternalState().QueryCoord().ReleaseReplicas(ctx, collectionID, catalog.WriteOptions{})
}

func (q queryCoordCatalog) ReleaseReplica(ctx context.Context, collection int64, replicas ...int64) error {
	return q.c.InternalState().QueryCoord().ReleaseReplica(ctx, collection, replicas, catalog.WriteOptions{})
}

func (q queryCoordCatalog) SaveResourceGroup(ctx context.Context, rgs ...*querypb.ResourceGroup) error {
	return q.c.InternalState().QueryCoord().SaveResourceGroup(ctx, rgs, catalog.WriteOptions{})
}

func (q queryCoordCatalog) RemoveResourceGroup(ctx context.Context, rgName string) error {
	return q.c.InternalState().QueryCoord().RemoveResourceGroup(ctx, rgName, catalog.WriteOptions{})
}

func (q queryCoordCatalog) GetResourceGroups(ctx context.Context) ([]*querypb.ResourceGroup, error) {
	return q.c.InternalState().QueryCoord().GetResourceGroups(ctx, catalog.ReadOptions{})
}

func (q queryCoordCatalog) SaveCollectionTargets(ctx context.Context, target ...*querypb.CollectionTarget) error {
	return q.c.InternalState().QueryCoord().SaveCollectionTargets(ctx, target, catalog.WriteOptions{})
}

func (q queryCoordCatalog) RemoveCollectionTarget(ctx context.Context, collectionID int64) error {
	return q.c.InternalState().QueryCoord().RemoveCollectionTarget(ctx, collectionID, catalog.WriteOptions{})
}

func (q queryCoordCatalog) RemoveCollectionTargets(ctx context.Context) error {
	return q.c.InternalState().QueryCoord().RemoveCollectionTargets(ctx, catalog.WriteOptions{})
}

func (q queryCoordCatalog) GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error) {
	return q.c.InternalState().QueryCoord().GetCollectionTargets(ctx, catalog.ReadOptions{})
}

func (s streamingCoordCatalog) GetCChannel(ctx context.Context) (*streamingpb.CChannelMeta, error) {
	return s.c.InternalState().Streaming().GetCChannel(ctx, catalog.ReadOptions{})
}

func (s streamingCoordCatalog) SaveCChannel(ctx context.Context, info *streamingpb.CChannelMeta) error {
	return s.c.InternalState().Streaming().SaveCChannel(ctx, info, catalog.WriteOptions{})
}

func (s streamingCoordCatalog) GetVersion(ctx context.Context) (*streamingpb.StreamingVersion, error) {
	return s.c.InternalState().Streaming().GetVersion(ctx, catalog.ReadOptions{})
}

func (s streamingCoordCatalog) SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion) error {
	return s.c.InternalState().Streaming().SaveVersion(ctx, version, catalog.WriteOptions{})
}

func (s streamingCoordCatalog) ListPChannel(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
	return s.c.InternalState().Streaming().ListPChannels(ctx, catalog.ReadOptions{})
}

func (s streamingCoordCatalog) SavePChannels(ctx context.Context, info []*streamingpb.PChannelMeta) error {
	return s.c.InternalState().Streaming().SavePChannels(ctx, info, catalog.WriteOptions{})
}

func (s streamingCoordCatalog) ListBroadcastTask(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
	return s.c.InternalState().Streaming().ListBroadcastTasks(ctx, catalog.ReadOptions{})
}

func (s streamingCoordCatalog) SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask) error {
	return s.c.InternalState().Streaming().SaveBroadcastTask(ctx, broadcastID, task, catalog.WriteOptions{})
}

func (s streamingCoordCatalog) SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
	return s.c.InternalState().Streaming().SaveReplicateConfiguration(ctx, config, replicatingTasks, catalog.WriteOptions{})
}

func (s streamingCoordCatalog) GetReplicateConfiguration(ctx context.Context) (*streamingpb.ReplicateConfigurationMeta, error) {
	return s.c.InternalState().Streaming().GetReplicateConfiguration(ctx, catalog.ReadOptions{})
}

func (s streamingNodeCatalog) ListVChannel(ctx context.Context, pchannelName string) ([]*streamingpb.VChannelMeta, error) {
	return s.c.InternalState().Streaming().ListVChannels(ctx, pchannelName, catalog.ReadOptions{})
}

func (s streamingNodeCatalog) SaveVChannels(ctx context.Context, pchannelName string, vchannels map[string]*streamingpb.VChannelMeta) error {
	return s.c.InternalState().Streaming().SaveVChannels(ctx, pchannelName, vchannels, catalog.WriteOptions{})
}

func (s streamingNodeCatalog) ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error) {
	return s.c.InternalState().Streaming().ListSegmentAssignments(ctx, pChannelName, catalog.ReadOptions{})
}

func (s streamingNodeCatalog) SaveSegmentAssignments(ctx context.Context, pChannelName string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error {
	return s.c.InternalState().Streaming().SaveSegmentAssignments(ctx, pChannelName, infos, catalog.WriteOptions{})
}

func (s streamingNodeCatalog) GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error) {
	return s.c.InternalState().Streaming().GetConsumeCheckpoint(ctx, pChannelName, catalog.ReadOptions{})
}

func (s streamingNodeCatalog) SaveConsumeCheckpoint(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint) error {
	return s.c.InternalState().Streaming().SaveConsumeCheckpoint(ctx, pChannelName, checkpoint, catalog.WriteOptions{})
}

func (s streamingNodeCatalog) SaveSalvageCheckpoint(ctx context.Context, pChannelName string, checkpoint *commonpb.ReplicateCheckpoint) error {
	return s.c.InternalState().Streaming().SaveSalvageCheckpoint(ctx, pChannelName, checkpoint, catalog.WriteOptions{})
}

func (s streamingNodeCatalog) GetSalvageCheckpoint(ctx context.Context, pChannelName string) ([]*commonpb.ReplicateCheckpoint, error) {
	return s.c.InternalState().Streaming().GetSalvageCheckpoint(ctx, pChannelName, catalog.ReadOptions{})
}
