package metastore

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

//go:generate mockery --name=RootCoordCatalog
type RootCoordCatalog interface {
	CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error
	DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error
	ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error)
	AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error

	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error)
	GetCollectionByName(ctx context.Context, dbID int64, collectionName string, ts typeutil.Timestamp) (*model.Collection, error)
	ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error)
	CollectionExists(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool
	DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType AlterType, ts typeutil.Timestamp) error

	CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error
	DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error
	AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType AlterType, ts typeutil.Timestamp) error

	CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error
	AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error
	ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error)

	// GetCredential gets the credential info for the username, returns error if no credential exists for this username.
	GetCredential(ctx context.Context, username string) (*model.Credential, error)
	// CreateCredential creates credential by Username and EncryptedPassword in crediential. Please make sure credential.Username isn't empty before calling this API. Credentials already exists will be altered.
	CreateCredential(ctx context.Context, credential *model.Credential) error
	// AlterCredential does exactly the same as CreateCredential
	AlterCredential(ctx context.Context, credential *model.Credential) error
	// DropCredential removes the credential of this username
	DropCredential(ctx context.Context, username string) error
	// ListCredentials gets all usernames.
	ListCredentials(ctx context.Context) ([]string, error)

	// CreateRole creates role by the entity for the tenant. Please make sure the tenent and entity.Name aren't empty. Empty entity.Name may end up with deleting all roles
	// Returns common.IgnorableError if the role already existes
	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
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

	ListCredentialsWithPasswd(ctx context.Context) (map[string]string, error)
	BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error)
	RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error

	GetPrivilegeGroup(ctx context.Context, groupName string) (*milvuspb.PrivilegeGroupInfo, error)
	DropPrivilegeGroup(ctx context.Context, groupName string) error
	SavePrivilegeGroup(ctx context.Context, data *milvuspb.PrivilegeGroupInfo) error
	ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error)

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

type BinlogsIncrement struct {
	Segment *datapb.SegmentInfo
}

//go:generate mockery --name=DataCoordCatalog --with-expecter
type DataCoordCatalog interface {
	ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error)
	AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error
	// TODO Remove this later, we should update flush segments info for each segment separately, so far we still need transaction
	AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...BinlogsIncrement) error
	SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error
	DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error

	MarkChannelAdded(ctx context.Context, channel string) error
	MarkChannelDeleted(ctx context.Context, channel string) error
	ShouldDropChannel(ctx context.Context, channel string) bool
	ChannelExists(ctx context.Context, channel string) bool
	DropChannel(ctx context.Context, channel string) error

	ListChannelCheckpoint(ctx context.Context) (map[string]*msgpb.MsgPosition, error)
	SaveChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error
	SaveChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error
	DropChannelCheckpoint(ctx context.Context, vChannel string) error

	CreateIndex(ctx context.Context, index *model.Index) error
	ListIndexes(ctx context.Context) ([]*model.Index, error)
	AlterIndexes(ctx context.Context, newIndexes []*model.Index) error
	DropIndex(ctx context.Context, collID, dropIdxID typeutil.UniqueID) error

	CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error
	ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error)
	AlterSegmentIndexes(ctx context.Context, newSegIdxes []*model.SegmentIndex) error
	DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error

	SaveImportJob(ctx context.Context, job *datapb.ImportJob) error
	ListImportJobs(ctx context.Context) ([]*datapb.ImportJob, error)
	DropImportJob(ctx context.Context, jobID int64) error
	SavePreImportTask(ctx context.Context, task *datapb.PreImportTask) error
	ListPreImportTasks(ctx context.Context) ([]*datapb.PreImportTask, error)
	DropPreImportTask(ctx context.Context, taskID int64) error
	SaveImportTask(ctx context.Context, task *datapb.ImportTaskV2) error
	ListImportTasks(ctx context.Context) ([]*datapb.ImportTaskV2, error)
	DropImportTask(ctx context.Context, taskID int64) error

	GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool

	ListCompactionTask(ctx context.Context) ([]*datapb.CompactionTask, error)
	SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error
	DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error

	ListAnalyzeTasks(ctx context.Context) ([]*indexpb.AnalyzeTask, error)
	SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask) error
	DropAnalyzeTask(ctx context.Context, taskID typeutil.UniqueID) error

	ListPartitionStatsInfos(ctx context.Context) ([]*datapb.PartitionStatsInfo, error)
	SavePartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error
	DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error

	SaveCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string, currentVersion int64) error
	GetCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) (int64, error)
	DropCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) error

	ListStatsTasks(ctx context.Context) ([]*indexpb.StatsTask, error)
	SaveStatsTask(ctx context.Context, task *indexpb.StatsTask) error
	DropStatsTask(ctx context.Context, taskID typeutil.UniqueID) error
}

type QueryCoordCatalog interface {
	SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error
	SavePartition(ctx context.Context, info ...*querypb.PartitionLoadInfo) error
	SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error
	GetCollections(ctx context.Context) ([]*querypb.CollectionLoadInfo, error)
	GetPartitions(ctx context.Context, collectionIDs []int64) (map[int64][]*querypb.PartitionLoadInfo, error)
	GetReplicas(ctx context.Context) ([]*querypb.Replica, error)
	ReleaseCollection(ctx context.Context, collection int64) error
	ReleasePartition(ctx context.Context, collection int64, partitions ...int64) error
	ReleaseReplicas(ctx context.Context, collectionID int64) error
	ReleaseReplica(ctx context.Context, collection int64, replicas ...int64) error
	SaveResourceGroup(ctx context.Context, rgs ...*querypb.ResourceGroup) error
	RemoveResourceGroup(ctx context.Context, rgName string) error
	GetResourceGroups(ctx context.Context) ([]*querypb.ResourceGroup, error)

	SaveCollectionTargets(ctx context.Context, target ...*querypb.CollectionTarget) error
	RemoveCollectionTarget(ctx context.Context, collectionID int64) error
	GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error)
}

// StreamingCoordCataLog is the interface for streamingcoord catalog
type StreamingCoordCataLog interface {
	// physical channel watch related

	// ListPChannel list all pchannels on milvus.
	ListPChannel(ctx context.Context) ([]*streamingpb.PChannelMeta, error)

	// SavePChannel save a pchannel info to metastore.
	SavePChannels(ctx context.Context, info []*streamingpb.PChannelMeta) error

	// ListBroadcastTask list all broadcast tasks.
	// Used to recovery the broadcast tasks.
	ListBroadcastTask(ctx context.Context) ([]*streamingpb.BroadcastTask, error)

	// SaveBroadcastTask save the broadcast task to metastore.
	// Make the task recoverable after restart.
	// When broadcast task is done, it will be removed from metastore.
	SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask) error
}

// StreamingNodeCataLog is the interface for streamingnode catalog
type StreamingNodeCataLog interface {
	// WAL select the wal related recovery infos.
	// Which must give the pchannel name.

	// ListSegmentAssignment list all segment assignments for the wal.
	ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error)

	// SaveSegmentAssignments save the segment assignments for the wal.
	SaveSegmentAssignments(ctx context.Context, pChannelName string, infos []*streamingpb.SegmentAssignmentMeta) error

	// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
	// Return nil, nil if the checkpoint is not exist.
	GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error)

	// SaveConsumeCheckpoint saves the consuming checkpoint of the wal.
	SaveConsumeCheckpoint(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint) error
}
