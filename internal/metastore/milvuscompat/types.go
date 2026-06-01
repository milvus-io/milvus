package milvuscompat

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/catalog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type Catalog interface {
	catalog.Catalog
	MetadataInternal() MetadataCatalog
	InternalState() MilvusStateCatalog
}

type MetadataCatalog interface {
	catalog.MetadataCatalog
	Segments() SegmentCatalog
	Files() FileResourceCatalog
	Snapshots() SnapshotCatalog
}

type ListSegmentsRequest struct {
	CollectionID catalog.UniqueID
	PartitionID  catalog.UniqueID
	SegmentIDs   []catalog.UniqueID
}

type UpdateSegmentsRequest struct {
	Segments []*datapb.SegmentInfo
	Binlogs  []metastore.BinlogsIncrement
}

type SegmentCatalog interface {
	Save(ctx context.Context, segment *datapb.SegmentInfo, opts catalog.WriteOptions) error
	Get(ctx context.Context, ref catalog.SegmentRef, opts catalog.ReadOptions) (*datapb.SegmentInfo, error)
	List(ctx context.Context, req ListSegmentsRequest, opts catalog.ReadOptions) ([]*datapb.SegmentInfo, error)
	UpdateBatch(ctx context.Context, req UpdateSegmentsRequest, opts catalog.WriteOptions) error
	MarkDropped(ctx context.Context, segments []*datapb.SegmentInfo, opts catalog.WriteOptions) error
	Drop(ctx context.Context, segment *datapb.SegmentInfo, opts catalog.WriteOptions) error
}

type FileResourceCatalog interface {
	Save(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64, opts catalog.WriteOptions) error
	Remove(ctx context.Context, resourceID int64, version uint64, opts catalog.WriteOptions) error
	List(ctx context.Context, opts catalog.ReadOptions) ([]*internalpb.FileResourceInfo, uint64, error)
}

type GetSnapshotRequest struct {
	CollectionID catalog.UniqueID
	Timestamp    catalog.Timestamp
}

type ListManifestsRequest struct {
	CollectionID catalog.UniqueID
	PartitionID  catalog.UniqueID
}

type Snapshot struct {
	CollectionID catalog.UniqueID
	Timestamp    catalog.Timestamp
	Segments     []*datapb.SegmentInfo
}

type Manifest struct {
	CollectionID catalog.UniqueID
	PartitionID  catalog.UniqueID
	SegmentID    catalog.UniqueID
	Files        []string
}

type SnapshotCatalog interface {
	Get(ctx context.Context, req GetSnapshotRequest, opts catalog.ReadOptions) (*Snapshot, error)
	ListManifests(ctx context.Context, req ListManifestsRequest, opts catalog.ReadOptions) ([]*Manifest, error)
	Save(ctx context.Context, snapshot *datapb.SnapshotInfo, opts catalog.WriteOptions) error
	Drop(ctx context.Context, collectionID catalog.UniqueID, snapshotID catalog.UniqueID, opts catalog.WriteOptions) error
	List(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.SnapshotInfo, error)
}

type MilvusStateCatalog interface {
	DataCoord() DataCoordStateCatalog
	QueryCoord() QueryCoordStateCatalog
	Streaming() StreamingStateCatalog
}

type DataCoordStateCatalog interface {
	MarkChannelAdded(ctx context.Context, channel string, opts catalog.WriteOptions) error
	MarkChannelDeleted(ctx context.Context, channel string, opts catalog.WriteOptions) error
	ShouldDropChannel(ctx context.Context, channel string, opts catalog.ReadOptions) (bool, error)
	ChannelExists(ctx context.Context, channel string, opts catalog.ReadOptions) (bool, error)
	DropChannel(ctx context.Context, channel string, opts catalog.WriteOptions) error
	SaveImportJob(ctx context.Context, job *datapb.ImportJob, opts catalog.WriteOptions) error
	ListImportJobs(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ImportJob, error)
	DropImportJob(ctx context.Context, jobID int64, opts catalog.WriteOptions) error
	SavePreImportTask(ctx context.Context, task *datapb.PreImportTask, opts catalog.WriteOptions) error
	ListPreImportTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.PreImportTask, error)
	DropPreImportTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error
	SaveImportTask(ctx context.Context, task *datapb.ImportTaskV2, opts catalog.WriteOptions) error
	ListImportTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ImportTaskV2, error)
	DropImportTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error
	SaveCopySegmentJob(ctx context.Context, job *datapb.CopySegmentJob, opts catalog.WriteOptions) error
	ListCopySegmentJobs(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.CopySegmentJob, error)
	DropCopySegmentJob(ctx context.Context, jobID int64, opts catalog.WriteOptions) error
	SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask, opts catalog.WriteOptions) error
	SaveCopySegmentTasksBatch(ctx context.Context, tasks []*datapb.CopySegmentTask, opts catalog.WriteOptions) error
	ListCopySegmentTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.CopySegmentTask, error)
	DropCopySegmentTask(ctx context.Context, taskID int64, opts catalog.WriteOptions) error
	GcConfirm(ctx context.Context, collectionID catalog.UniqueID, partitionID catalog.UniqueID, opts catalog.ReadOptions) (bool, error)
	SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask, opts catalog.WriteOptions) error
	ListCompactionTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.CompactionTask, error)
	DropCompactionTask(ctx context.Context, task *datapb.CompactionTask, opts catalog.WriteOptions) error
	SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask, opts catalog.WriteOptions) error
	ListAnalyzeTasks(ctx context.Context, opts catalog.ReadOptions) ([]*indexpb.AnalyzeTask, error)
	DropAnalyzeTask(ctx context.Context, taskID catalog.UniqueID, opts catalog.WriteOptions) error
	SaveStatsTask(ctx context.Context, task *indexpb.StatsTask, opts catalog.WriteOptions) error
	ListStatsTasks(ctx context.Context, opts catalog.ReadOptions) ([]*indexpb.StatsTask, error)
	DropStatsTask(ctx context.Context, taskID catalog.UniqueID, opts catalog.WriteOptions) error
	SaveChannelCheckpoint(ctx context.Context, vchannel string, position *msgpb.MsgPosition, opts catalog.WriteOptions) error
	SaveChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition, opts catalog.WriteOptions) error
	ListChannelCheckpoints(ctx context.Context, opts catalog.ReadOptions) (map[string]*msgpb.MsgPosition, error)
	DropChannelCheckpoint(ctx context.Context, vchannel string, opts catalog.WriteOptions) error
	ListPartitionStatsInfos(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.PartitionStatsInfo, error)
	SavePartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo, opts catalog.WriteOptions) error
	DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo, opts catalog.WriteOptions) error
	SaveCurrentPartitionStatsVersion(ctx context.Context, collID catalog.UniqueID, partID catalog.UniqueID, vchannel string, currentVersion int64, opts catalog.WriteOptions) error
	GetCurrentPartitionStatsVersion(ctx context.Context, collID catalog.UniqueID, partID catalog.UniqueID, vchannel string, opts catalog.ReadOptions) (int64, error)
	DropCurrentPartitionStatsVersion(ctx context.Context, collID catalog.UniqueID, partID catalog.UniqueID, vchannel string, opts catalog.WriteOptions) error
	ListExternalCollectionRefreshJobs(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ExternalCollectionRefreshJob, error)
	SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob, opts catalog.WriteOptions) error
	DropExternalCollectionRefreshJob(ctx context.Context, jobID catalog.UniqueID, opts catalog.WriteOptions) error
	ListExternalCollectionRefreshTasks(ctx context.Context, opts catalog.ReadOptions) ([]*datapb.ExternalCollectionRefreshTask, error)
	SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask, opts catalog.WriteOptions) error
	DropExternalCollectionRefreshTask(ctx context.Context, taskID catalog.UniqueID, opts catalog.WriteOptions) error
}

type QueryCoordStateCatalog interface {
	SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions []*querypb.PartitionLoadInfo, opts catalog.WriteOptions) error
	SavePartition(ctx context.Context, partitions []*querypb.PartitionLoadInfo, opts catalog.WriteOptions) error
	SaveReplica(ctx context.Context, replicas []*querypb.Replica, opts catalog.WriteOptions) error
	GetCollections(ctx context.Context, opts catalog.ReadOptions) ([]*querypb.CollectionLoadInfo, error)
	GetPartitions(ctx context.Context, collectionIDs []int64, opts catalog.ReadOptions) (map[int64][]*querypb.PartitionLoadInfo, error)
	GetReplicas(ctx context.Context, opts catalog.ReadOptions) ([]*querypb.Replica, error)
	ReleaseCollection(ctx context.Context, collectionID catalog.UniqueID, opts catalog.WriteOptions) error
	ReleasePartition(ctx context.Context, collectionID catalog.UniqueID, partitionIDs []catalog.UniqueID, opts catalog.WriteOptions) error
	ReleaseReplicas(ctx context.Context, collectionID catalog.UniqueID, opts catalog.WriteOptions) error
	ReleaseReplica(ctx context.Context, collectionID catalog.UniqueID, replicaIDs []catalog.UniqueID, opts catalog.WriteOptions) error
	SaveResourceGroup(ctx context.Context, groups []*querypb.ResourceGroup, opts catalog.WriteOptions) error
	RemoveResourceGroup(ctx context.Context, rgName string, opts catalog.WriteOptions) error
	GetResourceGroups(ctx context.Context, opts catalog.ReadOptions) ([]*querypb.ResourceGroup, error)
	SaveCollectionTargets(ctx context.Context, targets []*querypb.CollectionTarget, opts catalog.WriteOptions) error
	RemoveCollectionTarget(ctx context.Context, collectionID catalog.UniqueID, opts catalog.WriteOptions) error
	RemoveCollectionTargets(ctx context.Context, opts catalog.WriteOptions) error
	GetCollectionTargets(ctx context.Context, opts catalog.ReadOptions) (map[int64]*querypb.CollectionTarget, error)
}

type StreamingStateCatalog interface {
	GetCChannel(ctx context.Context, opts catalog.ReadOptions) (*streamingpb.CChannelMeta, error)
	SaveCChannel(ctx context.Context, channel *streamingpb.CChannelMeta, opts catalog.WriteOptions) error
	GetVersion(ctx context.Context, opts catalog.ReadOptions) (*streamingpb.StreamingVersion, error)
	SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion, opts catalog.WriteOptions) error
	ListPChannels(ctx context.Context, opts catalog.ReadOptions) ([]*streamingpb.PChannelMeta, error)
	SavePChannels(ctx context.Context, channels []*streamingpb.PChannelMeta, opts catalog.WriteOptions) error
	ListBroadcastTasks(ctx context.Context, opts catalog.ReadOptions) ([]*streamingpb.BroadcastTask, error)
	SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask, opts catalog.WriteOptions) error
	SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta, opts catalog.WriteOptions) error
	GetReplicateConfiguration(ctx context.Context, opts catalog.ReadOptions) (*streamingpb.ReplicateConfigurationMeta, error)
	ListVChannels(ctx context.Context, pchannel string, opts catalog.ReadOptions) ([]*streamingpb.VChannelMeta, error)
	SaveVChannels(ctx context.Context, pchannel string, channels map[string]*streamingpb.VChannelMeta, opts catalog.WriteOptions) error
	ListSegmentAssignments(ctx context.Context, pchannel string, opts catalog.ReadOptions) ([]*streamingpb.SegmentAssignmentMeta, error)
	SaveSegmentAssignments(ctx context.Context, pchannel string, assignments map[int64]*streamingpb.SegmentAssignmentMeta, opts catalog.WriteOptions) error
	GetConsumeCheckpoint(ctx context.Context, pchannel string, opts catalog.ReadOptions) (*streamingpb.WALCheckpoint, error)
	SaveConsumeCheckpoint(ctx context.Context, pchannel string, checkpoint *streamingpb.WALCheckpoint, opts catalog.WriteOptions) error
	SaveSalvageCheckpoint(ctx context.Context, pchannel string, checkpoint *commonpb.ReplicateCheckpoint, opts catalog.WriteOptions) error
	GetSalvageCheckpoint(ctx context.Context, pchannel string, opts catalog.ReadOptions) ([]*commonpb.ReplicateCheckpoint, error)
}
