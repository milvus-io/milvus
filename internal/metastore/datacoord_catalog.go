package metastore

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BinlogsIncrement struct {
	Segment    *datapb.SegmentInfo
	UpdateMask BinlogsUpdateMask
	// DroppedBinlogFieldIDs lists FieldBinlog.FieldID values whose insert-binlog
	// KV entries must be removed from etcd. AlterSegments only upserts; without
	// explicit removal, a prefix scan in listBinlogs will resurrect the zombie
	// entry on restart. Used by operators (e.g. V2 column-group backfill commit)
	// that structurally drop a FieldBinlog from segment.Binlogs.
	DroppedBinlogFieldIDs []int64
}

type BinlogsUpdateMask struct {
	WithoutBinlogs       bool // if true, the binlogs will not be updated
	WithoutDeltalogs     bool // if true, the deltalogs will not be updated
	WithoutStatslogs     bool // if true, the statslogs will not be updated
	WithoutBm25Statslogs bool // if true, the bm25 statslogs will not be updated
}

func (m *BinlogsIncrement) GetUpdateBinlogs() []*datapb.FieldBinlog {
	if m.UpdateMask.WithoutBinlogs {
		return nil
	}
	return m.cloneBinlogs(m.Segment.GetBinlogs())
}

func (m *BinlogsIncrement) GetUpdateDeltalogs() []*datapb.FieldBinlog {
	if m.UpdateMask.WithoutDeltalogs {
		return nil
	}
	return m.cloneBinlogs(m.Segment.GetDeltalogs())
}

func (m *BinlogsIncrement) GetUpdateStatslogs() []*datapb.FieldBinlog {
	if m.UpdateMask.WithoutStatslogs {
		return nil
	}
	return m.cloneBinlogs(m.Segment.GetStatslogs())
}

func (m *BinlogsIncrement) GetUpdateBm25Statslogs() []*datapb.FieldBinlog {
	if m.UpdateMask.WithoutBm25Statslogs {
		return nil
	}
	return m.cloneBinlogs(m.Segment.GetBm25Statslogs())
}

func (m *BinlogsIncrement) cloneBinlogs(binlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	res := make([]*datapb.FieldBinlog, len(binlogs))
	for i, binlog := range binlogs {
		res[i] = proto.Clone(binlog).(*datapb.FieldBinlog)
	}
	return res
}

//go:generate mockery --name=DataCoordCatalog --with-expecter
type DataCoordCatalog interface {
	ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error)
	AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error
	// TODO Remove this later, we should update flush segments info for each segment separately, so far we still need transaction
	AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...BinlogsIncrement) error
	SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error
	DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error

	// TODO: From MarkChannelAdded to DropChannel, it's totally a redundant design by now, remove it in future.
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
	ListSegmentIndexes(ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error)
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

	SaveCopySegmentJob(ctx context.Context, job *datapb.CopySegmentJob) error
	ListCopySegmentJobs(ctx context.Context) ([]*datapb.CopySegmentJob, error)
	DropCopySegmentJob(ctx context.Context, jobID int64) error

	SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask) error
	SaveCopySegmentTasksBatch(ctx context.Context, tasks []*datapb.CopySegmentTask) error
	ListCopySegmentTasks(ctx context.Context) ([]*datapb.CopySegmentTask, error)
	DropCopySegmentTask(ctx context.Context, taskID int64) error

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

	// External Collection Refresh - Separated Job/Task storage
	ListExternalCollectionRefreshJobs(ctx context.Context) ([]*datapb.ExternalCollectionRefreshJob, error)
	SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error
	DropExternalCollectionRefreshJob(ctx context.Context, jobID typeutil.UniqueID) error
	ListExternalCollectionRefreshTasks(ctx context.Context) ([]*datapb.ExternalCollectionRefreshTask, error)
	SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask) error
	DropExternalCollectionRefreshTask(ctx context.Context, taskID typeutil.UniqueID) error
	GetExternalCollectionRefreshInfo(ctx context.Context, collectionID int64) (*datapb.ExternalCollectionRefreshInfo, error)
	SaveExternalCollectionRefreshInfo(ctx context.Context, info *datapb.ExternalCollectionRefreshInfo) error

	// Analyzer Resource
	SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error
	RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error
	ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error)
	// snapshot related
	SaveSnapshot(ctx context.Context, snapshot *datapb.SnapshotInfo) error
	DropSnapshot(ctx context.Context, collectionID int64, snapshotID int64) error
	ListSnapshots(ctx context.Context) ([]*datapb.SnapshotInfo, error)
}
