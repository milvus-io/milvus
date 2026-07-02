// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metastore

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pkgmetastore "github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RootCoordCatalog + AlterType were moved to the shared pkg/v3/metastore module
// (so the pooled catalog service can depend on the rootcoord contract); they are
// re-exported here to keep the internal import path working unchanged. The other
// coord *Catalog contracts stay defined here until each coord's own move.
type (
	RootCoordCatalog = pkgmetastore.RootCoordCatalog
	AlterType        = pkgmetastore.AlterType
)

const (
	ADD    = pkgmetastore.ADD
	DELETE = pkgmetastore.DELETE
	MODIFY = pkgmetastore.MODIFY
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

	// Analyzer Resource
	SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error
	RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error
	ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error)
	// snapshot related
	SaveSnapshot(ctx context.Context, snapshot *datapb.SnapshotInfo) error
	DropSnapshot(ctx context.Context, collectionID int64, snapshotID int64) error
	ListSnapshots(ctx context.Context) ([]*datapb.SnapshotInfo, error)
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
	RemoveCollectionTargets(ctx context.Context) error
	GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error)
}

// StreamingCoordCataLog is the interface for streamingcoord catalog
// All write operation of catalog is reliable, the error will only be returned if the ctx is canceled,
// otherwise it will retry until success.
type StreamingCoordCataLog interface {
	// GetCChannel get the control channel from metastore.
	GetCChannel(ctx context.Context) (*streamingpb.CChannelMeta, error)

	// SaveCChannel save the control channel to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveCChannel(ctx context.Context, info *streamingpb.CChannelMeta) error

	// GetVersion get the streaming version from metastore.
	GetVersion(ctx context.Context) (*streamingpb.StreamingVersion, error)

	// SaveVersion save the streaming version to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion) error

	// physical channel watch related

	// ListPChannel list all pchannels on milvus.
	ListPChannel(ctx context.Context) ([]*streamingpb.PChannelMeta, error)

	// SavePChannel save a pchannel info to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SavePChannels(ctx context.Context, info []*streamingpb.PChannelMeta) error

	// ListBroadcastTask list all broadcast tasks.
	// Used to recovery the broadcast tasks.
	ListBroadcastTask(ctx context.Context) ([]*streamingpb.BroadcastTask, error)

	// SaveBroadcastTask save the broadcast task to metastore.
	// Make the task recoverable after restart.
	// When broadcast task is done, it will be removed from metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask) error

	// SaveReplicateConfiguration saves the replicate configuration to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error

	// GetReplicateConfiguration gets the replicate configuration from metastore.
	GetReplicateConfiguration(ctx context.Context) (*streamingpb.ReplicateConfigurationMeta, error)
}

// StreamingNodeCataLog is the interface for streamingnode catalog
type StreamingNodeCataLog interface {
	// WAL select the wal related recovery infos.
	// Which must give the pchannel name.

	// ListVChannel list all vchannels on current pchannel.
	ListVChannel(ctx context.Context, pchannelName string) ([]*streamingpb.VChannelMeta, error)

	// SaveVChannels save vchannel on current pchannel.
	SaveVChannels(ctx context.Context, pchannelName string, vchannels map[string]*streamingpb.VChannelMeta) error

	// ListSegmentAssignment list all segment assignments for the wal.
	ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error)

	// SaveSegmentAssignments save the segment assignments for the wal.
	SaveSegmentAssignments(ctx context.Context, pChannelName string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error

	// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
	// Return nil, nil if the checkpoint is not exist.
	GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error)

	// SaveConsumeCheckpoint saves the consuming checkpoint of the wal.
	SaveConsumeCheckpoint(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint) error

	// SaveSalvageCheckpoint saves the salvage checkpoint.
	// The checkpoint is captured during force promote.
	SaveSalvageCheckpoint(ctx context.Context, pChannelName string, checkpoint *commonpb.ReplicateCheckpoint) error

	// GetSalvageCheckpoint gets all salvage checkpoints for a channel.
	// Returns an empty slice if none exist. One checkpoint per source cluster.
	GetSalvageCheckpoint(ctx context.Context, pChannelName string) ([]*commonpb.ReplicateCheckpoint, error)
}
