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

package copysegment

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	snapshotresolver "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	snapshotio "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRestoreL0MaterializationResumesFixedPlans(t *testing.T) {
	mockey.PatchConvey("durable per-partition coverage gates publication", t, func() {
		paramtable.Init()
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		source := &restoreSnapshotSource{}
		m := &meta.Meta{MetaStore: store, Ctx: ctx, Catalog: catalog, Segments: meta.NewSegmentsInfo(store)}
		taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
		require.NoError(t, err)
		job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
		job.CreatesCollection = true
		job.SnapshotCache = &CopySegmentSnapshotCache{}
		job.CollectionId = 200
		job.IdMappings = nil
		for _, s := range []*datapb.SegmentInfo{
			{ID: 1, Level: datapb.SegmentLevel_L0, PartitionID: -1, InsertChannel: "v1"},
			{ID: 2, Level: datapb.SegmentLevel_L1, PartitionID: 10, InsertChannel: "v1", CommitTimestamp: 300},
			{ID: 3, Level: datapb.SegmentLevel_L1, PartitionID: 20, InsertChannel: "v1"},
			{ID: 4, Level: datapb.SegmentLevel_L1, PartitionID: 10, InsertChannel: "v2"},
			{ID: 5, Level: datapb.SegmentLevel_L0, PartitionID: 10, InsertChannel: "v1"},
		} {
			s.CollectionID = 200
			s.State = commonpb.SegmentState_Flushed
			s.IsImporting = true
			m.Segments.SetSegment(s.ID, meta.NewSegmentInfo(s))
			job.IdMappings = append(job.IdMappings, &datapb.CopySegmentIDMapping{SourceSegmentId: s.ID + 100, TargetSegmentId: s.ID, PartitionId: s.PartitionID})
		}
		require.NoError(t, copies.AddJob(ctx, job))
		snapshot := &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "snapshot"}}}
		mockey.Mock((*restoreSnapshotSource).ReadSnapshotData).Return(snapshot, nil).Build()
		allocations := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(1000), int64(1002), nil).Build()
		mockey.Mock((*compaction.Inspector).HasCompactionTask).Return(true).Build()
		submissions := 0
		mockey.Mock((*compaction.Inspector).EnqueueCompaction).To(func(_ *compaction.Inspector, plan *datapb.CompactionTask) error {
			submissions++
			require.NotZero(t, copies.GetJob(ctx, 100).(*Job).GetRestoreCompactionPlans()["v1/-1"])
			return taskMeta.SaveCompactionTask(ctx, proto.Clone(plan).(*datapb.CompactionTask))
		}).Build()
		c := &Checker{CompactionTasks: taskMeta, SnapshotSource: source, Ctx: ctx, Meta: m, CopyMeta: copies, alloc: &restoreAllocatorTarget{}, Compactions: &compaction.Inspector{}}
		ready, err := c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		plans := taskMeta.GetCompactionTasksByTriggerID(100)
		require.Len(t, plans, 2)
		for _, plan := range plans {
			task := ctask.NewL0CompactionTask(plan, nil, m, taskMeta)
			task.LookupRestoreJob = func(ctx context.Context, id int64) *datapb.CopySegmentJob {
				return copies.GetJob(ctx, id).(*Job).CopySegmentJob
			}
			targets, binlogs, err := task.SelectFlushedSegment()
			require.NoError(t, err)
			ids := make([]int64, 0, len(targets))
			for _, segment := range targets {
				ids = append(ids, segment.GetID())
			}
			if plan.PartitionID == -1 {
				require.ElementsMatch(t, []int64{2, 3}, ids)
			} else {
				require.Equal(t, []int64{2}, ids)
			}
			require.EqualValues(t, 300, binlogs[0].CommitTimestamp)
		}
		// Restart between submission and completion: no duplicate plan allocation.
		copies, err = NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
		require.NoError(t, err)
		c.CopyMeta = copies
		taskMeta, err = ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		c.CompactionTasks = taskMeta
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		require.Equal(t, 1, allocations.Times())
		first := proto.Clone(plans[0]).(*datapb.CompactionTask)
		first.State = datapb.CompactionTaskState_cleaned
		first.TerminalState = datapb.CompactionTaskState_completed
		require.NoError(t, taskMeta.SaveCompactionTask(ctx, first))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		last := proto.Clone(plans[1]).(*datapb.CompactionTask)
		last.State = datapb.CompactionTaskState_cleaned
		last.TerminalState = datapb.CompactionTaskState_failed
		require.NoError(t, taskMeta.SaveCompactionTask(ctx, last))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 3, submissions, "failed cleaned attempt retries its stable plan")
		var retried *datapb.CompactionTask
		for _, plan := range taskMeta.GetCompactionTasksByTriggerID(100) {
			if plan.GetPlanID() == last.GetPlanID() {
				retried = plan
			}
		}
		require.NotNil(t, retried)
		require.Equal(t, datapb.CompactionTaskState_unknown, retried.GetTerminalState(), "a new attempt must not inherit the failed terminal state")
		retryTask := ctask.NewL0CompactionTask(retried, nil, m, taskMeta)
		require.NoError(t, retryTask.UpdateAndSaveTaskMeta(ctask.SetState(datapb.CompactionTaskState_meta_saved)))
		require.True(t, retryTask.Process())
		require.True(t, retryTask.Clean())
		taskMeta, err = ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		c.CompactionTasks = taskMeta
		copies, err = NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
		require.NoError(t, err)
		c.CopyMeta = copies
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.True(t, ready)
		require.True(t, m.GetSegment(ctx, 2).GetIsImporting(), "materialization does not itself publish data")
	})
}

func TestRestoreL0RequiresSuccessfulTerminalState(t *testing.T) {
	for _, test := range []struct {
		name     string
		state    datapb.CompactionTaskState
		terminal datapb.CompactionTaskState
		ready    bool
		retry    bool
	}{
		{name: "legacy cleaned", state: datapb.CompactionTaskState_cleaned, retry: true},
		{name: "cleaned failure", state: datapb.CompactionTaskState_cleaned, terminal: datapb.CompactionTaskState_failed, retry: true},
		{name: "cleaned timeout", state: datapb.CompactionTaskState_cleaned, terminal: datapb.CompactionTaskState_timeout, retry: true},
		{name: "meta saved", state: datapb.CompactionTaskState_meta_saved},
		{name: "failed before cleanup", state: datapb.CompactionTaskState_failed, terminal: datapb.CompactionTaskState_failed},
		{name: "completed", state: datapb.CompactionTaskState_completed, terminal: datapb.CompactionTaskState_completed, ready: true},
		{name: "cleaned success", state: datapb.CompactionTaskState_cleaned, terminal: datapb.CompactionTaskState_completed, ready: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mockey.PatchConvey("only a durable success permits restore publication", t, func() {
				ctx := t.Context()
				catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
				store := metacache.NewMetaStore(nil)
				m := &meta.Meta{MetaStore: store, Ctx: ctx, Catalog: catalog, Segments: meta.NewSegmentsInfo(store)}
				taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
				require.NoError(t, err)
				copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil, nil)
				require.NoError(t, err)
				job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
				job.CollectionId = 200
				job.CreatesCollection = true
				job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}}
				job.RestoreCompactionPlans = map[string]int64{"v1/-1": 77}
				require.NoError(t, copies.AddJob(ctx, job))
				m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 200, InsertChannel: "v1", PartitionID: -1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, IsImporting: true}))
				require.NoError(t, taskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{PlanID: 77, TriggerID: 100, Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, State: test.state, TerminalState: test.terminal}))
				mockey.Mock(loadCopySnapshot).Return(&snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{}}, nil).Build()
				submit := mockey.Mock((*compaction.Inspector).EnqueueCompaction).To(func(_ *compaction.Inspector, plan *datapb.CompactionTask) error {
					require.EqualValues(t, 77, plan.GetPlanID())
					require.Equal(t, datapb.CompactionTaskState_unknown, plan.GetTerminalState())
					return taskMeta.SaveCompactionTask(ctx, plan)
				}).Build()
				checker := &Checker{Ctx: ctx, Meta: m, CopyMeta: copies, Compactions: &compaction.Inspector{}, CompactionTasks: taskMeta}
				ready, err := checker.materializeRestoreDeletes(copies.GetJob(ctx, 100))
				require.NoError(t, err)
				require.Equal(t, test.ready, ready)
				require.Equal(t, test.retry, submit.Times() != 0)
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(ctx, 100).GetState())
				require.True(t, m.GetSegment(ctx, 1).GetIsImporting())
			})
		})
	}
}

func TestRestoreL0AdmissionRetry(t *testing.T) {
	mockey.PatchConvey("rejected admission reuses its durable plan ID", t, func() {
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		source := &restoreSnapshotSource{}
		m := &meta.Meta{MetaStore: store, Ctx: ctx, Catalog: catalog, Segments: meta.NewSegmentsInfo(store)}
		taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil, nil)
		require.NoError(t, err)
		job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
		job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}}
		require.NoError(t, copies.AddJob(ctx, job))
		c := &Checker{CompactionTasks: taskMeta, SnapshotSource: source, Ctx: ctx, Meta: m, CopyMeta: copies, alloc: &restoreAllocatorTarget{}}
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L1}))
		ready, err := c.materializeRestoreDeletes(job)
		require.NoError(t, err)
		require.True(t, ready, "a snapshot without L0 needs no materialization")
		m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L0, InsertChannel: "v1"}))
		snapshotRead := mockey.Mock(loadCopySnapshot).Return(nil, merr.ErrServiceUnavailable).Build()
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		snapshotRead.UnPatch()
		mockey.Mock(loadCopySnapshot).Return(&snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{}}, nil).Build()
		allocation := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(0), int64(0), merr.ErrServiceUnavailable).Build()
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		allocation.UnPatch()
		allocation = mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(1000), int64(1001), nil).Build()
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable, "plan identity persists before scheduler becomes available")
		c.Compactions = &compaction.Inspector{}
		tracked := mockey.Mock((*compaction.Inspector).HasCompactionTask).Return(false).Build()
		rejected := true
		submissions := 0
		mockey.Mock((*compaction.Inspector).EnqueueCompaction).To(func(_ *compaction.Inspector, plan *datapb.CompactionTask) error {
			submissions++
			require.EqualValues(t, 1000, plan.PlanID)
			require.NoError(t, taskMeta.SaveCompactionTask(ctx, proto.Clone(plan).(*datapb.CompactionTask)))
			if rejected {
				return merr.ErrServiceUnavailable
			}
			return nil
		}).Build()
		_, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		rejected = false
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		require.Equal(t, 1, allocation.Times())
		require.Equal(t, 1, tracked.Times())
	})
}

func TestRestoreSegmentRetainsEffectiveInsertTime(t *testing.T) {
	source := &datapb.SegmentDescription{
		SegmentId: 1, SegmentLevel: datapb.SegmentLevel_L1, CommitTimestamp: 300,
		StartPosition: &msgpb.MsgPosition{Timestamp: 100, ChannelName: "source"}, DmlPosition: &msgpb.MsgPosition{Timestamp: 100},
	}
	data, err := proto.Marshal(source)
	require.NoError(t, err)
	decoded := &datapb.SegmentDescription{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	target := RestoredSegmentInfo(decoded, 20, 30, 40, "target", 500)
	require.EqualValues(t, 300, meta.SegmentEffectiveTs(target.SegmentInfo))
	require.EqualValues(t, 300, target.GetCommitTimestamp())
	require.EqualValues(t, 100, target.GetStartPosition().GetTimestamp())
	require.EqualValues(t, 500, target.GetTransformStartAfterTimetick())
	require.Equal(t, "source", source.GetStartPosition().GetChannelName())
}

func TestLoadCopySnapshotPreservesLegacyFingerprintAfterRestart(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		version int64
		origin  string
	}{{2, "source"}, {3, "source"}, {2, "old"}, {3, "old"}} {
		version, origin := tc.version, tc.origin
		t.Run(fmt.Sprintf("v%d/origin_%s", version, origin), func(t *testing.T) {
			ctx := t.Context()
			uri := "s3://source/root/snapshots/1/metadata/2.json"
			manifest := "s3://source/root/snapshots/1/manifests/20.avro"
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: 6, Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
				SnapshotInfo: &datapb.SnapshotInfo{Id: 2, CollectionId: 1, S3Location: "s3://" + origin + "/root/snapshots/1/metadata/2.json"},
				Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{}},
				ManifestList: []string{manifest}, SegmentIds: []int64{20},
			}
			segment := &datapb.SegmentDescription{
				SegmentId: 20, StorageVersion: version, SegmentLevel: datapb.SegmentLevel_L1,
				Statslogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "s3://source/root/files/stats"}}}},
			}
			if version == 2 {
				segment.Binlogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "s3://source/root/files/insert"}}}}
			} else {
				segment.ManifestPath = packed.MarshalManifestPath("s3://source/root/files/segment", 7)
				metadata.Storagev2ManifestList = []*datapb.StorageV2SegmentManifest{{SegmentId: 20, Manifest: segment.ManifestPath}}
			}
			raw, err := protojson.Marshal(metadata)
			require.NoError(t, err)
			avro, err := snapshotio.MarshalSegmentManifest(segment)
			require.NoError(t, err)
			cm := storage.NewLocalChunkManager()
			read := mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ *storage.LocalChunkManager, _ context.Context, key string) ([]byte, error) {
				switch key {
				case "root/snapshots/1/metadata/2.json":
					return raw, nil
				case "root/snapshots/1/manifests/20.avro":
					return avro, nil
				default:
					return nil, merr.WrapErrDataIntegrityMsg("unexpected object key: %s", key)
				}
			}).Build()
			defer read.UnPatch()
			reader := snapshotstorage.NewSnapshotReader(cm)
			// Legacy same-prefix reads retained destination URI fields even with an old origin.
			legacySegment, err := reader.ReadSegmentManifest(ctx, manifest, 6)
			require.NoError(t, err)
			legacySegment.ManifestPath = segment.ManifestPath
			legacy := &snapshotstorage.SnapshotData{
				SnapshotInfo: proto.Clone(metadata.SnapshotInfo).(*datapb.SnapshotInfo), Collection: metadata.Collection,
				Segments: []*datapb.SegmentDescription{legacySegment}, ManifestPaths: metadata.ManifestList,
				SegmentIDs: metadata.SegmentIds, Layout: metadata.Layout,
			}
			legacy.SnapshotInfo.S3Location = uri
			fingerprint, err := snapshotstorage.SnapshotFingerprint(legacy)
			require.NoError(t, err)
			catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			require.NoError(t, catalog.SaveCopySegmentJob(ctx, &datapb.CopySegmentJob{
				JobId: 100, External: true, SnapshotS3Location: uri, SnapshotFingerprint: fingerprint,
				State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			}))
			resolve := mockey.Mock(snapshotresolver.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm}, nil).Build()
			defer resolve.UnPatch()
			source := &restoreSnapshotSource{}
			externalRead := mockey.Mock((*restoreSnapshotSource).ReadExternalSnapshotDataWithChunkManager).To(
				func(_ *restoreSnapshotSource, ctx context.Context, cm snapshotstorage.ChunkManager, location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
					return snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, location, includeSegments)
				}).Build()
			defer externalRead.UnPatch()
			restored, err := NewCopySegmentMeta(ctx, catalog, nil, nil, source, nil)
			require.NoError(t, err)
			job := restored.GetJob(ctx, 100)
			require.Nil(t, job.(*Job).SnapshotCache.data)
			got, err := loadCopySnapshot(ctx, job, source)
			require.NoError(t, err)
			require.True(t, proto.Equal(legacySegment, got.Segments[0]))

			// Real content changes must still fail after another restart.
			segment.NumOfRows = 42
			avro, err = snapshotio.MarshalSegmentManifest(segment)
			require.NoError(t, err)
			restored, err = NewCopySegmentMeta(ctx, catalog, nil, nil, source, nil)
			require.NoError(t, err)
			_, err = loadCopySnapshot(ctx, restored.GetJob(ctx, 100), source)
			require.ErrorContains(t, err, "external snapshot changed after restore job creation")
		})
	}
}
