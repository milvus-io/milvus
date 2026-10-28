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

package datacoord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentChangeMixSortChain(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "rows", true: "empty"}[empty], func(t *testing.T) {
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableCompaction, "true")
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableSortCompaction, "true")
			m, task, result := seedMixProducer(t)
			result.Segments[0].IsSorted = false
			_, err := m.CompleteCompactionMutation(t.Context(), task, result)
			require.NoError(t, err)
			inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
			inspector.checkAllGroups(t.Context())
			require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
			// Existing groups must finish sorting after opt-out.
			require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
			ci := &compactionInspector{meta: m, allocator: newMockAllocator(t)}
			sortPlan := proto.Clone(task).(*datapb.CompactionTask)
			sortPlan.PlanID = 5001
			sortPlan.Type = datapb.CompactionType_SortCompaction
			sortPlan.InputSegments = []int64{1001}
			require.True(t, m.groupSortCandidates(10)[1001])
			sortTask, err := ci.createCompactTask(sortPlan)
			require.NoError(t, err)
			sortResult := proto.Clone(result).(*datapb.CompactionPlanResult)
			sortResult.PlanID = 5001
			sortResult.Segments[0].SegmentID = 1101
			sortResult.Segments[0].IsSorted = true
			if empty {
				sortResult.Segments[0].NumOfRows = 0
				sortResult.Segments[0].InsertLogs = nil
			}
			_, err = m.CompleteCompactionMutation(t.Context(), sortPlan, sortResult)
			require.NoError(t, err)
			g := m.GetSegmentChangeGroup(t.Context(), 10, 5000)
			require.Equal(t, []int64{1001}, g.InitialSegmentIDs)
			require.Equal(t, []int64{1101}, g.NewSegmentIDs)
			require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(t.Context(), 1001).GetState())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
			_, err = m.CompleteCompactionMutation(t.Context(), task, result)
			require.NoError(t, err, "root replay must use the original result identity")
			_, err = m.CompleteCompactionMutation(t.Context(), sortPlan, sortResult)
			require.NoError(t, err, "sort replay must use its durable group lineage")
			sortTask.QueryTaskOnWorker(session.NewMockCluster(t))
			require.True(t, sortTask.Process(), "materialized inner sorts release the planning budget before group publication")
			require.True(t, sortTask.Clean())
			require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
			forged := g.Clone()
			forged.NewSegmentIDs = []int64{9999}
			require.ErrorIs(t, m.UpdateSegmentChangeGroup(t.Context(), forged), merr.ErrDataIntegrity)
			// Reload the real catalog before publishing the replacement output.
			recovered := reloadSegmentChangeMeta(t, m)
			restored, err := (&compactionInspector{meta: recovered, allocator: newMockAllocator(t)}).createCompactTask(sortPlan)
			require.NoError(t, err)
			restored.QueryTaskOnWorker(session.NewMockCluster(t))
			inspector = newSegmentChangeInspector(t.Context(), recovered, newMockAllocator(t))
			inspector.checkAllGroups(t.Context())
			inspector.checkAllGroups(t.Context())
			require.Equal(t, model.SegmentChangeStateCommitted, recovered.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
			require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(t.Context(), 2001).GetState())
			require.True(t, restored.Process())
		})
	}
}

func TestSegmentChangeImportSubgroupsWaitForFence(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeGroupMaxMembers, "1")
	m, _, _ := seedMixProducer(t)
	for _, id := range []int64{1001, 1002} {
		require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed,
			IsImporting: true, IsSorted: true, NumOfRows: 10, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}},
		})))
	}
	s := &Server{meta: m}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 8000, CollectionID: 10, Vchannels: []string{"ch-1"}}}
	groups, handled, err := s.stageImportGroups(t.Context(), job, "ch-1", []int64{1001, 1002}, 0, model.SegmentChangeSourceImportJob)
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, groups, 2)
	require.Equal(t, groups[0].GroupID, groups[1].PreviousGroupID)
	sortTask := newMixCompactionTask(&datapb.CompactionTask{
		PlanID: 9001, CollectionID: 10, PartitionID: 100, Channel: "ch-1", Type: datapb.CompactionType_SortCompaction,
		InputSegments: []int64{1001}, Schema: readinessVectorSchema(), CollectionTtl: int64(time.Hour),
	}, newMockAllocator(t), m, newMockVersionManager())
	plan, err := sortTask.BuildCompactionRequest()
	require.NoError(t, err)
	require.Zero(t, plan.GetCollectionTtl(), "preparation must not expire rows using provisional import timestamps")
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	for _, g := range groups {
		require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(t.Context(), 10, g.GroupID).State)
		require.True(t, m.GetSegment(t.Context(), g.NewSegmentIDs[0]).GetIsInvisible())
	}
	// The preparation survives restart and opt-out without publishing before WAL.
	recovered := reloadSegmentChangeMeta(t, m)
	require.NoError(t, recovered.reconcileSegmentChangeGroups(t.Context(), nil, liveSegmentChangeCollection))
	require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
	s.meta = recovered
	groups, handled, err = s.stageImportGroups(t.Context(), job, "ch-1", nil, 9000, model.SegmentChangeSourceImportJob)
	require.True(t, handled)
	require.NoError(t, err)
	require.ErrorIs(t, recovered.publishGroup(t.Context(), groups[1], newMockAllocator(t)), merr.ErrServiceUnavailable)
	inspector = newSegmentChangeInspector(t.Context(), recovered, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	for _, g := range groups {
		require.Equal(t, model.SegmentChangeStateCommitted, recovered.GetSegmentChangeGroup(t.Context(), 10, g.GroupID).State)
		require.Equal(t, uint64(9000), recovered.GetSegment(t.Context(), g.NewSegmentIDs[0]).GetCommitTimestamp())
	}
}

func TestSegmentChangeImportCommittedFenceCannotBeAbandoned(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, _, _ := seedMixProducer(t)
	require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed, IsImporting: true, IsSorted: true, NumOfRows: 10, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}}})))
	s := &Server{meta: m}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 8000, CollectionID: 10}}
	_, err := s.stageImportChange(t.Context(), job, "ch-1", []int64{1001}, 9000)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	g := m.producerChangeGroup(model.SegmentChangeSourceImportJob, 8000, "ch-1")
	g.CreateTS = time.Now().Add(-2 * time.Hour).Unix()
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	require.False(t, inspector.stagingTimedOut(g))
	aborted := g.Clone()
	aborted.State = model.SegmentChangeStateAborted
	require.ErrorIs(t, m.UpdateSegmentChangeGroup(t.Context(), aborted), merr.ErrDataIntegrity)
	seg := m.GetSegment(t.Context(), 1001)
	require.NoError(t, m.catalog.DropSegment(t.Context(), seg.SegmentInfo))
	recovered := reloadSegmentChangeMeta(t, m)
	require.ErrorIs(t, recovered.reconcileSegmentChangeGroups(t.Context(), nil, liveSegmentChangeCollection), merr.ErrDataIntegrity)
}

func TestSegmentChangeImportCallbackPublishesPreparedGroups(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	callbacks, _, result := newImportCommitCallbackTest(t)
	m, _, _ := seedMixProducer(t)
	m.dataViewManager = nil
	m.AddCollection(&collectionInfo{ID: 100, Schema: readinessVectorSchema(), VChannelNames: []string{"v1", "v2"}})
	for id, channel := range map[int64]string{10: "v1", 11: "v1", 20: "v2"} {
		require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 100, PartitionID: 1, InsertChannel: channel, State: commonpb.SegmentState_Flushed, IsImporting: true, IsSorted: true, NumOfRows: 10,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: id, TimestampTo: 100}}}},
		})))
	}
	callbacks.meta = m
	job := callbacks.importMeta.GetJob(t.Context(), 1)
	require.NoError(t, callbacks.prepareImportPublication(t.Context(), job))
	for _, g := range m.GetSegmentChangeGroupsByCollection(t.Context(), 100) {
		require.True(t, g.AwaitCommitFence)
		require.Zero(t, g.CommitTS)
	}
	require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		err = callbacks.commitImportV2AckCallback(t.Context(), result)
		if err == nil {
			break
		}
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		inspector.checkAllGroups(t.Context())
		inspector.checkAllGroups(t.Context())
	}
	require.NoError(t, err)
	require.Equal(t, internalpb.ImportJobState_Completed, callbacks.importMeta.GetJob(t.Context(), 1).GetState())
	for id, ts := range map[int64]uint64{10: 200, 11: 200, 20: 300} {
		require.Equal(t, ts, m.GetSegment(t.Context(), id).GetCommitTimestamp())
		require.False(t, m.GetSegment(t.Context(), id).GetIsInvisible())
	}
}

func TestSegmentChangeSchemaBumpReplacement(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	task.Type = datapb.CompactionType_BumpSchemaVersionCompaction
	task.InputSegments = []int64{2001}
	task.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 1101, End: 1102}
	result.Segments[0].SegmentID = 1101
	result.Segments[0].StorageVersion = 3
	result.Segments[0].Manifest = packed.MarshalManifestPath("/private/tmp/bump/10/100/1101", 1)
	bt := newBumpSchemaVersionTask(task, newMockAllocator(t), m, nil)
	require.NoError(t, bt.saveSegmentMeta(result))
	require.False(t, bt.Process())
	require.True(t, m.GetSegment(t.Context(), 1101).GetIsInvisible())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	require.True(t, bt.Process())
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(t.Context(), 2001).GetState())
	require.False(t, m.GetSegment(t.Context(), 1101).GetIsInvisible())
}

func TestSegmentChangeSortRegistrationRecovery(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	result.Segments[0].IsSorted = false
	_, err := m.CompleteCompactionMutation(t.Context(), task, result)
	require.NoError(t, err)
	// A chunked registration persisted the new lineage but not the new member
	// or the retirement of its invisible sort input.
	g := m.GetSegmentChangeGroup(t.Context(), 10, 5000)
	g.NewSegmentIDs = []int64{1101}
	g.SortJobs = map[int64]model.SegmentChangeSortJob{5001: {InputSegmentID: 1001, OutputSegmentID: 1101}}
	require.NoError(t, m.catalog.Update(t.Context(), metastore.SaveSegmentChangeGroup(g)))
	recovered := reloadSegmentChangeMeta(t, m)
	require.NoError(t, recovered.reconcileSegmentChangeGroups(t.Context(), nil, liveSegmentChangeCollection))
	require.Equal(t, model.SegmentChangeStateFailed, recovered.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(t.Context(), 1001).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, recovered.GetSegment(t.Context(), 2001).GetState())
}

func TestSegmentChangeCopyPublication(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, _, result := seedMixProducer(t)
	require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Importing, IsImporting: true, IsSorted: true, NumOfRows: 84, CommitTimestamp: 7000,
	})))
	task := createTestCopyTask(10, 1001)
	copyMeta, err := NewCopySegmentMeta(t.Context(), m.catalog, m, nil, nil)
	require.NoError(t, err)
	require.NoError(t, copyMeta.AddTask(t.Context(), task))
	resp := &datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 1001, ImportedRows: 84, Binlogs: result.Segments[0].InsertLogs}}}
	require.NoError(t, SyncCopySegmentTask(task, resp, copyMeta, m))
	g := m.producerChangeGroup(model.SegmentChangeSourceCopySegment, task.GetTaskId(), "")
	require.NotNil(t, g)
	require.True(t, g.PreserveMemberTimestamps)
	require.True(t, m.GetSegment(t.Context(), 1001).GetIsInvisible())
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, copyMeta.GetTask(t.Context(), task.GetTaskId()).GetState())
	require.NoError(t, copyMeta.UpdateTask(t.Context(), task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress), updateCopyTaskCleanup(true)))
	ct := task.(*copySegmentTask)
	ct.CreateTaskOnWorker(1, session.NewMockCluster(t))
	ct.QueryTaskOnWorker(session.NewMockCluster(t))
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, ct.GetState(), "accepted files must not be copied again after worker/task-state loss")
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(t.Context(), 10, g.GroupID).State)
	require.Equal(t, uint64(7000), m.GetSegment(t.Context(), 1001).GetCommitTimestamp(), "file copying must preserve source row/delete timestamps")
	require.NoError(t, SyncCopySegmentTask(task, resp, copyMeta, m), "lost task-meta response must not reset published members")
	require.NoError(t, copyMeta.UpdateTask(t.Context(), task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), updateCopyTaskCleanup(true)))
	(&copySegmentInspector{ctx: t.Context(), meta: m}).processFailed(task)
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 1001).GetState(), "late task failure cannot withdraw a committed group")
	require.NoError(t, cleanupRejectedCopy(t.Context(), task, m, copyMeta))
	require.False(t, copyMeta.GetTask(t.Context(), task.GetTaskId()).GetCleanupRequired())
}

func TestSegmentChangeExternalRefreshMembership(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, _, _ := seedMixProducer(t)
	m.GetCollection(10).Schema = readinessExternalSchema()
	m.GetCollection(10).Partitions = []int64{100}
	incoming := &datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, NumOfRows: 84, StorageVersion: 3, ManifestPath: packed.MarshalManifestPath("/private/tmp/external/10/100/1001", 1),
		Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 1001, EntriesNum: 84}}}},
	}
	err := applyExternalCollectionSegmentUpdateWithGroup(t.Context(), m, 10, 8000, []int64{2001, 2002}, []int64{2002}, []*datapb.SegmentInfo{incoming})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, m.GetSegment(t.Context(), 1001).GetIsInvisible())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
	require.NoError(t, m.UpdateSegmentsInfo(t.Context(), func(pack *updateSegmentPack) bool {
		pack.Get(1001).JsonKeyStats = map[int64]*datapb.JsonKeyStats{2: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}}
		return true
	}))
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(t.Context(), 10, 8000).State)
	require.ElementsMatch(t, []int64{1001, 2002}, segmentChangeViewIDs(t, m))
	require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
	require.NoError(t, applyExternalCollectionSegmentUpdateWithGroup(t.Context(), m, 10, 8000, []int64{2001, 2002}, []int64{2002}, []*datapb.SegmentInfo{incoming}))
}

func TestSegmentChangeClusteringPublication(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	task.Type = datapb.CompactionType_ClusteringCompaction
	task.State = datapb.CompactionTaskState_indexing
	task.ResultSegments = []int64{1001}
	task.TmpSegments = []int64{1001}
	output := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed,
		IsInvisible: true, CreatedByCompaction: true, NumOfRows: 84, IsSorted: true, Level: datapb.SegmentLevel_L2, Binlogs: result.Segments[0].InsertLogs,
	})
	require.NoError(t, m.AddSegment(t.Context(), output))
	ct := newClusteringCompactionTask(task, newMockAllocator(t), m, nil, nil, nil)
	require.NoError(t, ct.completeTask())
	require.Equal(t, datapb.CompactionTaskState_indexing, ct.GetTaskProto().GetState())
	require.Empty(t, m.partitionStatsMeta.ListAllPartitionStatsInfos())
	require.ElementsMatch(t, []int64{2001, 2002}, segmentChangeViewIDs(t, m))
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	g := m.GetSegmentChangeGroup(t.Context(), 10, 5000)
	require.Equal(t, model.SegmentChangeStateCommitted, g.State)
	require.Equal(t, []int64{1001}, segmentChangeViewIDs(t, m))
	require.Equal(t, int64(5000), m.GetSegment(t.Context(), 1001).GetPartitionStatsVersion())
	infos, err := m.catalog.ListPartitionStatsInfos(t.Context())
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Equal(t, []int64{1001}, infos[0].GetSegmentIDs())
	version, err := m.catalog.GetCurrentPartitionStatsVersion(t.Context(), 10, 100, "ch-1")
	require.NoError(t, err)
	require.Equal(t, int64(5000), version)
	require.NoError(t, ct.completeTask())
	require.Equal(t, datapb.CompactionTaskState_completed, ct.GetTaskProto().GetState())
}

func TestSegmentChangeClusteringEmptyStats(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, _ := seedMixProducer(t)
	task.Type = datapb.CompactionType_ClusteringCompaction
	task.State = datapb.CompactionTaskState_indexing
	task.ResultSegments = []int64{1001}
	task.TmpSegments = []int64{1001}
	require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Dropped, IsInvisible: true, Level: datapb.SegmentLevel_L2})))
	ct := newClusteringCompactionTask(task, newMockAllocator(t), m, nil, nil, nil)
	require.NoError(t, ct.completeTask())
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	infos, err := m.catalog.ListPartitionStatsInfos(t.Context())
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Empty(t, infos[0].GetSegmentIDs(), "zero-row results are not partition stats members")
	require.Empty(t, segmentChangeViewIDs(t, m))
	require.NoError(t, ct.completeTask())
	require.True(t, ct.Clean())
	infos, err = m.catalog.ListPartitionStatsInfos(t.Context())
	require.NoError(t, err)
	require.Len(t, infos, 1)
}

func TestSegmentChangeImportFence(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, _, _ := seedMixProducer(t)
	require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, IsImporting: true, NumOfRows: 10, IsSorted: true, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}},
	})))
	s := &Server{meta: m}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 8000, CollectionID: 10}}
	staged, err := s.stageImportChange(t.Context(), job, "ch-1", []int64{1001}, 9000)
	require.True(t, staged)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, m.GetSegment(t.Context(), 1001).GetIsInvisible())
	require.Zero(t, m.GetSegment(t.Context(), 1001).GetCommitTimestamp())
	require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
	inspector := newSegmentChangeInspector(t.Context(), m, newMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	staged, err = s.stageImportChange(t.Context(), job, "ch-1", nil, 9000)
	require.True(t, staged)
	require.NoError(t, err)
	require.Equal(t, uint64(9000), m.GetSegment(t.Context(), 1001).GetCommitTimestamp())
	require.False(t, m.GetSegment(t.Context(), 1001).GetIsInvisible())
}
