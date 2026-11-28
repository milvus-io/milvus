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
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func newImportCommitCallbackTest(t *testing.T) (*Server, *datacoordkv.Catalog, message.BroadcastResultCommitImportMessageV2) {
	t.Helper()
	ctx := context.Background()
	catalog := &datacoordkv.Catalog{MetaKv: meta.NewMetaMemoryKV()}
	imports, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.NoError(t, imports.AddJob(ctx, &bulkimport.Job{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 100, Vchannels: []string{"v1", "v2"}, State: internalpb.ImportJobState_Uncommitted, CommitByCoordinator: true},
		Tr:        timerecord.NewTimeRecorder("import-commit"),
	}))
	task := &bulkimport.Task{}
	task.Task.Store(&datapb.ImportTaskV2{
		JobID: 1, TaskID: 41, CollectionID: 100, State: datapb.ImportTaskStateV2_Completed,
		SegmentIDs: []int64{10, 20}, SortedSegmentIDs: []int64{11},
	})
	require.NoError(t, imports.AddTask(ctx, task))
	metaStore := metacache.NewMetaStore(nil)
	metaStore.PutCollection(&meta.CollectionInfo{ID: 100})
	segments := meta.NewSegmentsInfo(metaStore)
	for id, channel := range map[int64]string{10: "v1", 11: "v1", 20: "v2"} {
		segments.SetSegment(id, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, CollectionID: 100, PartitionID: 1, InsertChannel: channel,
			State: commonpb.SegmentState_Flushed, IsImporting: true, NumOfRows: 1,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: id, TimestampTo: 100, EntriesNum: 1}}}},
		}})
	}
	server := &Server{importMeta: imports, meta: &meta.Meta{Ctx: ctx, Catalog: catalog, Segments: segments, MetaStore: metaStore}}
	manager, err := dataview.RecoverManager(t.Context(), catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil,
		dataview.WithPendingTransformProjector(func(ctx context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return pendingTransformBounds(ctx, server.meta, server.importMeta, view)
		}))
	require.NoError(t, err)
	_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{"v1", "v2"}, TransformStarts: map[string]uint64{"v1": 50, "v2": 60},
	})
	require.NoError(t, err)
	server.dataViewManager = manager
	server.meta.DataViewManager = manager
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	server.ddl = newDDLCallbacks(server)
	result := buildCommitImportBroadcastResult(1)
	result.Results = map[string]*message.AppendResult{
		"v1": {TimeTick: 200}, "v2": {TimeTick: 300}, funcutil.GetControlChannel("test"): {TimeTick: 900},
	}
	return server, catalog, result
}

func newLegacyImportCommitCallbackTest(t *testing.T) (*Server, *datacoordkv.Catalog, message.BroadcastResultCommitImportMessageV2) {
	t.Helper()
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "false")
	return newImportCommitCallbackTest(t)
}

func TestImportCommitCallbackPublishesPerChannelVisibility(t *testing.T) {
	server, catalog, result := newLegacyImportCommitCallbackTest(t)
	ctx := context.Background()
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	for id, tick := range map[int64]uint64{10: 200, 11: 200, 20: 300} {
		segment := server.meta.GetSegment(ctx, id)
		require.False(t, segment.GetIsImporting())
		require.Equal(t, tick, segment.GetCommitTimestamp())
		require.Equal(t, tick, segment.GetTransformStartAfterTimetick())
	}
	// Completion is durable and no per-vchannel RPC/counter is needed.
	restored, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	job := restored.GetJob(ctx, 1)
	require.Equal(t, internalpb.ImportJobState_Completed, job.GetState())
	require.NotEmpty(t, job.GetCompleteTime())
	require.Empty(t, job.GetCommittedVchannels())
	patch := mockey.Mock((*meta.Meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	server.importMeta = restored
	server.ddl = newDDLCallbacks(server)
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result), "completed replay must not publish again")
}

func TestImportCommitCallbackRetriesSegmentPersistence(t *testing.T) {
	server, _, result := newLegacyImportCommitCallbackTest(t)
	ctx := context.Background()
	patch := mockey.Mock((*meta.Meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	patch.UnPatch()
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
	require.True(t, server.meta.GetSegment(ctx, 10).GetIsImporting())
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
}

type importDemandViewRef struct {
	dataview.DataViewRef
	dataViewCalls int
}

func (r *importDemandViewRef) DataView() *viewpb.DataViewOfCollection {
	r.dataViewCalls++
	return r.DataViewRef.DataView()
}

func TestImportCommitCallbackSkipsSegmentSelectionWhenCollectionNotLoaded(t *testing.T) {
	server, _, result := newLegacyImportCommitCallbackTest(t)
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	refresh := server.ddl.RefreshImportDataView
	var view *importDemandViewRef
	server.ddl.RefreshImportDataView = func(ctx context.Context, collectionID int64) (dataview.DataViewRef, error) {
		ref, err := refresh(ctx, collectionID)
		require.NoError(t, err)
		require.NotNil(t, ref)
		view = &importDemandViewRef{DataViewRef: ref}
		return view, nil
	}
	checks := 0
	server.ddl.GetImportLoadDemandRecorder = func(_ context.Context, collectionID int64) func([]int64) {
		require.Equal(t, int64(100), collectionID)
		checks++
		return nil
	}
	ctx := t.Context()
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, 1, checks)
	require.NotNil(t, view)
	require.Zero(t, view.dataViewCalls, "an unloaded collection skips copying the snapshot for segment selection")
	require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
}

func TestImportCommitCallbackRecordsPublishedFallbackLoadDemandOnce(t *testing.T) {
	server, _, result := newLegacyImportCommitCallbackTest(t)
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.WaitForIndex, "false")
	ctx := t.Context()
	original := server.meta.GetSegment(ctx, 10).Clone()
	original.State = commonpb.SegmentState_Dropped
	original.Compacted = true
	server.meta.Segments.SetSegment(10, original)
	sorted := server.meta.GetSegment(ctx, 11).Clone()
	sorted.CompactionFrom = []int64{10}
	sorted.IsSorted = true
	server.meta.Segments.SetSegment(11, sorted)
	server.meta.Segments.SetSegment(30, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 30, CollectionID: 100, PartitionID: 1, InsertChannel: "v1", State: commonpb.SegmentState_Flushed, NumOfRows: 1,
		StartPosition: &msgpb.MsgPosition{ChannelName: "v1", Timestamp: 100},
		Binlogs:       []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 30, EntriesNum: 1}}}},
	}))
	const scalarIndexID = 1001
	server.indexMeta = newReadinessIndexMeta(100,
		map[int64]*model.Index{scalarIndexID: {CollectionID: 100, FieldID: 101, IndexID: scalarIndexID}},
		map[int64][]*model.SegmentIndex{
			10: {segmentIndex(10, scalarIndexID, commonpb.IndexState_InProgress)},
			11: {segmentIndex(11, scalarIndexID, commonpb.IndexState_InProgress)},
			20: {segmentIndex(20, scalarIndexID, commonpb.IndexState_Finished)},
		})
	server.meta.IndexCatalog = server.indexMeta

	var recorded [][]int64
	fallbackPublished := false
	defer server.dataViewManager.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) {
		if _, ok := view.Segment(10); ok {
			require.Empty(t, recorded, "publish the DataView before recording demand")
			require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
			fallbackPublished = true
		}
	})()
	WithImportLoadDemandRecorder(func(ctx context.Context, collectionID int64) func([]int64) {
		require.Equal(t, int64(100), collectionID)
		require.True(t, fallbackPublished)
		require.False(t, server.meta.GetSegment(ctx, 10).GetIsImporting(), "the timestamp and visibility write already succeeded")
		require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
		return func(segmentIDs []int64) {
			require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
			recorded = append(recorded, segmentIDs)
		}
	})(server)
	server.ddl = newDDLCallbacks(server)

	err := func() error {
		failure := mockey.Mock((*datacoordkv.Catalog).SaveDataView).Return(context.DeadlineExceeded).Build()
		defer failure.UnPatch()
		return server.ddl.CommitImportV2AckCallback(ctx, result)
	}()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
	require.Empty(t, recorded, "failed DataView write must not record demand")
	// A recompute can publish the already committed SegmentMeta before the
	// WAL callback retries. It goes through the same publication as the worker.
	_, err = server.RefreshDataView(ctx, 100)
	require.NoError(t, err)
	require.Empty(t, recorded, "a standalone recompute does not record import demand")
	ref, err := server.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	fallbackVersion := ref.Version()
	view := ref.DataView()
	ref.Deref()
	var published []int64
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			published = append(published, partition.GetSegmentIds()...)
		}
	}
	require.ElementsMatch(t, []int64{10, 20, 30}, published, "publish the fallback while the replacement's scalar index is incomplete")
	require.True(t, fallbackPublished)

	// Publish a newer snapshot and run GC before the callback selects IDs.
	refresh := server.ddl.RefreshImportDataView
	server.ddl.RefreshImportDataView = func(ctx context.Context, collectionID int64) (dataview.DataViewRef, error) {
		confirmed, err := refresh(ctx, collectionID)
		require.NoError(t, err)
		require.Equal(t, fallbackVersion, confirmed.Version())
		indexes, ok := server.indexMeta.SegmentIndexes.Get(11)
		require.True(t, ok)
		indexes.Insert(scalarIndexID, segmentIndex(11, scalarIndexID, commonpb.IndexState_Finished))
		latest, err := server.RefreshDataView(ctx, collectionID)
		require.NoError(t, err)
		require.Greater(t, latest.GetCompactVersion(), fallbackVersion.GetCompactVersion())
		require.NoError(t, server.dataViewManager.GarbageCollect(ctx, collectionID, 1))
		retained, err := server.dataViewManager.Get(ctx, collectionID, fallbackVersion)
		require.NoError(t, err)
		require.NotNil(t, retained, "the import reference protects the exact version from GC")
		retained.Deref()
		return confirmed, nil
	}
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.NoError(t, server.dataViewManager.GarbageCollect(ctx, 100, 1))
	ref, err = server.dataViewManager.Get(ctx, 100, fallbackVersion)
	require.NoError(t, err)
	require.Nil(t, ref, "the completed callback releases the snapshot reference")
	server.ddl.RefreshImportDataView = refresh
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
	require.Len(t, recorded, 1, "replay must not record the fallback again")
	require.ElementsMatch(t, []int64{10, 20}, recorded[0])
}

func TestImportCommitCallbackRecordsTaskOutputsAfterStagingEnabledOnRetry(t *testing.T) {
	server, _, result := newLegacyImportCommitCallbackTest(t)
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	ctx := t.Context()
	var recorded [][]int64
	server.ddl.GetImportLoadDemandRecorder = func(context.Context, int64) func([]int64) {
		return func(ids []int64) { recorded = append(recorded, ids) }
	}

	failure := mockey.Mock((*datacoordkv.Catalog).SaveDataView).Return(context.DeadlineExceeded).Build()
	err := server.ddl.CommitImportV2AckCallback(ctx, result)
	failure.UnPatch()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, recorded)

	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Empty(t, server.meta.ProducerChangeGroups(model.SegmentChangeSourceImportJob, 1, ""))
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Len(t, recorded, 1)
	require.ElementsMatch(t, []int64{10, 11, 20}, recorded[0])
}

func TestImportCommitCallbackRecordsStagedLoadDemandOnce(t *testing.T) {
	server, _, result := newImportCommitCallbackTest(t)
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.WaitForIndex, "false")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeGroupMaxMembers, "1")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.DVForceAllIndexReady, "false")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableCompaction, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableSortCompaction, "true")
	ctx := t.Context()
	schema := readinessVectorSchema()
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64})
	server.meta.AddCollection(&meta.CollectionInfo{ID: 100, Schema: schema, VChannelNames: []string{"v1", "v2"}})
	const (
		vectorIndexID = 1000
		scalarIndexID = 1001
	)
	segmentIDs := []int64{10, 11, 20}
	segmentIndexes := make(map[int64][]*model.SegmentIndex)
	rows := max(1, Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64())
	for _, id := range segmentIDs {
		segment := server.meta.GetSegment(ctx, id).Clone()
		segment.NumOfRows = rows
		segment.Binlogs[0].Binlogs[0].EntriesNum = rows
		segment.IsSorted = id != 10
		segment.StartPosition = &msgpb.MsgPosition{ChannelName: segment.GetInsertChannel(), Timestamp: 100}
		server.meta.Segments.SetSegment(id, segment)
		segmentIndexes[id] = []*model.SegmentIndex{
			segmentIndex(id, vectorIndexID, commonpb.IndexState_Finished),
			segmentIndex(id, scalarIndexID, commonpb.IndexState_InProgress),
		}
	}
	const sortedOutputID = 12
	segmentIndexes[sortedOutputID] = []*model.SegmentIndex{
		segmentIndex(sortedOutputID, vectorIndexID, commonpb.IndexState_Finished),
		segmentIndex(sortedOutputID, scalarIndexID, commonpb.IndexState_InProgress),
	}
	server.indexMeta = newReadinessIndexMeta(100, map[int64]*model.Index{
		vectorIndexID: vectorIndex(2, vectorIndexID),
		scalarIndexID: {CollectionID: 100, FieldID: 101, IndexID: scalarIndexID, IndexName: "scalar"},
	}, segmentIndexes)
	server.meta.IndexCatalog = server.indexMeta
	job := server.importMeta.GetJob(ctx, 1)
	require.ErrorIs(t, server.prepareImportPublication(ctx, job), merr.ErrServiceUnavailable)
	original := server.meta.GetSegment(ctx, 10)
	_, err := server.meta.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
		PlanID: 50, Type: datapb.CompactionType_SortCompaction, CollectionID: 100, Schema: schema, InputSegments: []int64{10},
	}, &datapb.CompactionPlanResult{
		PlanID: 50, Segments: []*datapb.CompactionSegment{{
			SegmentID: sortedOutputID, NumOfRows: rows, IsSorted: true, InsertLogs: original.GetBinlogs(),
		}},
	})
	require.NoError(t, err)
	require.NoError(t, server.prepareImportPublication(ctx, job))

	var recorded [][]int64
	var published *api.CollectionDataView
	defer server.dataViewManager.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) {
		published = view
	})()
	server.ddl.GetImportLoadDemandRecorder = func(_ context.Context, collectionID int64) func([]int64) {
		require.Equal(t, int64(100), collectionID)
		return func(ids []int64) {
			require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
			locks := server.meta.GetSegmentManifestLocks()
			require.True(t, locks.TryLockMany(ids), "group members are unlocked when recording demand")
			locks.UnlockMany(ids)
			for _, id := range ids {
				_, ok := published.Segment(id)
				require.True(t, ok, "group member has reached DataView listeners")
				require.False(t, server.meta.GetSegment(ctx, id).GetIsInvisible(), "group transaction has succeeded")
			}
			recorded = append(recorded, ids)
		}
	}
	refresh := server.ddl.RefreshImportDataView
	server.ddl.RefreshImportDataView = func(context.Context, int64) (dataview.DataViewRef, error) {
		return nil, context.DeadlineExceeded
	}
	require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	groups := server.meta.ProducerChangeGroups(model.SegmentChangeSourceImportJob, 1, "")
	require.Len(t, groups, 3)
	for _, group := range groups {
		require.Equal(t, model.SegmentChangeStateCommitted, group.State)
	}
	require.Empty(t, recorded, "group publication alone does not record import demand")
	server.ddl.RefreshImportDataView = refresh
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Len(t, recorded, 1)
	require.ElementsMatch(t, []int64{sortedOutputID, 11, 20}, recorded[0], "use the staged sort output in the DataView")
}

func TestImportCommitCallbackRecordsCDCDemandAfterLocalIndex(t *testing.T) {
	server, _, result := newImportCommitCallbackTest(t)
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.WaitForIndex, "false")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	ctx := t.Context()
	broadcast := message.NewCommitImportMessageBuilderV2().
		WithHeader(result.Message.Header()).
		WithBody(&messagespb.CommitImportMessageBody{}).
		WithBroadcast([]string{"v1", "v2", funcutil.GetControlChannel("test")}).
		MustBuildBroadcast()
	replicated := broadcast.WithBroadcastID(1).SplitIntoMutableMessage()[0].WithReplicateHeader(&message.ReplicateHeader{
		ClusterID: "primary", TimeTick: 200, VChannel: "v1",
		MessageID: walimplstest.NewTestMessageID(2), LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
	})
	result.Message = message.MustAsSpecializedBroadcastMessage[*message.CommitImportMessageHeader, *messagespb.CommitImportMessageBody](replicated)

	const indexID = 1000
	segmentIDs := []int64{10, 11, 20}
	server.meta.AddCollection(&meta.CollectionInfo{ID: 100, Schema: readinessVectorSchema(), VChannelNames: []string{"v1", "v2"}})
	indexes := make(map[int64][]*model.SegmentIndex, len(segmentIDs))
	rows := max(1, Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64())
	for _, id := range segmentIDs {
		segment := server.meta.GetSegment(ctx, id).Clone()
		segment.NumOfRows = rows
		segment.Binlogs[0].Binlogs[0].EntriesNum = rows
		segment.IsSorted = true
		segment.StartPosition = &msgpb.MsgPosition{ChannelName: segment.GetInsertChannel(), Timestamp: 100}
		server.meta.Segments.SetSegment(id, segment)
		indexes[id] = []*model.SegmentIndex{segmentIndex(id, indexID, commonpb.IndexState_InProgress)}
	}
	server.indexMeta = newReadinessIndexMeta(100, map[int64]*model.Index{indexID: vectorIndex(2, indexID)}, indexes)
	server.meta.IndexCatalog = server.indexMeta

	var recorded [][]int64
	server.ddl.GetImportLoadDemandRecorder = func(_ context.Context, collectionID int64) func([]int64) {
		require.Equal(t, int64(100), collectionID)
		return func(ids []int64) { recorded = append(recorded, ids) }
	}
	require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), merr.ErrServiceUnavailable)
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
	require.Empty(t, recorded)

	groups := server.meta.ProducerChangeGroups(model.SegmentChangeSourceCDCReplicated, 1, "")
	require.Len(t, groups, 1, "the first unready channel interrupts the callback")
	require.Equal(t, model.SegmentChangeStateStaged, groups[0].State)
	for _, id := range segmentIDs {
		indexes, ok := server.indexMeta.SegmentIndexes.Get(id)
		require.True(t, ok)
		indexes.Insert(indexID, segmentIndex(id, indexID, commonpb.IndexState_Finished))
	}
	inspector := newSegmentChangeInspector(ctx, server.meta, server.allocator)
	defer inspector.Stop()
	inspector.checkAllGroups(ctx)
	require.Empty(t, recorded, "readiness alone does not publish")
	inspector.checkAllGroups(ctx)
	require.NoError(t, server.meta.RequirePublishedChange(ctx, groups[0]))
	require.Empty(t, recorded, "the inspector does not record demand for a single CDC group")
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())

	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
	require.Len(t, recorded, 1)
	require.ElementsMatch(t, segmentIDs, recorded[0], "report the complete CDC job once")
}

func TestImportCommitCallbackAfterCollectionDrop(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		for _, state := range []internalpb.ImportJobState{
			internalpb.ImportJobState_Uncommitted,
			internalpb.ImportJobState_Committing,
			internalpb.ImportJobState_Completed,
		} {
			name := "runtime/" + state.String()
			if recovered {
				name = "recovered/" + state.String()
			}
			t.Run(name, func(t *testing.T) {
				server, catalog, result := newImportCommitCallbackTest(t)
				ctx := t.Context()
				require.NoError(t, server.importMeta.UpdateJob(ctx, 1, bulkimport.UpdateJobState(state)))
				_, err := server.dataViewManager.OnDropCollection(ctx, 100)
				require.NoError(t, err)
				if recovered {
					server.importMeta, err = bulkimport.NewImportMeta(ctx, catalog, nil, nil)
					server.ddl = newDDLCallbacks(server)
					require.NoError(t, err)
					// No snapshots or in-memory drop markers survive this restart.
					// Production requires Create/Bootstrap before ordinary recompute.
					server.dataViewManager, err = dataview.RecoverManager(ctx, catalog,
						func(context.Context, int64) (bool, error) { return false, nil }, nil, nil, nil,
						dataview.WithFrontierProjector(func(context.Context, int64) (map[string]uint64, error) {
							return nil, nil
						}))
					require.NoError(t, err)
					server.meta.DataViewManager = server.dataViewManager
				}
				lookupErr := context.DeadlineExceeded
				server.SetCollectionDroppedChecker(func(ctx context.Context, id int64) (bool, error) {
					require.Equal(t, int64(100), id)
					return lookupErr == nil, lookupErr
				})
				// A failed metadata query must not be mistaken for deletion.
				require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
				if state != internalpb.ImportJobState_Completed {
					require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
				}
				lookupErr = nil
				require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
				require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result), "completed replay must also finish after drop")
				imports, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
				require.NoError(t, err)
				require.Equal(t, internalpb.ImportJobState_Completed, imports.GetJob(ctx, 1).GetState())
				views, err := catalog.ListAllDataViews(ctx)
				require.NoError(t, err)
				require.Empty(t, views, "commit must not resurrect the deleted collection")
			})
		}
	}
}

func TestImportCommitCallbackPublishesBeforeCompleted(t *testing.T) {
	server, catalog, result := newImportCommitCallbackTest(t)
	ctx := t.Context()
	// Another import is still being prepared. It must not be visible or block
	// this commit while the collection's broadcast lock excludes its commit.
	require.NoError(t, server.importMeta.AddJob(ctx, &bulkimport.Job{
		ImportJob: &datapb.ImportJob{JobID: 2, CollectionID: 100, Vchannels: []string{"v1", "v2"}, State: internalpb.ImportJobState_Importing, CommitByCoordinator: true},
	}))
	server.meta.Segments.SetSegment(30, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 30, CollectionID: 100, PartitionID: 1, InsertChannel: "v1", State: commonpb.SegmentState_Flushed, IsImporting: true,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100}},
	}))
	failure := mockey.Mock((*datacoordkv.Catalog).SaveDataView).Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	failure.UnPatch()
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
	ref, err := server.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	for _, shard := range ref.DataView().Shards {
		require.Empty(t, shard.Partitions, "failed publication must retain the old view")
	}
	ref.Deref()
	// The durable callback can resume after a coordinator restart. No QueryView
	// infrastructure is installed: import completion does not wait for QNs.
	server.importMeta, err = bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	server.ddl = newDDLCallbacks(server)
	require.NoError(t, err)
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
	require.Equal(t, internalpb.ImportJobState_Importing, server.importMeta.GetJob(ctx, 2).GetState())
	ref, err = server.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	defer ref.Deref()
	ids := []int64{}
	for _, shard := range ref.DataView().Shards {
		for _, partition := range shard.Partitions {
			ids = append(ids, partition.SegmentIds...)
		}
	}
	require.ElementsMatch(t, []int64{10, 11, 20}, ids)
	version := ref.DataView().GetDataVersion()
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	replay, err := server.dataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	defer replay.Deref()
	require.Equal(t, version, replay.DataView().GetDataVersion(), "completed replay is idempotent")
}

func TestImportCommitCallbackRetriesCompletionAfterRestart(t *testing.T) {
	server, catalog, result := newLegacyImportCommitCallbackTest(t)
	ctx := context.Background()
	// All segment metadata succeeds, but the final job write fails.
	patch := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).
		When(func(_ *datacoordkv.Catalog, _ context.Context, job *datapb.ImportJob) bool {
			return job.GetState() == internalpb.ImportJobState_Completed
		}).
		Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	patch.UnPatch()
	require.False(t, server.meta.GetSegment(ctx, 10).GetIsImporting())
	restored, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.Equal(t, internalpb.ImportJobState_Committing, restored.GetJob(ctx, 1).GetState())
	server.importMeta = restored
	server.ddl = newDDLCallbacks(server)
	// A stale timeout snapshot must not fail a partially published commit.
	checker := &bulkimport.Checker{Ctx: ctx, ImportMeta: restored}
	stale := &bulkimport.Job{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Uncommitted, TimeoutTs: 1}}
	checker.TryTimeoutJob(stale)
	require.Equal(t, internalpb.ImportJobState_Committing, restored.GetJob(ctx, 1).GetState())
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, restored.GetJob(ctx, 1).GetState())
	require.Equal(t, uint64(200), server.meta.GetSegment(ctx, 10).GetCommitTimestamp())
}

func TestImportCommitCallbackRejectsTimestampBeforeRows(t *testing.T) {
	server, _, result := newLegacyImportCommitCallbackTest(t)
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	checks := 0
	server.ddl.GetImportLoadDemandRecorder = func(context.Context, int64) func([]int64) {
		checks++
		return nil
	}
	result.Results["v1"].TimeTick = 99
	err := server.ddl.CommitImportV2AckCallback(context.Background(), result)
	require.ErrorIs(t, err, merr.ErrImportSysFailed)
	require.Zero(t, checks, "the demand check starts only after the import publishes")
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(context.Background(), 1).GetState())
	for _, id := range []int64{10, 11, 20} {
		segment := server.meta.GetSegment(context.Background(), id)
		require.True(t, segment.GetIsImporting())
		require.Zero(t, segment.GetCommitTimestamp())
	}
}

func TestImportCommitCallbackRetriesCommitPhasePersistence(t *testing.T) {
	server, _, result := newLegacyImportCommitCallbackTest(t)
	ctx := context.Background()
	patch := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, server.ddl.CommitImportV2AckCallback(ctx, result), context.DeadlineExceeded)
	require.Equal(t, internalpb.ImportJobState_Uncommitted, server.importMeta.GetJob(ctx, 1).GetState())
	require.True(t, server.meta.GetSegment(ctx, 10).GetIsImporting())
	patch.UnPatch()
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, internalpb.ImportJobState_Completed, server.importMeta.GetJob(ctx, 1).GetState())
}

func TestImportCallbackExcludesControlFromJobChannels(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	type ackAllocator struct{ allocator.Allocator }
	type ackHandler struct{ meta.Handler }
	allocate := mockey.Mock((*ackAllocator).AllocN).Return(int64(10), int64(11), nil).Build()
	defer allocate.UnPatch()
	collection := mockey.Mock((*ackHandler).GetCollection).Return(
		&meta.CollectionInfo{ID: 100, VChannelNames: []string{"v1"}}, nil).Build()
	defer collection.UnPatch()
	catalog := &datacoordkv.Catalog{MetaKv: meta.NewMetaMemoryKV()}
	imports, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	server := &Server{
		allocator: &ackAllocator{}, handler: &ackHandler{}, importMeta: imports,
	}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	server.ddl = newDDLCallbacks(server)

	control := funcutil.GetControlChannel("test")
	msg := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
		WithBody(&message.ImportMsg{JobID: 1, CollectionID: 100}).
		WithBroadcast([]string{"v1", control}).MustBuildBroadcast()
	// Routing belongs to job creation; do not mock away the ACK-to-job path.
	err = server.ddl.ImportV1AckCallback(ctx, message.BroadcastResultImportMessageV1{
		Message: message.MustAsSpecializedBroadcastMessage[*message.ImportMessageHeader, *message.ImportMsg](msg),
		Results: map[string]*message.AppendResult{"v1": {TimeTick: 100}, control: {TimeTick: 900}},
	})
	require.NoError(t, err)
	job := imports.GetJob(ctx, 1)
	require.NotNil(t, job)
	require.False(t, job.GetCommitByCoordinator())
	require.Equal(t, []string{"v1"}, job.GetReadyVchannels())
	require.Equal(t, int64(1), job.GetJobID())
	require.EqualValues(t, 900, job.GetDataTs())
}

func TestLegacyImportCommitSurvivesRetiredBroadcast(t *testing.T) {
	ctx := context.Background()
	server, catalog, result := newLegacyImportCommitCallbackTest(t)
	server.stateCode.Store(commonpb.StateCode_Healthy)
	require.NoError(t, server.importMeta.UpdateJob(ctx, 1, func(job bulkimport.ImportJob) {
		job.(*bulkimport.Job).CommitByCoordinator = false
	}))
	msg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{JobId: 1}).
		WithBody(&message.CommitImportMessageBody{}).
		WithBroadcast([]string{"v1", "v2"}).MustBuildBroadcast()
	result.Message = message.MustAsSpecializedBroadcastMessage[*message.CommitImportMessageHeader, *message.CommitImportMessageBody](msg)
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.True(t, server.meta.GetSegment(ctx, 10).GetIsImporting())
	require.Equal(t, internalpb.ImportJobState_Committing, server.importMeta.GetJob(ctx, 1).GetState())
	// The old broadcaster can now be TOMBSTONE. Recovery needs only the
	// original per-channel WAL message, not its broadcast task.
	restored, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	server.importMeta = restored
	server.ddl = newDDLCallbacks(server)
	checker := &bulkimport.Checker{Ctx: ctx, ImportMeta: restored}
	request := &datapb.HandleCommitVchannelRequest{JobId: 1, Vchannel: "v1", CommitTimestamp: 200}
	failure := mockey.Mock((*meta.Meta).UpdateSegmentsInfo).Return(context.DeadlineExceeded).Build()
	status, err := server.HandleCommitVchannel(ctx, request)
	require.Error(t, merr.CheckRPCCall(status, err))
	failure.UnPatch()
	require.Empty(t, restored.GetJob(ctx, 1).GetCommittedVchannels())
	status, err = server.HandleCommitVchannel(ctx, request)
	require.NoError(t, merr.CheckRPCCall(status, err))
	checker.CheckCommittingJob(restored.GetJob(ctx, 1))
	require.Equal(t, internalpb.ImportJobState_Committing, restored.GetJob(ctx, 1).GetState())
	require.False(t, server.meta.GetSegment(ctx, 10).GetIsImporting())
	require.True(t, server.meta.GetSegment(ctx, 20).GetIsImporting())
	request.CommitTimestamp = 999
	status, err = server.HandleCommitVchannel(ctx, request)
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.EqualValues(t, 200, server.meta.GetSegment(ctx, 10).GetCommitTimestamp(), "duplicate RPC must retain original visibility")
	status, err = server.HandleCommitVchannel(ctx, &datapb.HandleCommitVchannelRequest{JobId: 1, Vchannel: "v2", CommitTimestamp: 300})
	require.NoError(t, merr.CheckRPCCall(status, err))
	checker.CheckCommittingJob(restored.GetJob(ctx, 1))
	require.Equal(t, internalpb.ImportJobState_Completed, restored.GetJob(ctx, 1).GetState())
}

func TestCoordinatorImportIgnoresLegacyCompletion(t *testing.T) {
	ctx := context.Background()
	server, catalog, _ := newLegacyImportCommitCallbackTest(t)
	server.stateCode.Store(commonpb.StateCode_Healthy)
	for _, state := range []internalpb.ImportJobState{internalpb.ImportJobState_Importing, internalpb.ImportJobState_Uncommitted, internalpb.ImportJobState_Committing} {
		require.NoError(t, server.importMeta.UpdateJob(ctx, 1, bulkimport.UpdateJobState(state)))
		restored, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
		require.NoError(t, err)
		server.importMeta = restored
		server.ddl = newDDLCallbacks(server)
		require.True(t, restored.GetJob(ctx, 1).GetCommitByCoordinator())
		status, err := server.HandleCommitVchannel(ctx, &datapb.HandleCommitVchannelRequest{JobId: 1, Vchannel: "v1", CommitTimestamp: 200})
		require.NoError(t, merr.CheckRPCCall(status, err))
		require.True(t, server.meta.GetSegment(ctx, 10).GetIsImporting())
		require.Empty(t, restored.GetJob(ctx, 1).GetCommittedVchannels())
		checker := &bulkimport.Checker{Ctx: ctx, ImportMeta: restored}
		checker.CheckCommittingJob(restored.GetJob(ctx, 1))
		require.Equal(t, state, restored.GetJob(ctx, 1).GetState())
	}
}

// TestImportCommitCallbackV3JobPublishesVisibility pins the commit path for an
// Import V3 job: the segment ownership is resolved through the completed
// ImportTaskV3 records, and the same visibility fence (per-channel commit
// timestamp + is_importing clear) and Completed transition apply.
func TestImportCommitCallbackV3JobPublishesVisibility(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "false")
	setSegmentChangeTestParam(t, &Params.QueryCoordCfg.AutoscalePrecheckEnabled, "true")
	ctx := context.Background()
	catalog := &datacoordkv.Catalog{MetaKv: meta.NewMetaMemoryKV()}
	imports, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.NoError(t, imports.AddJob(ctx, &bulkimport.Job{
		ImportJob: &datapb.ImportJob{
			JobID: 1, CollectionID: 100, Vchannels: []string{"v1"},
			State: internalpb.ImportJobState_Uncommitted, CommitByCoordinator: true,
			Version: internalpb.ImportVersion_ImportVersionV3,
		},
		Tr: timerecord.NewTimeRecorder("import-commit"),
	}))
	metaStore := metacache.NewMetaStore(nil)
	metaStore.PutCollection(&meta.CollectionInfo{ID: 100})
	segments := meta.NewSegmentsInfo(metaStore)
	segments.SetSegment(10, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "v1",
		State: commonpb.SegmentState_Flushed, IsImporting: true, NumOfRows: 10,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 10, TimestampTo: 100}}}},
	}})
	// The V3 segment is owned by a Completed ImportTaskV3; the commit callback
	// resolves it through getImportSegmentIDsByVchannel's V3 branch.
	v3task := bulkimport.NewImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 41, CollectionId: 100, State: datapb.ImportTaskStateV2_Completed,
		SegmentId: 10, Vchannel: "v1",
	}, imports, nil, nil)
	require.NoError(t, imports.AddTask(ctx, v3task))

	server := &Server{importMeta: imports, meta: &meta.Meta{Ctx: ctx, Catalog: catalog, Segments: segments, MetaStore: metaStore}}
	manager, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil,
		dataview.WithPendingTransformProjector(func(ctx context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
			return pendingTransformBounds(ctx, server.meta, server.importMeta, view)
		}))
	require.NoError(t, err)
	_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{"v1"}, TransformStarts: map[string]uint64{"v1": 50},
	})
	require.NoError(t, err)
	server.dataViewManager = manager
	server.meta.DataViewManager = manager
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	server.ddl = newDDLCallbacks(server)
	result := buildCommitImportBroadcastResult(1)
	result.Results = map[string]*message.AppendResult{"v1": {TimeTick: 200}}

	var recorded [][]int64
	server.ddl.GetImportLoadDemandRecorder = func(_ context.Context, collectionID int64) func([]int64) {
		require.Equal(t, int64(100), collectionID)
		return func(ids []int64) {
			require.Equal(t, internalpb.ImportJobState_Completed, imports.GetJob(ctx, 1).GetState())
			recorded = append(recorded, ids)
		}
	}
	err = func() error {
		failure := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).
			When(func(_ *datacoordkv.Catalog, _ context.Context, job *datapb.ImportJob) bool {
				return job.GetState() == internalpb.ImportJobState_Completed
			}).
			Return(context.DeadlineExceeded).Build()
		defer failure.UnPatch()
		return server.ddl.CommitImportV2AckCallback(ctx, result)
	}()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, recorded, "a failed Completed write does not record import demand")
	seg := server.meta.GetSegment(ctx, 10)
	require.False(t, seg.GetIsImporting(), "the commit fence must clear is_importing")
	require.Equal(t, uint64(200), seg.GetCommitTimestamp(), "the visibility fence must stamp the channel's timetick")
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.NoError(t, server.ddl.CommitImportV2AckCallback(ctx, result))
	require.Equal(t, [][]int64{{10}}, recorded, "retry accounts for the V3 task output once after clearing is_importing")
	job := imports.GetJob(ctx, 1)
	require.Equal(t, internalpb.ImportJobState_Completed, job.GetState())
	require.NotEmpty(t, job.GetCompleteTime())
}

func TestImportCommitTransformFenceSurvivesRecovery(t *testing.T) {
	server, catalog, _ := newImportCommitCallbackTest(t)
	ctx := context.Background()
	raw := message.NewCommitImportMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitImportMessageHeader{CollectionId: 100, JobId: 1, CommitByCoordinator: true}).
		WithBody(&message.CommitImportMessageBody{}).MustBuildMutable().WithTimeTick(200).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	result := message.AckResultCommitImportMessageV2{Message: message.MustAsImmutableCommitImportMessageV2(raw)}
	failure := mockey.Mock((*datacoordkv.Catalog).SaveImportJob).Return(context.DeadlineExceeded).Build()
	require.ErrorIs(t, server.ddl.CommitImportV2AckOnceCallback(ctx, result), context.DeadlineExceeded)
	failure.UnPatch()
	require.Empty(t, server.importMeta.GetJob(ctx, 1).(*bulkimport.Job).GetTransformCommitTimeticks())
	require.NoError(t, server.ddl.CommitImportV2AckOnceCallback(ctx, result))
	require.NoError(t, server.ddl.CommitImportV2AckOnceCallback(ctx, result), "ack retry is idempotent")
	recovered, err := bulkimport.NewImportMeta(ctx, catalog, nil, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"v1": 200}, recovered.GetJob(ctx, 1).(*bulkimport.Job).GetTransformCommitTimeticks())
	require.Equal(t, internalpb.ImportJobState_Uncommitted, recovered.GetJob(ctx, 1).GetState(), "registration alone must not publish imported rows")
}

func TestImportPendingJobCapsFlushCertificate(t *testing.T) {
	server, _, _ := newImportCommitCallbackTest(t)
	ctx := t.Context()
	check := func(expected map[string]uint64) {
		view, _, abort, err := server.dataViewManager.PrepareFlush(ctx, dataview.FlushDataViewEvent{
			CollectionID:           100,
			GrowingTransformStarts: map[string]uint64{"v1": 1000, "v2": 1000},
			Segments:               []dataview.LoadableSegment{{SegmentID: 40, VChannel: "v1", PartitionID: 1, TransformStartAfterTimetick: 70}},
		})
		require.NoError(t, err)
		defer abort()
		for _, shard := range view.GetShards() {
			require.Equal(t, expected[shard.GetVchannel()], shard.GetGrowingTransformStartAfterTimetick())
		}
	}
	// A certificate observed past a not-yet-acknowledged CommitImport may
	// publish its L1, but must not advance W across that unknown commit.
	check(map[string]uint64{"v1": 50, "v2": 60})
	require.NoError(t, server.importMeta.UpdateJob(ctx, 1, func(job bulkimport.ImportJob) {
		job.(*bulkimport.Job).TransformCommitTimeticks = map[string]uint64{"v1": 200, "v2": 300}
	}))
	check(map[string]uint64{"v1": 200, "v2": 300})
}
