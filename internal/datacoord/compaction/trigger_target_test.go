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

package compaction

import (
	"context"
	"net/url"
	"testing"

	prometheusdto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	rootcatalog "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStandingSortPublishesRewriteCreditAtSharedCapacity(t *testing.T) {
	params := paramtable.Get()
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&Params.DataCoordCfg.EnableAutoCompaction, "false"},
		{&Params.DataCoordCfg.CompactionMaxTaskNum, "1"},
		{&Params.DataCoordCfg.CompactionRateLimitTokens, "1"},
		{&Params.DataCoordCfg.CompactionRateLimitInterval, "3600"},
		{&Params.DataCoordCfg.CompactionPreAllocateIDExpansionFactor, "1"},
	} {
		params.Save(setting.item.Key, setting.value)
		t.Cleanup(func() { params.Reset(setting.item.Key) })
	}
	sortConfig := &Params.DataCoordCfg.EnableSortCompaction
	previous := sortConfig.SwapTempValue("true")
	t.Cleanup(func() { sortConfig.SwapTempValue(previous) })

	ctx := t.Context()
	config := embed.NewConfig()
	config.Dir = t.TempDir()
	config.LogLevel = "error"
	loopback, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	config.ListenClientUrls = []url.URL{*loopback}
	config.ListenPeerUrls = []url.URL{*loopback}
	config.AdvertisePeerUrls = []url.URL{*loopback}
	config.InitialCluster = config.InitialClusterFromName(config.Name)
	kv, err := etcdkv.NewEmbededEtcdKV(config, "test")
	require.NoError(t, err)
	t.Cleanup(kv.Close)
	catalog := kvdatacoord.NewCatalog(kv, "", "")
	collection := &model.Collection{CollectionID: 1, DBID: 2, State: etcdpb.CollectionState_CollectionCreated, UpdateTimestamp: 10}
	require.NoError(t, rootcatalog.NewCatalog(kv).CreateCollection(ctx, collection, collection.UpdateTimestamp))
	taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	segmentMeta := meta.NewTestMetaForCompactionTargets()
	segmentMeta.Catalog, segmentMeta.Ctx = catalog, ctx
	segmentMeta.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Version: 1}})
	segmentMeta.Segments.SetSegment(10, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 10, CollectionID: 1, PartitionID: 2, InsertChannel: "ch",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true, NumOfRows: 10,
		StartPosition: &msgpb.MsgPosition{Timestamp: 90}, DmlPosition: &msgpb.MsgPosition{Timestamp: 99},
		Binlogs:   []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 1000)},
		Statslogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 1001)},
	}))

	manager, err := targets.NewCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(100, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(100), nil).Once()
	rewrite, err := targets.NewRewriteRecord(ctx, alloc, 1, []int64{10})
	require.NoError(t, err)
	scope := &metastore.CompactionTargetCollectionVersion{DatabaseID: collection.DBID, Revision: collection.UpdateTimestamp}
	_, err = manager.Create(ctx, rewrite, scope, func() error { return nil })
	require.NoError(t, err)
	alloc.EXPECT().AllocID(mock.Anything).Return(200, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(110), nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(201, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(111), nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(202, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(112), nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(203, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(113), nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(120), nil).Once()
	alloc.EXPECT().AllocN(int64(3)).Return(10_000, 10_003, nil).Once()

	handler := meta.NewMockHandlerWithMeta(segmentMeta)
	indexMeta := &index.IndexMeta{}
	inspector := NewCompactionInspector(segmentMeta, taskMeta, indexMeta, nil, nil, nil, alloc, handler, nil, nil, index.NewTestVersionManager())
	inspector.SetCompactionTargetMeta(manager)
	require.NoError(t, inspector.InitAdmission())
	triggers := NewCompactionTriggerManager(alloc, handler, inspector, segmentMeta, indexMeta, taskMeta, nil, index.NewTestVersionManager())
	triggers.SetCompactionTargetMeta(manager, func(_ context.Context, id int64) (bool, error) {
		return segmentMeta.GetCollection(id) != nil, nil
	})
	triggers.handleTicker(ctx, TargetTicker)

	stored, err := catalog.ListCompactionTask(ctx)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	sort := stored[0]
	require.Equal(t, datapb.CompactionType_SortCompaction, sort.GetType())
	require.Equal(t, []int64{10}, sort.GetInputSegments())
	require.Equal(t, uint64(120), sort.GetCreateTs())
	require.NotEqual(t, rewrite.GetTargetID(), sort.GetTriggerID())
	require.Zero(t, sort.GetOutputSegmentCountHint())

	result := &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
		SegmentID: sort.GetPreAllocatedSegmentIDs().GetBegin(), NumOfRows: 10, IsSorted: true,
		InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 2000)},
		Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 2001)},
	}}}
	published, _, err := segmentMeta.CompleteCompactionMutation(ctx, sort, result)
	require.NoError(t, err)
	require.Len(t, published, 1)
	require.True(t, published[0].GetIsSorted())
	require.False(t, published[0].GetIsInvisible())
	require.Equal(t, sort.GetCreateTs(), published[0].GetCreateTs())

	// The bucket is exhausted and the Inspector still reports full backlog.
	// The semantic pass must still persist REWRITE's satisfied result.
	triggers.handleTicker(ctx, TargetTicker)
	restarted, err := targets.NewCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	require.Equal(t, targets.StopSatisfied, restarted.GetCompactionTarget(rewrite.GetTargetID()).GetStopReason())
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, restarted.GetCompactionTarget(200).GetState())
}

func TestTargetSubmissionRotatesIndependentPlansAfterFailure(t *testing.T) {
	params := paramtable.Get()
	expansion := &Params.DataCoordCfg.CompactionPreAllocateIDExpansionFactor
	params.Save(expansion.Key, "1")
	t.Cleanup(func() { params.Reset(expansion.Key) })

	alloc := allocator.NewMockAllocator(t)
	nextID := int64(1000)
	alloc.EXPECT().AllocN(int64(4)).RunAndReturn(func(count int64) (int64, int64, error) {
		start := nextID
		nextID += count
		return start, nextID, nil
	}).Times(5)
	inspector := NewMockCompactionInspector(t)
	manager := &CompactionTriggerManager{allocator: alloc, inspector: inspector}
	pool := func(collectionID, partitionID int64, ids ...int64) view.RewriteInputs {
		group := &view.CompactionGroupLabel{CollectionID: collectionID, PartitionID: partitionID, Channel: "ch"}
		inputs := view.RewriteInputs{Group: group, Schema: &schemapb.CollectionSchema{Version: 1}, ExpectedSize: 10}
		for _, id := range ids {
			inputs.Segments = append(inputs.Segments, &view.SegmentView{ID: id, Label: group, NumOfRows: 1, InsertBytes: 15})
		}
		return inputs
	}
	views := []view.TargetView{
		&view.SortView{RewriteInputs: pool(1, 1, 11, 12)},
		&view.RewriteView{RewriteInputs: pool(1, 1, 13)},
		&view.SortView{RewriteInputs: pool(1, 2, 14)},
		&view.SortView{RewriteInputs: pool(2, 1, 21, 22)},
	}
	inspector.EXPECT().attemptLimit(mock.Anything, ProducerTBC, 6).Return(5, admissionRateLimit).Once()
	inspector.EXPECT().recordAdmissionDeferral(ProducerTBC, admissionRateLimit).Once()
	var attempted []int64
	inspector.EXPECT().EnqueueCompaction(mock.Anything, mock.Anything, AdmissionOptions{Producer: ProducerTBC}).
		RunAndReturn(func(_ context.Context, task *datapb.CompactionTask, _ AdmissionOptions) error {
			require.Len(t, task.GetInputSegments(), 1)
			require.Equal(t, int64(2), task.GetPreAllocatedSegmentIDs().GetEnd()-task.GetPreAllocatedSegmentIDs().GetBegin())
			input := task.GetInputSegments()[0]
			attempted = append(attempted, input)
			if input == 11 {
				return merr.ErrCompactionPlanConflict
			}
			return nil
		}).Times(5)
	manager.submitTargetViews(t.Context(), views)
	require.Equal(t, []int64{11, 21, 14, 22, 13}, attempted)
}

func TestStandingSizeAdmitsOneCompletePackageAcrossGroups(t *testing.T) {
	const mib = 1024 * 1024
	params := paramtable.Get()
	cfg := &params.DataCoordCfg
	for _, setting := range []struct{ key, value string }{
		{cfg.EnableAutoCompaction.Key, "true"},
		{cfg.SegmentMaxSize.Key, "1"}, {cfg.MaxFragmentsPerGroup.Key, "8"},
		{cfg.IndexBasedCompaction.Key, "false"}, {cfg.CompactionRateLimitTokens.Key, "1"},
		{cfg.CompactionRateLimitInterval.Key, "3600"}, {cfg.CompactionPreAllocateIDExpansionFactor.Key, "1"},
		{cfg.TwoTierCompaction.Key, "false"},
	} {
		require.NoError(t, params.Save(setting.key, setting.value))
		t.Cleanup(func() { params.Reset(setting.key) })
	}
	sortConfig := &cfg.EnableSortCompaction
	previousSort := sortConfig.SwapTempValue("false")
	t.Cleanup(func() { sortConfig.SwapTempValue(previousSort) })
	ctx := t.Context()
	config := embed.NewConfig()
	config.Dir, config.LogLevel = t.TempDir(), "error"
	addr := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	config.ListenClientUrls, config.AdvertiseClientUrls = []url.URL{addr}, []url.URL{addr}
	config.ListenPeerUrls, config.AdvertisePeerUrls = []url.URL{addr}, []url.URL{addr}
	config.InitialCluster = config.InitialClusterFromName(config.Name)
	backend, err := etcdkv.NewEmbededEtcdKV(config, "test")
	require.NoError(t, err)
	t.Cleanup(backend.Close)
	catalog := kvdatacoord.NewCatalog(backend, "", "")
	taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	targetMeta, err := targets.NewCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	segmentMeta := meta.NewTestMetaForCompactionTargets()
	segmentMeta.Catalog, segmentMeta.Ctx = catalog, ctx
	segmentMeta.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
		Version: 1, Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_FloatVector}},
	}})
	add := func(id, part, residual int64, level datapb.SegmentLevel) {
		stats := &datapb.Statistics{InsertBinlogSize: residual, TimestampFrom: 99}
		insertLogs := meta.GetFieldBinlogIDs(0, 10*id)
		insertLogs.Binlogs[0].EntriesNum = 10
		segmentMeta.Segments.SetSegment(id, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: part, InsertChannel: "ch", Level: level,
			State: commonpb.SegmentState_Flushed, IsSorted: true, NumOfRows: 10, Stats: stats,
			Binlogs:   []*datapb.FieldBinlog{insertLogs},
			Statslogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10*id+1)},
		}))
	}
	for _, id := range []int64{100, 101, 102, 103} {
		add(id, 1+(id-100)/2, 480_000, datapb.SegmentLevel_L1)
	}
	add(104, 3, 475_000, datapb.SegmentLevel_L1) // isolated tail
	add(105, 3, 475_000, datapb.SegmentLevel_L2) // cannot fill that tail
	for i := int64(0); i < 16; i++ {
		add(106+i, 4+i/8, 25_000, datapb.SegmentLevel_L1)
	}
	// Groups 4 and 5 have exactly eight fragments. Put the extra fragment
	// into group 5, leaving the others at their original boundary.
	add(122, 5, 25_000, datapb.SegmentLevel_L1)
	alloc := meta.NewMockAllocator(t)
	handler := meta.NewMockHandlerWithMeta(segmentMeta)
	inspector := NewCompactionInspector(segmentMeta, taskMeta, &index.IndexMeta{}, nil, nil, nil, alloc, handler, nil, nil, index.NewTestVersionManager())
	inspector.SetCompactionTargetMeta(targetMeta)
	require.NoError(t, inspector.InitAdmission())
	triggers := NewCompactionTriggerManager(alloc, handler, inspector, segmentMeta, &index.IndexMeta{}, taskMeta, nil, index.NewTestVersionManager())
	triggers.SetCompactionTargetMeta(targetMeta, func(_ context.Context, id int64) (bool, error) {
		return segmentMeta.GetCollection(id) != nil, nil
	})
	planned := metrics.DataCoordCompactionAdmission.WithLabelValues(ProducerTBC.String(), admissionTaskType(datapb.CompactionType_MixCompaction), "plan", "")
	var before prometheusdto.Metric
	require.NoError(t, planned.Write(&before))
	triggers.handleTicker(ctx, TargetTicker)
	var after prometheusdto.Metric
	require.NoError(t, planned.Write(&after))
	require.Equal(t, float64(3), after.GetCounter().GetValue()-before.GetCounter().GetValue())
	stored, err := catalog.ListCompactionTask(ctx)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	task := stored[0]
	require.Equal(t, datapb.CompactionType_MixCompaction, task.GetType())
	require.Equal(t, []int64{100, 101}, task.GetInputSegments())
	require.Equal(t, int64(mib), task.GetMaxSize())
	require.Equal(t, int64(1), task.GetPreAllocatedSegmentIDs().GetEnd()-task.GetPreAllocatedSegmentIDs().GetBegin())
	require.False(t, segmentMeta.IsSegmentCompacting(102))
	insertLogs := meta.GetFieldBinlogIDs(0, 10_000)
	insertLogs.Binlogs[0].EntriesNum = 20
	output, _, err := segmentMeta.CompleteCompactionMutation(ctx, task, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
		SegmentID: task.GetPreAllocatedSegmentIDs().GetBegin(), NumOfRows: 20, IsSorted: true,
		Stats:               &datapb.Statistics{InsertBinlogSize: 960_000, TimestampFrom: 99},
		InsertLogs:          []*datapb.FieldBinlog{insertLogs},
		Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(0, 10_001)},
	}}})
	require.NoError(t, err)
	require.Len(t, output, 1)
	require.Equal(t, []int64{100, 101}, output[0].GetCompactionFrom())
	require.Equal(t, int64(960_000), output[0].GetResidualSegmentSize())
	for _, record := range targetMeta.GetCompactionTargets() {
		if targets.IsStandingSize(record) {
			require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, record.GetState())
			return
		}
	}
	t.Fatal("missing Standing SIZE target")
}
