//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDirectSegmentTaskExecutorSearchBuildsHistoricalRequestFromHandles(t *testing.T) {
	runner := &fakeSearchTaskRunner{result: &internalpb.SearchResults{NumQueries: 10}}
	executor := NewDirectSegmentTaskExecutorForTest(100, runner, &fakeQueryTaskRunner{})
	legacyReq := &internalpb.SearchRequest{CollectionID: 10}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	tasks := []qnview.QNSearchSegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc, Options: &viewpb.SearchExecutionOptions{FilterOnly: true, EnableExprCache: true}},
		{Handle: fakeSealedSegmentHandle{id: 2, segment: fakeReadableSegment{id: 2, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
	}

	result, err := executor.Search(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, runner.result, result)
	require.NotNil(t, runner.req)
	assert.True(t, runner.req.GetFilterOnly())
	assert.True(t, runner.req.GetEnableExprCache())
	assert.Equal(t, querypb.DataScope_Historical, runner.req.GetScope())
	assert.Equal(t, []int64{1, 2}, runner.req.GetSegmentIDs())
	assert.Equal(t, []string{"v1"}, runner.req.GetDmlChannels())
	assert.Equal(t, uint64(99), runner.req.GetReq().GetMvccTimestamp())
	assert.Equal(t, int64(100), runner.serverID)
	require.Len(t, runner.segments, 2)
}

func TestDirectSegmentTaskExecutorSearchRejectsUnreadableSegment(t *testing.T) {
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, &fakeQueryTaskRunner{})
	tasks := []qnview.QNSearchSegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeTransformOnlySegment{id: 1}}, Request: &internalpb.SearchRequest{}, MVCC: &viewpb.QueryPlanMVCC{}},
	}

	_, err := executor.Search(context.Background(), tasks)

	require.Error(t, err)
}

func TestDirectSegmentTaskExecutorQueryBuildsHistoricalRequestFromHandles(t *testing.T) {
	runner := &fakeQueryTaskRunner{result: &internalpb.RetrieveResults{AllRetrieveCount: 10}}
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, runner)
	legacyReq := &internalpb.RetrieveRequest{CollectionID: 10}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	tasks := []qnview.QNQuerySegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
		{Handle: fakeSealedSegmentHandle{id: 2, segment: fakeReadableSegment{id: 2, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
	}

	result, err := executor.Query(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, runner.result, result)
	require.NotNil(t, runner.req)
	assert.Equal(t, querypb.DataScope_Historical, runner.req.GetScope())
	assert.Equal(t, []int64{1, 2}, runner.req.GetSegmentIDs())
	assert.Equal(t, []string{"v1"}, runner.req.GetDmlChannels())
	assert.Equal(t, uint64(99), runner.req.GetReq().GetMvccTimestamp())
	assert.Equal(t, int64(100), runner.serverID)
	require.Len(t, runner.segments, 2)
}

func TestDirectSegmentTaskExecutorQueryRejectsUnreadableSegment(t *testing.T) {
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, &fakeQueryTaskRunner{})
	tasks := []qnview.QNQuerySegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeTransformOnlySegment{id: 1}}, Request: &internalpb.RetrieveRequest{}, MVCC: &viewpb.QueryPlanMVCC{}},
	}

	_, err := executor.Query(context.Background(), tasks)

	require.Error(t, err)
}

func TestSchedulerQueryPrunesSealedSegmentsByPK(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg.EnableSegmentFilter
	original := cfg.GetValue()
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(cfg.Key, original)) })
	require.NoError(t, paramtable.Get().Save(cfg.Key, "true"))

	schema, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
		Name: "point_queries",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
		},
	})
	require.NoError(t, err)
	plan, err := planparserv2.CreateRetrievePlan(schema, "id == -1", nil)
	require.NoError(t, err)
	encoded, err := proto.Marshal(plan)
	require.NoError(t, err)
	req := &internalpb.RetrieveRequest{SerializedExprPlan: encoded, PkFilter: common.PkFilterHasPkFilter, Limit: 1}
	mvcc := &viewpb.QueryPlanMVCC{TransformingTimetick: 99}

	newCandidate := func(id, pk int64) *pkoracle.BloomFilterSet {
		candidate := pkoracle.NewBloomFilterSet(id, 1, commonpb.SegmentState_Sealed)
		stats, err := storage.NewPrimaryKeyStats(100, int64(schemapb.DataType_Int64), 1)
		require.NoError(t, err)
		stats.Update(storage.NewInt64PrimaryKey(pk))
		candidate.AddHistoricalStats(&storage.PkStatistics{PkFilter: stats.BF, MinPK: stats.MinPk, MaxPK: stats.MaxPk})
		return candidate
	}
	matching := newCandidate(1, -1)
	excluded := newCandidate(2, 42)
	unknown := pkoracle.NewBloomFilterSet(3, 1, commonpb.SegmentState_Sealed)
	makeTask := func(candidate *pkoracle.BloomFilterSet) qnview.QNQuerySegmentTask {
		return qnview.QNQuerySegmentTask{
			Handle: fakeSealedSegmentHandle{
				id: candidate.ID(),
				segment: fakeReadableSegment{
					id: candidate.ID(), vchannel: "v1",
					querySegment: &sealedPKTestSegment{candidate: candidate},
				},
			},
			Request: req,
			MVCC:    mvcc,
		}
	}
	tasks := qnview.NewQNQuerySegmentTasks([]qnview.QNQuerySegmentTask{makeTask(matching), makeTask(excluded), makeTask(unknown)})
	defer tasks.Release()

	runner := &fakeQueryTaskRunner{result: &internalpb.RetrieveResults{Ids: &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{-1}}},
	}}}
	scheduler := NewScheduler(NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, runner))

	for _, tc := range []struct {
		name    string
		enabled string
		filter  int32
		ids     []int64
	}{
		{name: "point query", enabled: "true", filter: common.PkFilterHasPkFilter, ids: []int64{1, 3}},
		{name: "disabled", enabled: "false", filter: common.PkFilterHasPkFilter, ids: []int64{1, 2, 3}},
		{name: "no PK filter", enabled: "true", filter: common.PkFilterNoPkFilter, ids: []int64{1, 2, 3}},
		{name: "repeated point query", enabled: "true", filter: common.PkFilterHasPkFilter, ids: []int64{1, 3}},
		{name: "older request without filter hint", enabled: "true", filter: common.PkFilterNotChecked, ids: []int64{1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, paramtable.Get().Save(cfg.Key, tc.enabled))
			req.PkFilter = tc.filter
			result, err := scheduler.Query(context.Background(), tasks)
			require.NoError(t, err)
			assert.Equal(t, tc.ids, runner.req.GetSegmentIDs())
			var selectedIDs []int64
			for _, seg := range runner.segments {
				selectedIDs = append(selectedIDs, seg.ID())
			}
			assert.Equal(t, tc.ids, selectedIDs)
			assert.Equal(t, []int64{-1}, result.GetIds().GetIntId().GetData())
			assert.Equal(t, uint64(99), runner.req.GetReq().GetMvccTimestamp())
			assert.Zero(t, req.GetMvccTimestamp())
		})
	}

	t.Run("all segments excluded", func(t *testing.T) {
		tasks := qnview.NewQNQuerySegmentTasks([]qnview.QNQuerySegmentTask{makeTask(excluded)})
		defer tasks.Release()
		runner.result = &internalpb.RetrieveResults{}
		result, err := scheduler.Query(context.Background(), tasks)
		require.NoError(t, err)
		assert.Empty(t, runner.req.GetSegmentIDs())
		assert.Empty(t, runner.segments)
		assert.Same(t, runner.result, result)
	})
}

type sealedPKTestSegment struct {
	segments.Segment
	candidate *pkoracle.BloomFilterSet
}

func (s *sealedPKTestSegment) ID() int64              { return s.candidate.ID() }
func (s *sealedPKTestSegment) PkCandidateExist() bool { return s.candidate.PkCandidateExist() }
func (s *sealedPKTestSegment) BatchPkExist(cache *storage.BatchLocationsCache) []bool {
	return s.candidate.BatchPkExist(cache)
}
func (s *sealedPKTestSegment) GetMinPk() *storage.PrimaryKey { return nil }
func (s *sealedPKTestSegment) GetMaxPk() *storage.PrimaryKey { return nil }

type fakeSearchTaskRunner struct {
	collection *segments.Collection
	segments   []segments.Segment
	req        *querypb.SearchRequest
	serverID   int64
	result     *internalpb.SearchResults
	err        error
}

func (r *fakeSearchTaskRunner) Search(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error) {
	r.collection = collection
	r.segments = append([]segments.Segment(nil), selected...)
	r.req = req
	r.serverID = serverID
	return r.result, r.err
}

type fakeQueryTaskRunner struct {
	collection *segments.Collection
	segments   []segments.Segment
	req        *querypb.QueryRequest
	serverID   int64
	result     *internalpb.RetrieveResults
	err        error
}

func (r *fakeQueryTaskRunner) Query(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error) {
	r.collection = collection
	r.segments = append([]segments.Segment(nil), selected...)
	r.req = req
	r.serverID = serverID
	return r.result, r.err
}

type fakeReadableSegment struct {
	id           int64
	vchannel     string
	querySegment segments.Segment
}

func (s fakeReadableSegment) ID() int64 {
	return s.id
}

func (s fakeReadableSegment) VChannel() string {
	return s.vchannel
}

func (s fakeReadableSegment) PartitionID() int64 {
	return 0
}

func (s fakeReadableSegment) TransformStartAfterTimeTick() uint64 {
	return 0
}

func (s fakeReadableSegment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (s fakeReadableSegment) AppliedTransformTimeTick() uint64 {
	return 0
}

func (s fakeReadableSegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s fakeReadableSegment) Release(context.Context) error {
	return nil
}

func (s fakeReadableSegment) QuerySegment() segments.Segment {
	return s.querySegment
}

func (s fakeReadableSegment) Collection() *segments.Collection {
	return nil
}

type fakeTransformOnlySegment struct {
	id int64
}

func (s fakeTransformOnlySegment) ID() int64 {
	return s.id
}

func (s fakeTransformOnlySegment) VChannel() string {
	return "v1"
}

func (s fakeTransformOnlySegment) PartitionID() int64 {
	return 0
}

func (s fakeTransformOnlySegment) TransformStartAfterTimeTick() uint64 {
	return 0
}

func (s fakeTransformOnlySegment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (s fakeTransformOnlySegment) AppliedTransformTimeTick() uint64 {
	return 0
}

func (s fakeTransformOnlySegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s fakeTransformOnlySegment) Release(context.Context) error {
	return nil
}

func NewDirectSegmentTaskExecutorForTest(serverID int64, searchRunner SearchTaskRunner, queryRunner QueryTaskRunner) *DirectSegmentTaskExecutor {
	return &DirectSegmentTaskExecutor{
		serverID:     serverID,
		searchRunner: searchRunner,
		queryRunner:  queryRunner,
	}
}
