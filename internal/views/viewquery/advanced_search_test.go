//go:build test && dynamic

package viewquery

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

func TestBuildAndUpdateSubSearchRequest(t *testing.T) {
	parent := &internalpb.SearchRequest{
		CollectionID:            12,
		OutputFieldsId:          []int64{13},
		MvccTimestamp:           14,
		IsAdvanced:              true,
		CollectionTtlTimestamps: 17,
	}
	sub := &internalpb.SubSearchRequest{
		Dsl:                "dsl",
		PlaceholderGroup:   []byte{1},
		SerializedExprPlan: []byte{2},
		Nq:                 3,
		PartitionIDs:       []int64{4},
		Topk:               5,
		Offset:             6,
		MetricType:         metric.IP,
		GroupByFieldId:     7,
		GroupSize:          8,
		FieldId:            9,
		IgnoreGrowing:      true,
		AnalyzerName:       "analyzer",
		SearchType:         internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
	}
	parent.SubReqs = []*internalpb.SubSearchRequest{sub}

	req, err := BuildSubSearchRequest(parent, sub)
	require.NoError(t, err)
	assert.False(t, req.GetIsAdvanced())
	assert.Nil(t, req.GetSubReqs())
	assert.Equal(t, parent.GetCollectionID(), req.GetCollectionID())
	assert.Equal(t, parent.GetOutputFieldsId(), req.GetOutputFieldsId())
	assert.Equal(t, parent.GetMvccTimestamp(), req.GetMvccTimestamp())
	assert.Equal(t, parent.GetCollectionTtlTimestamps(), req.GetCollectionTtlTimestamps())
	assert.Equal(t, sub.GetPartitionIDs(), req.GetPartitionIDs())
	assert.Equal(t, sub.GetSerializedExprPlan(), req.GetSerializedExprPlan())
	assert.Equal(t, sub.GetPlaceholderGroup(), req.GetPlaceholderGroup())
	assert.Equal(t, sub.GetFieldId(), req.GetFieldId())
	assert.Equal(t, common.PkFilterNoPkFilter, req.GetPkFilter())

	req.PlaceholderGroup = []byte{20}
	req.SerializedExprPlan = []byte{21}
	req.PartitionIDs = []int64{22}
	req.MetricType = metric.BM25
	require.NoError(t, UpdateSubSearchRequest(sub, req, true))
	assert.Equal(t, []byte{20}, sub.GetPlaceholderGroup())
	assert.Equal(t, []byte{21}, sub.GetSerializedExprPlan())
	assert.Equal(t, []int64{22}, sub.GetPartitionIDs())
	assert.Equal(t, metric.BM25, sub.GetMetricType())
	assert.True(t, sub.GetSkip())
}

func TestAssembleAdvancedSearchResults(t *testing.T) {
	firstData := &schemapb.SearchResultData{NumQueries: 2, TopK: 3, Topks: []int64{1, 0}}
	secondData := &schemapb.SearchResultData{NumQueries: 2, TopK: 4, Topks: []int64{0, 1}}
	result := assembleAdvancedSearchResults([]*internalpb.SearchResults{
		{
			MetricType:         metric.IP,
			NumQueries:         2,
			TopK:               3,
			ResultData:         firstData,
			ChannelsMvcc:       map[string]uint64{"channel-1": 10},
			CostAggregation:    &internalpb.CostAggregation{ResponseTime: 5, TotalRelatedDataSize: 100},
			ScannedRemoteBytes: 20,
			ScannedTotalBytes:  30,
		},
		{
			MetricType:         metric.BM25,
			NumQueries:         2,
			TopK:               4,
			ResultData:         secondData,
			ChannelsMvcc:       map[string]uint64{"channel-2": 11},
			CostAggregation:    &internalpb.CostAggregation{ResponseTime: 8, ServiceTime: 7, TotalRelatedDataSize: 200},
			IsTopkReduce:       true,
			IsRecallEvaluation: true,
			ScannedRemoteBytes: 40,
			ScannedTotalBytes:  50,
		},
	})

	assert.True(t, result.GetIsAdvanced())
	assert.True(t, merr.Ok(result.GetStatus()))
	assert.True(t, result.GetIsTopkReduce())
	assert.True(t, result.GetIsRecallEvaluation())
	assert.Equal(t, map[string]uint64{"channel-1": 10, "channel-2": 11}, result.GetChannelsMvcc())
	assert.Equal(t, int64(60), result.GetScannedRemoteBytes())
	assert.Equal(t, int64(80), result.GetScannedTotalBytes())
	assert.Equal(t, int64(8), result.GetCostAggregation().GetResponseTime())
	assert.Equal(t, int64(7), result.GetCostAggregation().GetServiceTime())
	assert.Equal(t, int64(300), result.GetCostAggregation().GetTotalRelatedDataSize())
	require.Len(t, result.GetSubResults(), 2)
	assert.Equal(t, int64(0), result.GetSubResults()[0].GetReqIndex())
	assert.Same(t, firstData, result.GetSubResults()[0].GetResultData())
	assert.Equal(t, int64(1), result.GetSubResults()[1].GetReqIndex())
	assert.Same(t, secondData, result.GetSubResults()[1].GetResultData())
}

func TestServerSearchOnViewExecutesAdvancedSubSearches(t *testing.T) {
	provider := newAdvancedTaskProvider(3)
	scheduler := &advancedScheduler{}
	server := NewServer(provider, scheduler)
	req := advancedSearchOnViewRequest([]*internalpb.SubSearchRequest{
		{FieldId: 1, Nq: 2, Topk: 3, MetricType: metric.IP},
		{FieldId: 2, Nq: 2, Topk: 4, MetricType: metric.BM25, Skip: true},
		{FieldId: 3, Nq: 2, Topk: 5, MetricType: metric.L2},
	})

	resp, err := server.SearchOnView(context.Background(), req)
	require.NoError(t, err)
	result := resp.GetLegacyResults()
	require.True(t, result.GetIsAdvanced())
	require.Len(t, result.GetSubResults(), 3)
	for index, subResult := range result.GetSubResults() {
		assert.Equal(t, int64(index), subResult.GetReqIndex())
		assert.Equal(t, int64(2), subResult.GetNumQueries())
	}
	assert.Equal(t, metric.IP, result.GetSubResults()[0].GetMetricType())
	assert.Equal(t, metric.BM25, result.GetSubResults()[1].GetMetricType())
	assert.Equal(t, []int64{0, 0}, result.GetSubResults()[1].GetResultData().GetTopks())
	assert.Equal(t, metric.L2, result.GetSubResults()[2].GetMetricType())
	assert.ElementsMatch(t, []int64{1, 3}, provider.acquiredFields())
	assert.Equal(t, []int64{1}, scheduler.executedFields())
	assert.Equal(t, 1, provider.releaseCount(1))
	assert.Equal(t, 1, provider.releaseCount(3))
}

func TestServerSearchOnViewFailsAdvancedRequestWhenOneSubSearchFails(t *testing.T) {
	provider := newAdvancedTaskProvider()
	scheduler := &advancedScheduler{failField: 2}
	server := NewServer(provider, scheduler)
	req := advancedSearchOnViewRequest([]*internalpb.SubSearchRequest{
		{FieldId: 1, Nq: 1, Topk: 1, MetricType: metric.IP},
		{FieldId: 2, Nq: 1, Topk: 1, MetricType: metric.L2},
	})

	_, err := server.SearchOnView(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, codes.Unknown, status.Code(err))
	assert.Equal(t, 1, provider.releaseCount(1))
	assert.Equal(t, 1, provider.releaseCount(2))
}

func advancedSearchOnViewRequest(subReqs []*internalpb.SubSearchRequest) *viewpb.SearchOnViewRequest {
	return &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{
			CollectionID: 10,
			IsAdvanced:   true,
			SubReqs:      subReqs,
		},
		ShardId: testShardID().IntoProto(),
		Version: testVersion().IntoProto(),
		Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
	}
}

type advancedSearchTask struct {
	req *internalpb.SearchRequest
}

type advancedSearchTasks struct {
	task    *advancedSearchTask
	empty   bool
	release func()
}

func (t *advancedSearchTasks) Tasks() []SearchSegmentTask {
	if t.empty {
		return nil
	}
	return []SearchSegmentTask{t.task}
}

func (t *advancedSearchTasks) Release() {
	if t.release != nil {
		t.release()
	}
}

type advancedTaskProvider struct {
	mu            sync.Mutex
	emptyFields   map[int64]struct{}
	acquired      []int64
	releaseCounts map[int64]int
}

func newAdvancedTaskProvider(emptyFields ...int64) *advancedTaskProvider {
	provider := &advancedTaskProvider{
		emptyFields:   make(map[int64]struct{}, len(emptyFields)),
		releaseCounts: make(map[int64]int),
	}
	for _, fieldID := range emptyFields {
		provider.emptyFields[fieldID] = struct{}{}
	}
	return provider
}

func (p *advancedTaskProvider) AcquireSearchSegmentTasks(
	_ context.Context,
	_ qviews.ShardID,
	_ qviews.QueryViewVersion,
	_ *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (SearchSegmentTasks, error) {
	fieldID := req.GetFieldId()
	p.mu.Lock()
	p.acquired = append(p.acquired, fieldID)
	_, empty := p.emptyFields[fieldID]
	p.mu.Unlock()
	return &advancedSearchTasks{
		task:  &advancedSearchTask{req: req},
		empty: empty,
		release: func() {
			p.mu.Lock()
			p.releaseCounts[fieldID]++
			p.mu.Unlock()
		},
	}, nil
}

func (*advancedTaskProvider) AcquireQuerySegmentTasks(
	context.Context,
	qviews.ShardID,
	qviews.QueryViewVersion,
	*viewpb.QueryPlanMVCC,
	*internalpb.RetrieveRequest,
) (QuerySegmentTasks, error) {
	return nil, viewerror.NewUnknownError("unexpected query")
}

func (p *advancedTaskProvider) acquiredFields() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.acquired...)
}

func (p *advancedTaskProvider) releaseCount(fieldID int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.releaseCounts[fieldID]
}

type advancedScheduler struct {
	mu        sync.Mutex
	executed  []int64
	failField int64
}

func (s *advancedScheduler) Search(_ context.Context, tasks SearchSegmentTasks) (*internalpb.SearchResults, error) {
	task := tasks.Tasks()[0].(*advancedSearchTask)
	fieldID := task.req.GetFieldId()
	s.mu.Lock()
	s.executed = append(s.executed, fieldID)
	s.mu.Unlock()
	if fieldID == s.failField {
		return nil, viewerror.NewUnknownError("sub-search %d failed", fieldID)
	}
	return &internalpb.SearchResults{
		Status:     merr.Success(),
		MetricType: task.req.GetMetricType(),
		NumQueries: task.req.GetNq(),
		TopK:       task.req.GetTopk(),
		ResultData: &schemapb.SearchResultData{
			NumQueries: task.req.GetNq(),
			TopK:       task.req.GetTopk(),
			Topks:      make([]int64, int(task.req.GetNq())),
		},
	}, nil
}

func (*advancedScheduler) Query(context.Context, QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	return nil, viewerror.NewUnknownError("unexpected query")
}

func (s *advancedScheduler) executedFields() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.executed...)
}
