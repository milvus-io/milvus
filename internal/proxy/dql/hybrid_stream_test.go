package dql

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestHybridStreamingEligibility(t *testing.T) {
	task := newQueryViewSearchTask(nil, 1)
	task.IsIterator, task.IsAdvanced = false, true
	task.rerankMeta = &legacyRerankMeta{}
	task.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 4, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
	require.True(t, task.supportsStreamingReduce())
	task.SubReqs[0].SearchType = internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER
	require.True(t, task.supportsStreamingReduce())
	task.SubReqs[0].SearchType = internalpb.SearchType_DEFAULT
	require.False(t, task.supportsStreamingReduce())
	task.SubReqs[0].SearchType = internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER
	task.SubReqs[0].GroupByFieldId = 101
	require.False(t, task.supportsStreamingReduce())
	task.SubReqs[0].GroupByFieldId = 0
	task.rerankMeta = &functionChainRerankMeta{}
	require.False(t, task.supportsStreamingReduce())
}

func TestSearchTaskConsumesHybridStreamsWithIndependentWindows(t *testing.T) {
	firstChunk := newFinalSearchChunk([]int64{10, 20, 30, 40}, []float32{.9, .8, .7, .6}, "first")
	secondChunk := newFinalSearchChunk([]int64{50, 60}, []float32{-.1, -.2}, "second")
	secondChunk.MetricType = "L2"
	firstChunk.FeatureBits, secondChunk.FeatureBits = 1, 2
	first := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: firstChunk}}}
	second := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: secondChunk}}}
	legacy := &fakeLegacyQueryClient{
		reduceStreamEnabled: true,
		searchResult:        &queryclient.LegacySearchResult{SubStreams: []searchutil.ReduceStream{first, second}},
	}
	task := newQueryViewSearchTask(legacy, 1)
	task.IsIterator, task.IsAdvanced = false, true
	task.rerankMeta = &legacyRerankMeta{}
	task.SubReqs = []*internalpb.SubSearchRequest{
		{Nq: 1, Topk: 4, Offset: 1, MetricType: "IP", SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER},
		{Nq: 1, Topk: 2, MetricType: "L2", SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER},
	}
	require.NoError(t, task.Execute(context.Background()))
	require.Empty(t, task.resultBuf.Collect())
	require.Nil(t, task.reducedResult)
	require.Len(t, task.reducedSubResults, 2)
	require.Equal(t, []int64{20, 30, 40}, task.reducedSubResults[0].GetResults().GetIds().GetIntId().GetData())
	require.Equal(t, []int64{50, 60}, task.reducedSubResults[1].GetResults().GetIds().GetIntId().GetData())
	require.Equal(t, []float32{.1, .2}, task.reducedSubResults[1].GetResults().GetScores())
	require.Equal(t, []string{"IP", "L2"}, task.reducedSubMetrics)
	require.EqualValues(t, 3, task.reducedFeatureBits)
	require.Equal(t, 1, first.closeCalls)
	require.Equal(t, 1, second.closeCalls)
}

type blockedHybridSearchStream struct {
	closed chan struct{}
	once   sync.Once
}

func (s *blockedHybridSearchStream) Recv() (*internalpb.SearchResults, error) {
	<-s.closed
	return nil, io.ErrClosedPipe
}

func (s *blockedHybridSearchStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (*blockedHybridSearchStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, errors.New("not implemented")
}

func TestHybridStreamFailureUnblocksSibling(t *testing.T) {
	failure := errors.New("failed sub-search")
	failed := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{err: failure}}}
	blocked := &blockedHybridSearchStream{closed: make(chan struct{})}
	task := newQueryViewSearchTask(nil, 1)
	task.IsAdvanced = true
	task.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 2}, {Nq: 1, Topk: 2}}
	done := make(chan error, 1)
	go func() {
		_, _, err := task.consumeHybridResultStreams([]searchutil.ReduceStream{failed, blocked}, func(*internalpb.SearchResults) {})
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, failure)
	case <-time.After(5 * time.Second):
		blocked.Close()
		t.Fatal("Hybrid failure did not unblock its sibling stream")
	}
	require.Equal(t, 1, failed.closeCalls)
}

func (s *SearchPipelineSuite) TestHybridStreamingHandoffMatchesBatch() {
	for _, strategy := range []string{"rrf", "weighted"} {
		s.Run(strategy, func() {
			task := getHybridSearchTask("test_collection", [][]string{{"1", "2"}, {"3", "4"}}, nil)
			params := `{"k":60}`
			if strategy == "weighted" {
				params = `{"weights":[0.3,0.7]}`
			}
			task.rerankMeta = newRerankMetaFromLegacy([]*commonpb.KeyValuePair{{Key: "strategy", Value: strategy}, {Key: "params", Value: params}})
			task.rankParams.limit, task.rankParams.offset = 3, 1
			task.SubReqs[0].Offset, task.SubReqs[1].Topk = 2, 5
			inputs := []*internalpb.SearchResults{genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)}
			p, err := newPipeline(hybridSearchPipe, task)
			s.Require().NoError(err)
			s.Require().NoError(p.AddNodes(task, endNode))
			batch, _, err := p.Run(context.Background(), s.span, inputs, segcore.StorageCost{})
			s.Require().NoError(err)
			op, err := newHybridSearchReduceOperator(task, nil)
			s.Require().NoError(err)
			reduced, err := op.run(context.Background(), s.span, inputs)
			s.Require().NoError(err)
			streamed, _, err := p.RunFromReduced(context.Background(), s.span, reduced[0].([]*milvuspb.SearchResults), reduced[1].([]string), segcore.StorageCost{})
			s.Require().NoError(err)
			s.Equal(batch.GetResults(), streamed.GetResults())
		})
	}
}
