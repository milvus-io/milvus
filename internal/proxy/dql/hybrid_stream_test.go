package dql

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestHybridStreamingEligibility(t *testing.T) {
	task := newQueryViewSearchTask(nil, 1)
	task.IsIterator, task.IsAdvanced = false, true
	task.rerankMeta = &legacyRerankMeta{}
	task.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 4, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
	require.True(t, task.supportsStreamingReduce())
	for _, kind := range []hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchStructEmbList} {
		task.hybridSubSearchInfos = []hybridSubSearchInfo{{Kind: kind}}
		for _, elementLevel := range []bool{false, true} {
			task.hybridElementLevel = elementLevel
			require.True(t, task.supportsStreamingReduce())
		}
	}
	task.GroupByFieldIds = []int64{101}
	require.False(t, task.supportsStreamingReduce())
	task.GroupByFieldIds = nil
	task.orderByFields = []OrderByField{{FieldID: 101}}
	require.False(t, task.supportsStreamingReduce())
	task.orderByFields = nil
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

func (s *SearchPipelineSuite) TestHybridElementStreamingMatchesBatch() {
	cases := []struct {
		name         string
		kinds        [2]hybridSubSearchKind
		elementLevel bool
		collapse     elementCollapseConfig
	}{
		{"native-elements", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchStructElement}, true, defaultElementCollapseConfig()},
		{"mixed-max", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchNormal}, false, defaultElementCollapseConfig()},
		{"different-parents", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchStructElement}, false, defaultElementCollapseConfig()},
		{"mixed-sum", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchNormal}, false, elementCollapseConfig{Strategy: elementCollapseSum}},
		{"mixed-avg", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchNormal}, false, elementCollapseConfig{Strategy: elementCollapseAvg}},
		{"mixed-topk-sum", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchNormal}, false, elementCollapseConfig{Strategy: elementCollapseTopKSum, TopK: 2}},
		{"mixed-topk-avg", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchNormal}, false, elementCollapseConfig{Strategy: elementCollapseTopKAvg, TopK: 2}},
		{"embedding-list", [2]hybridSubSearchKind{hybridSubSearchStructEmbList, hybridSubSearchStructEmbList}, false, defaultElementCollapseConfig()},
		{"element-and-list", [2]hybridSubSearchKind{hybridSubSearchStructElement, hybridSubSearchStructEmbList}, false, defaultElementCollapseConfig()},
	}
	for _, tc := range cases {
		for _, metric := range []string{"IP", "L2"} {
			if metric == "L2" && isElementCollapseSumFamily(tc.collapse.Strategy) {
				continue // The existing API rejects sum-family collapse for L2.
			}
			for _, strategy := range []string{"rrf", "weighted"} {
				for _, nq := range []int64{1, 2} {
					for _, emptyFirst := range []bool{false, true} {
						s.Run(fmt.Sprintf("%s/%s/%s/nq%d/empty%t", tc.name, metric, strategy, nq, emptyFirst), func() {
							task := getHybridSearchTask("test_collection", [][]string{{"1", "2"}, {"3", "4"}}, nil)
							task.Nq, task.hybridElementLevel = nq, tc.elementLevel
							params := `{"k":60}`
							if strategy == "weighted" {
								params = `{"weights":[0.3,0.7]}`
							}
							task.rerankMeta = newRerankMetaFromLegacy([]*commonpb.KeyValuePair{{Key: "strategy", Value: strategy}, {Key: "params", Value: params}})
							task.rankParams.limit, task.rankParams.offset, task.rankParams.roundDecimal = 2, 1, -1
							task.hybridSubSearchInfos = make([]hybridSubSearchInfo, 2)
							batch := make([]*milvuspb.SearchResults, 2)
							streams := make([]searchutil.ReduceStream, 2)
							for index, kind := range tc.kinds {
								task.SubReqs[index].Nq, task.SubReqs[index].Topk = nq, 4
								task.SubReqs[index].Offset = int64(index)
								task.SubReqs[index].MetricType, task.SubReqs[index].SearchType = metric, internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER
								task.hybridSubSearchInfos[index] = hybridSubSearchInfo{Kind: kind, Collapse: tc.collapse}
								request, err := viewquery.BuildSubSearchRequest(task.SearchRequest, task.SubReqs[index])
								s.Require().NoError(err)
								data := make([]*schemapb.SearchResultData, 2)
								children := make([]searchutil.ReduceStream, 2)
								for channel := range children {
									chunk := &internalpb.SearchResults{NumQueries: nq, TopK: 4, MetricType: metric, ResultData: &schemapb.SearchResultData{NumQueries: nq, TopK: 4, Ids: &schemapb.IDs{}}}
									for query := int64(0); query < nq; query++ {
										count := int64(3)
										if emptyFirst && index == 0 {
											count = 0
										}
										chunk.ResultData.Topks = append(chunk.ResultData.Topks, count)
										for row := int64(0); row < count; row++ {
											pk := 1 + int64(channel) + row*2
											if kind == hybridSubSearchStructElement && row < 2 {
												pk = 1 + int64(channel)
											}
											typeutil.AppendPKs(chunk.ResultData.Ids, pk+query*10)
											score := float32(1) - float32(channel)*.1 - float32(row)*.2
											if metric == "L2" {
												score -= 1.1
											}
											chunk.ResultData.Scores = append(chunk.ResultData.Scores, score)
											if kind == hybridSubSearchStructElement {
												if chunk.ResultData.ElementIndices == nil {
													chunk.ResultData.ElementIndices = &schemapb.LongArray{}
												}
												chunk.ResultData.ElementIndices.Data = append(chunk.ResultData.ElementIndices.Data, row%2)
											}
										}
									}
									data[channel] = proto.Clone(chunk.ResultData).(*schemapb.SearchResultData)
									chunks, err := searchutil.SplitSearchResult(chunk, 20)
									s.Require().NoError(err)
									// Duplicate worker replies must retain distinct elements of the same PK.
									workers := make([]searchutil.ReduceStream, 2)
									for worker := range workers {
										leaf := &fakeLegacySearchStream{}
										for _, part := range chunks {
											leaf.recv = append(leaf.recv, fakeLegacySearchRecv{chunk: part})
										}
										workers[worker] = leaf
									}
									children[channel], err = searchutil.NewShardReduceStream(request, workers, 20)
									s.Require().NoError(err)
								}
								batch[index], err = reduceSearchResultDataNoGroupBy(context.Background(), data, nq, 4, metric, schemapb.DataType_Int64, request.GetOffset())
								s.Require().NoError(err)
								streams[index], err = searchutil.NewReduceStream(request, children, 20)
								s.Require().NoError(err)
							}
							s.Require().True(task.supportsStreamingReduce())
							streamed, metrics, err := task.consumeHybridResultStreams(streams, func(*internalpb.SearchResults) {})
							s.Require().NoError(err)
							for i := range batch {
								if len(batch[i].GetResults().GetScores()) == 0 {
									s.Empty(streamed[i].GetResults().GetScores())
									s.Zero(typeutil.GetSizeOfIDs(streamed[i].GetResults().GetIds()))
									s.Equal(batch[i].GetResults().GetTopks(), streamed[i].GetResults().GetTopks())
									continue
								}
								s.True(proto.Equal(batch[i].GetResults(), streamed[i].GetResults()), "Batch: %v; Streaming: %v", batch[i], streamed[i])
							}
							pipeline, err := newPipeline(hybridSearchPipe, task)
							s.Require().NoError(err)
							s.Require().NoError(pipeline.AddNodes(task, endNode))
							expected, _, err := pipeline.RunFromReduced(context.Background(), s.span, batch, metrics, segcore.StorageCost{})
							s.Require().NoError(err)
							actual, _, err := pipeline.RunFromReduced(context.Background(), s.span, streamed, metrics, segcore.StorageCost{})
							s.Require().NoError(err)
							s.Equal(expected.GetResults(), actual.GetResults())
						})
					}
				}
			}
		}
	}
}
