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

package delegator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// advancedSearchRequest is a hybrid search with two sub-requests, addressed to
// the cascade family's source.
func advancedSearchRequest() *querypb.SearchRequest {
	return &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			IsAdvanced: true,
			SubReqs: []*internalpb.SubSearchRequest{
				{Nq: 1, Topk: 10, MetricType: "L2"},
				{Nq: 1, Topk: 5, MetricType: "IP"},
			},
		},
		DmlChannels: []string{"v0"},
	}
}

// subResults returns one result per sub-request, tagged with the delegator
// that produced it, so the per-slot merge is observable.
func subResults(vchannel string, n int) []*internalpb.SearchResults {
	results := make([]*internalpb.SearchResults, 0, n)
	for i := 0; i < n; i++ {
		results = append(results, &internalpb.SearchResults{ChannelIDsSearched: []string{vchannel}})
	}
	return results
}

// An advanced search through a split source keeps one result per sub-request:
// every delegator of the family contributes to each slot, and the slot is
// reduced with that sub-request's own topk and metric.
func TestFrontedAdvancedSearchMergesEveryDelegatorPerSubRequest(t *testing.T) {
	paramtable.Init()

	searchMock := mockey.Mock((*shardDelegator).searchInternal).To(
		func(sd *shardDelegator, _ context.Context, _ *querypb.SearchRequest, _ splitReadScope) ([]*internalpb.SearchResults, error) {
			return subResults(sd.vchannelName, 2), nil
		}).Build()
	defer searchMock.UnPatch()

	var reducedTopks []int64
	reduceMock := mockey.Mock(segments.ReduceSearchOnQueryNode).To(
		func(_ context.Context, results []*internalpb.SearchResults, info *reduce.ResultInfo) (*internalpb.SearchResults, error) {
			reducedTopks = append(reducedTopks, info.GetTopK())
			merged := &internalpb.SearchResults{}
			for _, r := range results {
				merged.ChannelIDsSearched = append(merged.ChannelIDsSearched, r.GetChannelIDsSearched()...)
			}
			return merged, nil
		}).Build()
	defer reduceMock.UnPatch()

	family := newCascadeFamily(0, 0, 0, 0)
	results, err := family.source.Search(context.Background(), advancedSearchRequest())
	require.NoError(t, err)
	require.Len(t, results, 2, "one result per sub-request")
	for _, r := range results {
		assert.ElementsMatch(t, []string{"v0", "v1", "v2", "v3"}, r.GetChannelIDsSearched())
	}
	assert.Equal(t, []int64{10, 5}, reducedTopks)
}

func TestFrontedAdvancedSearchRefusesMalformedResults(t *testing.T) {
	paramtable.Init()

	cases := []struct {
		name      string
		search    func(sd *shardDelegator) ([]*internalpb.SearchResults, error)
		reduceErr error
		want      string
	}{
		{
			name: "source returns the wrong number of sub-results",
			search: func(sd *shardDelegator) ([]*internalpb.SearchResults, error) {
				if sd.vchannelName == "v0" {
					return subResults(sd.vchannelName, 1), nil
				}
				return subResults(sd.vchannelName, 2), nil
			},
			want: "expected 2 sub-requests",
		},
		{
			name: "a child fails",
			search: func(sd *shardDelegator) ([]*internalpb.SearchResults, error) {
				if sd.vchannelName == "v2" {
					return nil, errors.New("child down")
				}
				return subResults(sd.vchannelName, 2), nil
			},
			want: "fronting advanced search on split child v2 failed",
		},
		{
			name: "a child returns the wrong number of sub-results",
			search: func(sd *shardDelegator) ([]*internalpb.SearchResults, error) {
				if sd.vchannelName == "v1" {
					return subResults(sd.vchannelName, 3), nil
				}
				return subResults(sd.vchannelName, 2), nil
			},
			want: "split child v1 returned 3 sub-results",
		},
		{
			name: "the per-slot reduce fails",
			search: func(sd *shardDelegator) ([]*internalpb.SearchResults, error) {
				return subResults(sd.vchannelName, 2), nil
			},
			reduceErr: errors.New("reduce failed"),
			want:      "reduce failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			searchMock := mockey.Mock((*shardDelegator).searchInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.SearchRequest, _ splitReadScope) ([]*internalpb.SearchResults, error) {
					return tc.search(sd)
				}).Build()
			defer searchMock.UnPatch()
			reduceMock := mockey.Mock(segments.ReduceSearchOnQueryNode).To(
				func(_ context.Context, _ []*internalpb.SearchResults, _ *reduce.ResultInfo) (*internalpb.SearchResults, error) {
					if tc.reduceErr != nil {
						return nil, tc.reduceErr
					}
					return &internalpb.SearchResults{}, nil
				}).Build()
			defer reduceMock.UnPatch()

			family := newCascadeFamily(0, 0, 0, 0)
			_, err := family.source.Search(context.Background(), advancedSearchRequest())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A child that fails a plain read fails the whole read through its source, with
// the child named, for every read type.
func TestFrontedReadFailsWhenAChildFails(t *testing.T) {
	paramtable.Init()
	childErr := errors.New("child down")

	cases := []struct {
		name  string
		mock  func() *mockey.Mocker
		fetch func(ctx context.Context, source *shardDelegator) error
		want  string
	}{
		{"search", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).searchInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.SearchRequest, _ splitReadScope) ([]*internalpb.SearchResults, error) {
					if sd.vchannelName == "v1" {
						return nil, childErr
					}
					return nil, nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			_, err := source.Search(ctx, &querypb.SearchRequest{Req: &internalpb.SearchRequest{}, DmlChannels: []string{"v0"}})
			return err
		}, "fronting search on split child v1 failed"},
		{"query", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).queryInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.QueryRequest, _ splitReadScope) ([]*internalpb.RetrieveResults, error) {
					if sd.vchannelName == "v1" {
						return nil, childErr
					}
					return nil, nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			_, err := source.Query(ctx, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}, DmlChannels: []string{"v0"}})
			return err
		}, "fronting query on split child v1 failed"},
		{"query stream", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).queryStreamInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.QueryRequest, _ streamrpc.QueryStreamServer, _ splitReadScope) error {
					if sd.vchannelName == "v1" {
						return childErr
					}
					return nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			return source.QueryStream(ctx, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}, DmlChannels: []string{"v0"}}, nil)
		}, "fronting query stream on split child v1 failed"},
		{"statistics", func() *mockey.Mocker {
			return mockey.Mock((*shardDelegator).getStatisticsInternal).To(
				func(sd *shardDelegator, _ context.Context, _ *querypb.GetStatisticsRequest, _ splitReadScope) ([]*internalpb.GetStatisticsResponse, error) {
					if sd.vchannelName == "v1" {
						return nil, childErr
					}
					return nil, nil
				}).Build()
		}, func(ctx context.Context, source *shardDelegator) error {
			_, err := source.GetStatistics(ctx, &querypb.GetStatisticsRequest{Req: &internalpb.GetStatisticsRequest{}, DmlChannels: []string{"v0"}})
			return err
		}, "fronting statistics on split child v1 failed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.mock()
			defer m.UnPatch()

			family := newCascadeFamily(0, 0, 0, 0)
			err := tc.fetch(context.Background(), family.source)
			require.ErrorIs(t, err, childErr)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
