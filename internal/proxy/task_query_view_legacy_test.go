// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeLegacyViewQueryClient struct {
	legacy queryclient.LegacyClient
}

func (c *fakeLegacyViewQueryClient) Legacy() queryclient.LegacyClient {
	return c.legacy
}

type fakeLegacyQueryClient struct {
	searchCalled int
	queryCalled  int
	searchResult *queryclient.LegacySearchResult
	queryResult  *queryclient.LegacyQueryResult
	err          error
}

func (c *fakeLegacyQueryClient) Search(ctx context.Context, req *queryclient.LegacySearchRequest) (*queryclient.LegacySearchResult, error) {
	c.searchCalled++
	return c.searchResult, c.err
}

func (c *fakeLegacyQueryClient) Query(ctx context.Context, req *queryclient.LegacyQueryRequest) (*queryclient.LegacyQueryResult, error) {
	c.queryCalled++
	return c.queryResult, c.err
}

func TestQueryTaskExecuteUsesQueryViewLegacyClient(t *testing.T) {
	legacy := &fakeLegacyQueryClient{
		queryResult: &queryclient.LegacyQueryResult{
			Results: []*internalpb.RetrieveResults{
				{
					Base:   commonpbutil.NewMsgBase(commonpbutil.WithSourceID(101)),
					Status: merr.Success(),
				},
			},
		},
	}
	task := &queryTask{
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgID(1)),
			CollectionID: 1,
		},
		request:         &milvuspb.QueryRequest{DbName: "default"},
		viewQueryClient: &fakeLegacyViewQueryClient{legacy: legacy},
	}

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, legacy.queryCalled)
	require.Equal(t, 0, legacy.searchCalled)

	var sourceIDs []int64
	task.resultBuf.Range(func(result *internalpb.RetrieveResults) bool {
		sourceIDs = append(sourceIDs, result.GetBase().GetSourceID())
		return true
	})
	require.ElementsMatch(t, []int64{101}, sourceIDs)
}

func TestSearchTaskExecuteUsesQueryViewLegacyClient(t *testing.T) {
	legacy := &fakeLegacyQueryClient{
		searchResult: &queryclient.LegacySearchResult{
			Results: []*internalpb.SearchResults{
				{
					Base:   commonpbutil.NewMsgBase(commonpbutil.WithSourceID(202)),
					Status: merr.Success(),
				},
			},
		},
	}
	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgID(2)),
			CollectionID: 1,
			Nq:           1,
		},
		request:         &milvuspb.SearchRequest{DbName: "default"},
		resultBuf:       typeutil.NewConcurrentSet[*internalpb.SearchResults](),
		viewQueryClient: &fakeLegacyViewQueryClient{legacy: legacy},
	}

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, legacy.searchCalled)
	require.Equal(t, 0, legacy.queryCalled)

	var sourceIDs []int64
	task.resultBuf.Range(func(result *internalpb.SearchResults) bool {
		sourceIDs = append(sourceIDs, result.GetBase().GetSourceID())
		return true
	})
	require.ElementsMatch(t, []int64{202}, sourceIDs)
	require.Zero(t, task.queryChannelsNode.Len())
}
