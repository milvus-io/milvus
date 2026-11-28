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

package httpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryPagination(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	quota := &params.QuotaConfig.QuotaAndLimitsEnabled
	originalQuota := quota.GetValue()
	require.NoError(t, params.Save(quota.Key, "false"))
	t.Cleanup(func() { assert.NoError(t, params.Save(quota.Key, originalQuota)) })

	for _, test := range []struct {
		name       string
		body       string
		wantParams map[string]string
		wantError  string
	}{
		{
			name:      "negative limit",
			body:      `{"collectionName":"book","filter":"book_id > 0","limit":-1}`,
			wantError: "limit must be non-negative, got -1",
		},
		{
			name:      "negative limit with offset",
			body:      `{"collectionName":"book","filter":"book_id > 0","limit":-2,"offset":1}`,
			wantError: "limit must be non-negative, got -2",
		},
		{
			name:      "negative offset",
			body:      `{"collectionName":"book","filter":"book_id > 0","limit":2,"offset":-1}`,
			wantError: "offset must be non-negative, got -1",
		},
		{
			name:      "zero limit with offset",
			body:      `{"collectionName":"book","filter":"book_id > 0","limit":0,"offset":1}`,
			wantError: "offset requires a positive limit",
		},
		{
			name:      "global count with offset",
			body:      `{"collectionName":"book","filter":"book_id > 0","outputFields":["count(*)"],"offset":1}`,
			wantError: "count entities with pagination is not allowed",
		},
		{
			name:      "global count with explicit default-sized limit",
			body:      `{"collectionName":"book","filter":"book_id > 0","outputFields":["count(*)"],"limit":100}`,
			wantError: "count entities with pagination is not allowed",
		},
		{
			name:      "global mixed count with limit",
			body:      `{"collectionName":"book","filter":"book_id > 0","outputFields":["sum(word_count)","count(*)"],"limit":2}`,
			wantError: "count entities with pagination is not allowed",
		},
		{
			name:      "global mixed count with offset",
			body:      `{"collectionName":"book","filter":"book_id > 0","outputFields":["sum(word_count)","count(*)"],"offset":1}`,
			wantError: "count entities with pagination is not allowed",
		},
		{
			name:       "limit and offset",
			body:       `{"collectionName":"book","filter":"book_id > 0","limit":2,"offset":1}`,
			wantParams: map[string]string{proxy.LimitKey: "2", proxy.OffsetKey: "1"},
		},
		{
			name:       "zero limit",
			body:       `{"collectionName":"book","filter":"book_id > 0","limit":0}`,
			wantParams: map[string]string{},
		},
		{
			name:       "global count without pagination",
			body:       `{"collectionName":"book","filter":"book_id > 0","outputFields":["count(*)"]}`,
			wantParams: map[string]string{},
		},
		{
			name:       "global mixed count without pagination",
			body:       `{"collectionName":"book","filter":"book_id > 0","outputFields":["sum(word_count)","COUNT ( * )"]}`,
			wantParams: map[string]string{},
		},
		{
			name:       "global count with empty grouping fields",
			body:       `{"collectionName":"book","filter":"book_id > 0","outputFields":["count(*)"],"groupByFields":[" ",","]}`,
			wantParams: map[string]string{proxy.GroupByFieldsKey: " ,,"},
		},
		{
			name:       "global count with explicit zero pagination",
			body:       `{"collectionName":"book","filter":"book_id > 0","outputFields":["count(*)"],"limit":0,"offset":0}`,
			wantParams: map[string]string{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			var errCode int32
			if test.wantError == "" {
				mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
					CollectionName: DefaultCollectionName,
					Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
					ShardsNum:      ShardNumDefault,
					Status:         commonSuccessStatus,
				}, nil).Once()
				mp.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
					params := make(map[string]string, len(req.GetQueryParams()))
					for _, pair := range req.GetQueryParams() {
						params[pair.GetKey()] = pair.GetValue()
					}
					assert.Equal(t, test.wantParams, params)
					return &milvuspb.QueryResults{Status: commonSuccessStatus}, nil
				}).Once()
			} else {
				errCode = merr.Code(merr.ErrParameterInvalid)
			}
			validateTestCases(t, initHTTPServerV2(mp, false), []requestBodyTestCase{{
				path:        QueryAction,
				requestBody: []byte(test.body),
				errCode:     errCode,
				errMsg:      test.wantError,
			}}, false)
		})
	}
}

func TestQueryInvalidPaginationAccessLog(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, QueryAction), nil)
	limit := int32(-1)
	h := &HandlersV2{}
	_, err := h.query(context.Background(), c, &QueryReqV2{
		CollectionName: "book",
		Filter:         "book_id > 0",
		RequestedLimit: &limit,
	}, "inventory")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	entry := info.NewRestfulInfo(c)
	entry.InitReq()
	assert.Equal(t, "1100", entry.ErrorCode())
	assert.Equal(t, "inventory", entry.DbName())
	assert.Equal(t, "book", entry.CollectionName())
	assert.Equal(t, "book_id > 0", entry.Expression())
}
