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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise JSON binding and the real hybrid REST handler together. The mock
// boundary checks the complete request sent to Proxy, not just the helper.
func TestHybridSearchFunctionChainsWithGroupParams(t *testing.T) {
	paramtable.Init()
	quotaKey := paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key
	paramtable.Get().Save(quotaKey, "false")
	t.Cleanup(func() { paramtable.Get().Reset(quotaKey) })

	for _, tc := range []struct {
		name         string
		groupJSON    string
		wantGroup    map[string]string
		invalidChain bool
		wantErr      string
	}{
		{name: "chains without grouping"},
		{
			name:      "group field with default size",
			groupJSON: `"groupingField":"word_count",`,
			wantGroup: map[string]string{ParamGroupByField: "word_count"},
		},
		{
			name:      "positive size and strict true",
			groupJSON: `"groupingField":"word_count","groupSize":2,"strictGroupSize":true,`,
			wantGroup: map[string]string{ParamGroupByField: "word_count", ParamGroupSize: "2", ParamStrictGroupSize: "true"},
		},
		{
			name:      "explicit strict false survives",
			groupJSON: `"groupingField":"word_count","groupSize":2,"strictGroupSize":false,`,
			wantGroup: map[string]string{ParamGroupByField: "word_count", ParamGroupSize: "2", ParamStrictGroupSize: "false"},
		},
		{
			name:      "strict true without explicit size",
			groupJSON: `"groupingField":"word_count","strictGroupSize":true,`,
			wantGroup: map[string]string{ParamGroupByField: "word_count", ParamStrictGroupSize: "true"},
		},
		{
			name:      "group field is trimmed",
			groupJSON: `"groupingField":" word_count ","groupSize":2,`,
			wantGroup: map[string]string{ParamGroupByField: "word_count", ParamGroupSize: "2"},
		},
		{
			name:      "zero size rejected with valid chains",
			groupJSON: `"groupingField":"word_count","groupSize":0,`,
			wantErr:   "groupSize must be a positive integer",
		},
		{
			name:      "negative size rejected with valid chains",
			groupJSON: `"groupingField":"word_count","groupSize":-1,`,
			wantErr:   "groupSize must be a positive integer",
		},
		{
			name:      "size requires group field despite chains",
			groupJSON: `"groupSize":2,`,
			wantErr:   "groupSize and strictGroupSize require groupingField",
		},
		{
			name:      "explicit strict false requires group field",
			groupJSON: `"strictGroupSize":false,`,
			wantErr:   "groupSize and strictGroupSize require groupingField",
		},
		{
			name:      "blank group field rejected",
			groupJSON: `"groupingField":"  ","groupSize":2,`,
			wantErr:   "groupSize and strictGroupSize require groupingField",
		},
		{
			name:         "invalid chain rejected with valid grouping",
			groupJSON:    `"groupingField":"word_count","groupSize":2,`,
			invalidChain: true,
			wantErr:      "unsupported function chain stage",
		},
		{
			name:         "chain error precedes group validation",
			groupJSON:    `"groupingField":"word_count","groupSize":0,`,
			invalidChain: true,
			wantErr:      "unsupported function chain stage",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			engine := initHTTPServerV2(mp, false)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         generateCollectionSchema(schemapb.DataType_Int64, true, true),
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()

			if tc.wantErr == "" {
				mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).RunAndReturn(
					func(_ context.Context, req *milvuspb.HybridSearchRequest) (*milvuspb.SearchResults, error) {
						wantRank := map[string]string{
							proxy.RankTypeKey: "", proxy.ParamsKey: "null",
							proxy.LimitKey: "2", proxy.OffsetKey: "1", ParamRoundDecimal: "-1",
						}
						for key, value := range tc.wantGroup {
							wantRank[key] = value
						}
						gotRank := make(map[string]string)
						for _, param := range req.GetRankParams() {
							_, duplicate := gotRank[param.GetKey()]
							assert.False(t, duplicate, "duplicate rank parameter %q", param.GetKey())
							gotRank[param.GetKey()] = param.GetValue()
						}
						assert.Equal(t, wantRank, gotRank)

						require.Len(t, req.GetFunctionChains(), 1)
						assert.True(t, proto.Equal(&schemapb.FunctionChain{
							Name: "hybrid_rerank", Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
							Ops: []*schemapb.FunctionChainOp{{
								Op: "merge",
								Params: map[string]*schemapb.FunctionParamValue{
									"strategy": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "rrf"}},
								},
							}},
						}, req.GetFunctionChains()[0]), "top-level chain must survive grouping validation")
						require.Len(t, req.GetRequests(), 2)
						require.Len(t, req.GetRequests()[0].GetFunctionChains(), 1)
						assert.True(t, proto.Equal(&schemapb.FunctionChain{
							Name: "sub_rerank", Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
							Ops: []*schemapb.FunctionChainOp{{
								Op: "limit",
								Params: map[string]*schemapb.FunctionParamValue{
									"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 3}},
								},
							}},
						}, req.GetRequests()[0].GetFunctionChains()[0]), "sub-search chain must retain its position")
						assert.Empty(t, req.GetRequests()[1].GetFunctionChains())
						return &milvuspb.SearchResults{
							Status: commonSuccessStatus, Results: &schemapb.SearchResultData{},
						}, nil
					}).Once()
			}

			stage := "FunctionChainStageL2Rerank"
			if tc.invalidChain {
				stage = "BadStage"
			}
			body := fmt.Sprintf(`{
				"collectionName":"hello_milvus","limit":2,"offset":1,
				%s
				"search":[
					{"data":[[0.1,0.2]],"annsField":"book_intro","limit":3,
					 "functionChains":[{"name":"sub_rerank","stage":"FunctionChainStageL1Rerank",
					  "ops":[{"op":"limit","params":{"limit":3}}]}]},
					{"data":[[0.1,0.2]],"annsField":"book_intro","limit":3}
				],
				"functionChains":[{"name":"hybrid_rerank","stage":"%s",
				 "ops":[{"op":"merge","params":{"strategy":"rrf"}}]}]
			}`, tc.groupJSON, stage)
			req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, HybridSearchAction), strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			var response ReturnErrMsg
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			if tc.wantErr == "" {
				assert.Equal(t, int32(0), response.Code, w.Body.String())
			} else {
				assert.Equal(t, merr.Code(merr.ErrParameterInvalid), response.Code, w.Body.String())
				assert.Contains(t, response.Message, tc.wantErr)
				mp.AssertNotCalled(t, "HybridSearch", mock.Anything, mock.Anything)
			}
		})
	}
}
