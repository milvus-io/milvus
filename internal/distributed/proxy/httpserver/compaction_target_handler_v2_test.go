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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompactionTargetRESTPreservesIDsAndRowPresence(t *testing.T) {
	const id int64 = 9_007_199_254_740_993
	const idJSON = "9007199254740993"
	proxy := mocks.NewMockProxy(t)
	proxy.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *milvuspb.CreateCompactionTargetRequest) (*milvuspb.CreateCompactionTargetResponse, error) {
			require.Equal(t, "records", req.GetCollectionName())
			require.Equal(t, []int64{id}, req.GetRewrite().GetSegmentIds())
			return &milvuspb.CreateCompactionTargetResponse{Status: merr.Success(), TargetId: id}, nil
		}).Once()
	proxy.EXPECT().DescribeCompactionTarget(mock.Anything, &milvuspb.DescribeCompactionTargetRequest{TargetId: id}).
		Return(&milvuspb.DescribeCompactionTargetResponse{
			Status: merr.Success(),
			Target: &milvuspb.CompactionTargetInfo{TargetId: id, RemainingRows: proto.Int64(0), CheckedAt: 42},
		}, nil).Once()
	proxy.EXPECT().ListCompactionTargets(mock.Anything, &milvuspb.ListCompactionTargetsRequest{}).
		Return(&milvuspb.ListCompactionTargetsResponse{
			Status: merr.Success(),
			Targets: []*milvuspb.CompactionTargetInfo{
				{TargetId: id},
			},
		}, nil).Once()
	engine := initHTTPServerV2(proxy, false)
	post := func(action string, input string) json.RawMessage {
		t.Helper()
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, versionalV2(CompactionTargetCategory, action), bytes.NewBufferString(input)))
		require.Equal(t, http.StatusOK, recorder.Code)
		var result struct {
			Code int32           `json:"code"`
			Data json.RawMessage `json:"data"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
		require.Zero(t, result.Code, recorder.Body.String())
		return result.Data
	}
	created := post(CreateAction, `{"collectionName":"records","rewrite":{"segmentIds":["`+idJSON+`"]}}`)
	require.JSONEq(t, `{"targetId":"`+idJSON+`","totalRows":"0"}`, string(created))

	var described map[string]any
	require.NoError(t, json.Unmarshal(post(DescribeAction, `{"targetId":"`+idJSON+`"}`), &described))
	require.Equal(t, idJSON, described["targetId"])
	require.Equal(t, "0", described["remainingRows"])
	require.Equal(t, "42", described["checkedAt"])

	var listed []map[string]any
	require.NoError(t, json.Unmarshal(post(ListAction, `{}`), &listed))
	require.Len(t, listed, 1)
	require.Equal(t, idJSON, listed[0]["targetId"])
	require.NotContains(t, listed[0], "remainingRows")
}

func TestCompactionTargetRESTRejectsInvalidFields(t *testing.T) {
	engine := initHTTPServerV2(mocks.NewMockProxy(t), false)
	for _, body := range []string{
		`{"collectionName":"records","rewrite":{},"optimize":{"inputTargetSizeBytes":"1024"}}`,
		`{"collectionName":"records","rewrite":{"partitionId":"17"}}`,
	} {
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, versionalV2(CompactionTargetCategory, CreateAction), bytes.NewBufferString(body)))
		require.Equal(t, http.StatusOK, recorder.Code)
		var result struct {
			Code int32 `json:"code"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
		require.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), result.Code)
	}
}
