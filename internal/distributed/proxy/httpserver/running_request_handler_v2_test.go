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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The four list filters are combined with AND, and every one of them left
// empty means "everything". The REST wrapper substitutes the default database
// whenever a request names none, which is right when the name says what to act
// on and wrong here, where it says what to match: it would quietly hide every
// other database's requests from an operator who asked about all of them.
func TestListRunningRequestsDatabaseIsAFilter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		wantDB string
	}{
		{"no database named lists every database", `{}`, ""},
		{"only other filters given still lists every database", `{"user": "alice"}`, ""},
		{"a database named is used as written", `{"dbName": "books"}`, "books"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *milvuspb.ListRunningRequestsRequest
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, req *milvuspb.ListRunningRequestsRequest) (*milvuspb.ListRunningRequestsResponse, error) {
					got = req
					return &milvuspb.ListRunningRequestsResponse{Status: merr.Success()}, nil
				}).Once()
			testEngine := initHTTPServerV2(mp, false)

			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, httptest.NewRequest(
				http.MethodPost,
				versionalV2(RunningRequestCategory, ListAction),
				bytes.NewReader([]byte(tc.body)),
			))
			assert.Equal(t, http.StatusOK, w.Code)
			require.NotNil(t, got, "the call never reached the proxy")
			assert.Equal(t, tc.wantDB, got.GetDbName())
		})
	}
}

// A cancel answers with three disjoint id lists. undetermined is the one that
// tells an operator to retry, so it must reach the HTTP body like the others.
func TestCancelRequestsReturnsEveryIDList(t *testing.T) {
	var got *milvuspb.CancelRequestsRequest
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().CancelRequests(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *milvuspb.CancelRequestsRequest) (*milvuspb.CancelRequestsResponse, error) {
			got = req
			return &milvuspb.CancelRequestsResponse{
				Status:       merr.Success(),
				Canceled:     []*milvuspb.RunningRequestInfo{{RequestId: "25-to03zu-rs"}},
				NotFound:     []string{"25-to03zu-rt"},
				Undetermined: []string{"26-to03zv-1"},
				NodeResults:  []*milvuspb.RunningRequestNodeResult{{NodeId: 7, Status: merr.Success()}},
			}, nil
		}).Once()
	testEngine := initHTTPServerV2(mp, false)

	w := httptest.NewRecorder()
	testEngine.ServeHTTP(w, httptest.NewRequest(
		http.MethodPost,
		versionalV2(RunningRequestCategory, CancelAction),
		bytes.NewReader([]byte(`{"requestIds": ["25-to03zu-rs", "25-to03zu-rt", "26-to03zv-1"]}`)),
	))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, got)
	assert.Equal(t, []string{"25-to03zu-rs", "25-to03zu-rt", "26-to03zv-1"}, got.GetRequestIds())

	var body struct {
		Code int32 `json:"code"`
		Data struct {
			Canceled     []map[string]any `json:"canceled"`
			NotFound     []string         `json:"notFound"`
			Undetermined []string         `json:"undetermined"`
			NodeResults  []map[string]any `json:"nodeResults"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body), w.Body.String())
	assert.Equal(t, int32(0), body.Code)
	require.Len(t, body.Data.Canceled, 1)
	assert.Equal(t, "25-to03zu-rs", body.Data.Canceled[0]["request_id"])
	assert.Equal(t, []string{"25-to03zu-rt"}, body.Data.NotFound)
	assert.Equal(t, []string{"26-to03zv-1"}, body.Data.Undetermined)
	assert.Len(t, body.Data.NodeResults, 1)
}

// The id a REST client reads from the list must reach the cancel unchanged
// even when the client decodes JSON numbers as doubles, as JavaScript does.
// With int64 ids around 4.7e17 that decoding moved an id by up to 32, and ids
// handed to one proxy are contiguous, so the cancel could stop another live
// request of the same user. A string id has no numeric form to round.
func TestRunningRequestIDSurvivesADoubleDecodingClient(t *testing.T) {
	const id = "25-to03zu-rs"
	var got *milvuspb.CancelRequestsRequest
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
		Status:   merr.Success(),
		Requests: []*milvuspb.RunningRequestInfo{{RequestId: id, ProxyId: 25, TaskIds: []int64{470062360166400033}}},
	}, nil).Once()
	mp.EXPECT().CancelRequests(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *milvuspb.CancelRequestsRequest) (*milvuspb.CancelRequestsResponse, error) {
			got = req
			return &milvuspb.CancelRequestsResponse{Status: merr.Success()}, nil
		}).Once()
	testEngine := initHTTPServerV2(mp, false)

	w := httptest.NewRecorder()
	testEngine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, versionalV2(RunningRequestCategory, ListAction), bytes.NewReader([]byte(`{}`))))
	require.Equal(t, http.StatusOK, w.Code)

	// decode the way a JavaScript client does: every number becomes a float64
	var listed map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &listed), w.Body.String())
	row := listed["data"].(map[string]any)["requests"].([]any)[0].(map[string]any)
	readBack := row["request_id"]
	require.Equal(t, id, readBack)

	cancelBody, err := json.Marshal(map[string]any{"requestIds": []any{readBack}})
	require.NoError(t, err)
	w = httptest.NewRecorder()
	testEngine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, versionalV2(RunningRequestCategory, CancelAction), bytes.NewReader(cancelBody)))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, got)
	assert.Equal(t, []string{id}, got.GetRequestIds())
}

// Numeric ids were the previous REST form. They are rejected rather than
// converted, so that no client keeps sending a rounded number.
func TestCancelRequestsRejectsNumericIDs(t *testing.T) {
	mp := mocks.NewMockProxy(t)
	testEngine := initHTTPServerV2(mp, false)

	w := httptest.NewRecorder()
	testEngine.ServeHTTP(w, httptest.NewRequest(
		http.MethodPost,
		versionalV2(RunningRequestCategory, CancelAction),
		bytes.NewReader([]byte(`{"requestIds": [470062360166400033]}`)),
	))
	var body struct {
		Code int32 `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body), w.Body.String())
	assert.NotEqual(t, int32(0), body.Code, w.Body.String())
}
