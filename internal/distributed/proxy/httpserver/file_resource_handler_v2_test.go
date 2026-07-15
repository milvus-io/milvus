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

package httpserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type rejectingFileResourceLimiter struct {
	t      *testing.T
	checks []fileResourceLimiterCheck
}

type fileResourceLimiterCheck struct {
	rateType internalpb.RateType
	n        int
}

func (l *rejectingFileResourceLimiter) Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rateType internalpb.RateType, n int) error {
	l.t.Helper()
	require.Equal(l.t, util.InvalidDBID, dbID)
	require.Empty(l.t, collectionIDToPartIDs)
	l.checks = append(l.checks, fileResourceLimiterCheck{rateType: rateType, n: n})
	if n <= 0 {
		return nil
	}
	require.Equal(l.t, internalpb.RateType_DDLCollection, rateType)
	require.Equal(l.t, 1, n)
	return merr.ErrServiceRateLimit
}

func (l *rejectingFileResourceLimiter) Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rateType internalpb.RateType, n int) error {
	return l.Check(dbID, collectionIDToPartIDs, rateType, n)
}

func postFileResourceRequest(t *testing.T, server http.Handler, path, body string) map[string]interface{} {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code)

	response := make(map[string]interface{})
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	return response
}

func TestFileResourceHandlerV2(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().AddFileResource(mock.Anything, mock.MatchedBy(func(req *milvuspb.AddFileResourceRequest) bool {
		return req.GetName() == "synonyms" && req.GetPath() == "files/synonyms.txt"
	})).Return(merr.Success(), nil).Once()
	mockProxy.EXPECT().RemoveFileResource(mock.Anything, mock.MatchedBy(func(req *milvuspb.RemoveFileResourceRequest) bool {
		return req.GetName() == "synonyms"
	})).Return(merr.Success(), nil).Once()
	mockProxy.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(&milvuspb.ListFileResourcesResponse{
		Status: merr.Success(),
		Resources: []*milvuspb.FileResourceInfo{
			{Id: 100, Name: "synonyms", Path: "files/synonyms.txt"},
		},
	}, nil).Once()
	server := initHTTPServerV2(mockProxy, false)

	t.Run("validation", func(t *testing.T) {
		response := postFileResourceRequest(t, server, versionalV2(FileResourceCategory, AddAction), `{}`)
		assert.EqualValues(t, merr.Code(merr.ErrMissingRequiredParameters), response[HTTPReturnCode])
		assert.Contains(t, response[HTTPReturnMessage], "FileResourceReq.Name")
		assert.Contains(t, response[HTTPReturnMessage], "FileResourceReq.Path")

		response = postFileResourceRequest(t, server, versionalV2(FileResourceCategory, AddAction), `{"name":"synonyms"}`)
		assert.EqualValues(t, merr.Code(merr.ErrMissingRequiredParameters), response[HTTPReturnCode])
		assert.Contains(t, response[HTTPReturnMessage], "FileResourceReq.Path")

		response = postFileResourceRequest(t, server, versionalV2(FileResourceCategory, RemoveAction), `{}`)
		assert.EqualValues(t, merr.Code(merr.ErrMissingRequiredParameters), response[HTTPReturnCode])
		assert.Contains(t, response[HTTPReturnMessage], "FileResourceNameReq.Name")
	})

	t.Run("add", func(t *testing.T) {
		response := postFileResourceRequest(t, server, versionalV2(FileResourceCategory, AddAction), `{"name":"synonyms","path":"files/synonyms.txt"}`)
		assert.EqualValues(t, 0, response[HTTPReturnCode])
		assert.Empty(t, response[HTTPReturnData])
	})

	t.Run("list", func(t *testing.T) {
		response := postFileResourceRequest(t, server, versionalV2(FileResourceCategory, ListAction), `{}`)
		assert.EqualValues(t, 0, response[HTTPReturnCode])
		resources := response[HTTPReturnData].([]interface{})
		require.Len(t, resources, 1)
		resource := resources[0].(map[string]interface{})
		assert.EqualValues(t, 100, resource["id"])
		assert.Equal(t, "synonyms", resource["name"])
		assert.Equal(t, "files/synonyms.txt", resource["path"])
	})

	t.Run("remove", func(t *testing.T) {
		response := postFileResourceRequest(t, server, versionalV2(FileResourceCategory, RemoveAction), `{"name":"synonyms"}`)
		assert.EqualValues(t, 0, response[HTTPReturnCode])
		assert.Empty(t, response[HTTPReturnData])
	})
}

func TestFileResourceHandlerV2PropagatesErrors(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	testCases := []struct {
		name       string
		path       string
		body       string
		setup      func(*mocks.MockProxy)
		errCode    int32
		errMessage string
	}{
		{
			name: "add conflict",
			path: versionalV2(FileResourceCategory, AddAction),
			body: `{"name":"synonyms","path":"other.txt"}`,
			setup: func(proxy *mocks.MockProxy) {
				proxy.EXPECT().AddFileResource(mock.Anything, mock.Anything).Return(merr.Status(merr.WrapErrParameterInvalidMsg("file resource synonyms already exists")), nil)
			},
			errCode:    merr.Code(merr.ErrParameterInvalid),
			errMessage: "already exists",
		},
		{
			name: "remove in use",
			path: versionalV2(FileResourceCategory, RemoveAction),
			body: `{"name":"synonyms"}`,
			setup: func(proxy *mocks.MockProxy) {
				proxy.EXPECT().RemoveFileResource(mock.Anything, mock.Anything).Return(merr.Status(merr.WrapErrParameterInvalidMsg("file resource synonyms is still in use")), nil)
			},
			errCode:    merr.Code(merr.ErrParameterInvalid),
			errMessage: "still in use",
		},
		{
			name: "list failure",
			path: versionalV2(FileResourceCategory, ListAction),
			body: `{}`,
			setup: func(proxy *mocks.MockProxy) {
				proxy.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(&milvuspb.ListFileResourcesResponse{
					Status: merr.Status(merr.WrapErrParameterInvalidMsg("cannot list file resources")),
				}, nil)
			},
			errCode:    merr.Code(merr.ErrParameterInvalid),
			errMessage: "cannot list file resources",
		},
		{
			name: "sync unavailable",
			path: versionalV2(FileResourceCategory, AddAction),
			body: `{"name":"synonyms","path":"synonyms.txt"}`,
			setup: func(proxy *mocks.MockProxy) {
				proxy.EXPECT().AddFileResource(mock.Anything, mock.Anything).Return(merr.Status(merr.WrapErrServiceUnavailableMsg("file resource sync failed")), nil)
			},
			errCode:    merr.Code(merr.ErrServiceUnavailable),
			errMessage: "file resource sync failed",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mockProxy := mocks.NewMockProxy(t)
			testCase.setup(mockProxy)
			server := initHTTPServerV2(mockProxy, false)

			response := postFileResourceRequest(t, server, testCase.path, testCase.body)
			assert.EqualValues(t, testCase.errCode, response[HTTPReturnCode])
			assert.Contains(t, response[HTTPReturnMessage], testCase.errMessage)
		})
	}
}

func TestFileResourceHandlerV2ReturnsEmptyList(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(&milvuspb.ListFileResourcesResponse{
		Status: merr.Success(),
	}, nil).Once()
	server := initHTTPServerV2(mockProxy, false)

	response := postFileResourceRequest(t, server, versionalV2(FileResourceCategory, ListAction), `{}`)
	assert.EqualValues(t, 0, response[HTTPReturnCode])
	assert.Empty(t, response[HTTPReturnData].([]interface{}))
}

func TestFileResourceHandlerV2ChecksQuota(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	limiter := &rejectingFileResourceLimiter{t: t}
	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().GetRateLimiter().Return(limiter, nil).Times(3)
	mockProxy.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(&milvuspb.ListFileResourcesResponse{
		Status: merr.Success(),
	}, nil).Once()
	server := initHTTPServerV2(mockProxy, false)

	writeCases := []struct {
		path string
		body string
	}{
		{path: versionalV2(FileResourceCategory, AddAction), body: `{"name":"synonyms","path":"synonyms.txt"}`},
		{path: versionalV2(FileResourceCategory, RemoveAction), body: `{"name":"synonyms"}`},
	}
	for _, testCase := range writeCases {
		response := postFileResourceRequest(t, server, testCase.path, testCase.body)
		assert.EqualValues(t, merr.Code(merr.ErrHTTPRateLimit), response[HTTPReturnCode])
		assert.Contains(t, response[HTTPReturnMessage], merr.ErrServiceRateLimit.Error())
	}

	response := postFileResourceRequest(t, server, versionalV2(FileResourceCategory, ListAction), `{}`)
	assert.EqualValues(t, 0, response[HTTPReturnCode])
	assert.Empty(t, response[HTTPReturnData])

	require.Len(t, limiter.checks, 3)
	assert.Equal(t, fileResourceLimiterCheck{rateType: internalpb.RateType_DDLCollection, n: 1}, limiter.checks[0])
	assert.Equal(t, fileResourceLimiterCheck{rateType: internalpb.RateType_DDLCollection, n: 1}, limiter.checks[1])
	assert.Zero(t, limiter.checks[2].n)
}
