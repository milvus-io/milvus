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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	DefaultPartitionName = "_default"
)

type rawTestCase struct {
	path    string
	errMsg  string
	errCode int32
}

type requestBodyTestCase struct {
	path        string
	requestBody []byte
	errMsg      string
	errCode     int32
}

type DefaultReq struct {
	DbName string `json:"dbName"`
}

func (DefaultReq) GetBase() *commonpb.MsgBase {
	return &commonpb.MsgBase{}
}

func (req *DefaultReq) GetDbName() string { return req.DbName }

func init() {
	paramtable.Init()
	streaming.SetupNoopWALForTest()
}

func sendReqAndVerify(t *testing.T, testEngine *gin.Engine, testName, method string, testcase requestBodyTestCase) {
	t.Run(testName, func(t *testing.T) {
		req := httptest.NewRequest(method, testcase.path, bytes.NewReader(testcase.requestBody))
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		returnBody := &ReturnErrMsg{}
		err := json.Unmarshal(w.Body.Bytes(), returnBody)
		assert.Nil(t, err)
		assert.Equal(t, testcase.errCode, returnBody.Code)
		if testcase.errCode != 0 {
			assert.Contains(t, returnBody.Message, testcase.errMsg)
		}
	})
}

func TestHTTPWrapper(t *testing.T) {
	postTestCases := []requestBodyTestCase{}
	postTestCasesTrace := []requestBodyTestCase{}
	ginHandler := gin.Default()
	app := ginHandler.Group("", genAuthMiddleWare(false))
	path := "/wrapper/post"
	app.POST(path, wrapperPost(func() any { return &DefaultReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	}))
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{}`),
	})
	path = "/wrapper/post/param"
	app.POST(path, wrapperPost(func() any { return &CollectionNameReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	}))
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{}`),
		errMsg:      "missing required parameters, error: Key: 'CollectionNameReq.CollectionName' Error:Field validation for 'CollectionName' failed on the 'required' tag",
		errCode:     1802, // ErrMissingRequiredParameters
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(``),
		errMsg:      "can only accept json format request, the request body should be nil, however {} is valid",
		errCode:     1801, // ErrIncorrectParameterFormat
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "book", "dbName"}`),
		errMsg:      "can only accept json format request, error: invalid character '}' after object key",
		errCode:     1801, // ErrIncorrectParameterFormat
	})
	path = "/wrapper/post/trace"
	app.POST(path, wrapperPost(func() any { return &DefaultReq{} }, wrapperTraceLog(func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	})))
	postTestCasesTrace = append(postTestCasesTrace, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
	})
	path = "/wrapper/post/trace/wrong"
	app.POST(path, wrapperPost(func() any { return &DefaultReq{} }, wrapperTraceLog(func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, merr.ErrCollectionNotFound
	})))
	postTestCasesTrace = append(postTestCasesTrace, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
	})
	path = "/wrapper/post/trace/call"
	app.POST(path, wrapperPost(func() any { return &DefaultReq{} }, wrapperTraceLog(func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return wrapperProxy(ctx, c, req, false, false, "", func(reqctx context.Context, req any) (any, error) {
			return nil, nil
		})
	})))
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			if testcase.errCode != 0 {
				returnBody := &ReturnErrMsg{}
				err := json.Unmarshal(w.Body.Bytes(), returnBody)
				assert.Nil(t, err)
				assert.Equal(t, testcase.errCode, returnBody.Code)
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
			fmt.Println(w.Body.String())
		})
	}

	for _, i := range []string{"1", "2", "3"} {
		paramtable.Get().Save(proxy.Params.CommonCfg.TraceLogMode.Key, i)
		for _, testcase := range postTestCasesTrace {
			t.Run("post"+testcase.path+"["+i+"]", func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
				w := httptest.NewRecorder()
				ginHandler.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
				if testcase.errCode != 0 {
					returnBody := &ReturnErrMsg{}
					err := json.Unmarshal(w.Body.Bytes(), returnBody)
					assert.Nil(t, err)
					assert.Equal(t, testcase.errCode, returnBody.Code)
					assert.Equal(t, testcase.errCode, returnBody.Code)
				}
				fmt.Println(w.Body.String())
			})
		}
	}
}

func TestGrpcWrapper(t *testing.T) {
	getTestCases := []rawTestCase{}
	getTestCasesNeedAuth := []rawTestCase{}
	needAuthPrefix := "/auth"
	ginHandler := gin.Default()
	app := ginHandler.Group("")
	appNeedAuth := ginHandler.Group(needAuthPrefix, genAuthMiddleWare(true))
	path := "/wrapper/grpc/-0"
	handle := func(reqctx context.Context, req any) (any, error) {
		return nil, nil
	}
	app.GET(path, func(c *gin.Context) {
		ctx := proxy.NewContextWithMetadata(c, "", DefaultDbName)
		wrapperProxy(ctx, c, &DefaultReq{}, false, false, "", handle)
	})
	appNeedAuth.GET(path, func(c *gin.Context) {
		username, _ := c.Get(ContextUsername)
		ctx := proxy.NewContextWithMetadata(c, username.(string), DefaultDbName)
		wrapperProxy(ctx, c, &milvuspb.DescribeCollectionRequest{}, true, false, "", handle)
	})
	getTestCases = append(getTestCases, rawTestCase{
		path: path,
	})
	getTestCasesNeedAuth = append(getTestCasesNeedAuth, rawTestCase{
		path: needAuthPrefix + path,
	})
	path = "/wrapper/grpc/01"
	handle = func(reqctx context.Context, req any) (any, error) {
		return nil, merr.ErrNeedAuthenticate // 1800
	}
	app.GET(path, func(c *gin.Context) {
		ctx := proxy.NewContextWithMetadata(c, "", DefaultDbName)
		wrapperProxy(ctx, c, &DefaultReq{}, false, false, "", handle)
	})
	appNeedAuth.GET(path, func(c *gin.Context) {
		username, _ := c.Get(ContextUsername)
		ctx := proxy.NewContextWithMetadata(c, username.(string), DefaultDbName)
		wrapperProxy(ctx, c, &milvuspb.DescribeCollectionRequest{}, true, false, "", handle)
	})
	getTestCases = append(getTestCases, rawTestCase{
		path:    path,
		errCode: 65535,
	})
	getTestCasesNeedAuth = append(getTestCasesNeedAuth, rawTestCase{
		path: needAuthPrefix + path,
	})
	path = "/wrapper/grpc/00"
	handle = func(reqctx context.Context, req any) (any, error) {
		return &milvuspb.BoolResponse{
			Status: commonSuccessStatus,
		}, nil
	}
	app.GET(path, func(c *gin.Context) {
		ctx := proxy.NewContextWithMetadata(c, "", DefaultDbName)
		wrapperProxy(ctx, c, &DefaultReq{}, false, false, "", handle)
	})
	appNeedAuth.GET(path, func(c *gin.Context) {
		username, _ := c.Get(ContextUsername)
		ctx := proxy.NewContextWithMetadata(c, username.(string), DefaultDbName)
		wrapperProxy(ctx, c, &milvuspb.DescribeCollectionRequest{}, true, false, "", handle)
	})
	getTestCases = append(getTestCases, rawTestCase{
		path: path,
	})
	getTestCasesNeedAuth = append(getTestCasesNeedAuth, rawTestCase{
		path: needAuthPrefix + path,
	})
	path = "/wrapper/grpc/10"
	handle = func(reqctx context.Context, req any) (any, error) {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
				Reason:    "",
			},
		}, nil
	}
	app.GET(path, func(c *gin.Context) {
		ctx := proxy.NewContextWithMetadata(c, "", DefaultDbName)
		wrapperProxy(ctx, c, &DefaultReq{}, false, false, "", handle)
	})
	appNeedAuth.GET(path, func(c *gin.Context) {
		username, _ := c.Get(ContextUsername)
		ctx := proxy.NewContextWithMetadata(c, username.(string), DefaultDbName)
		wrapperProxy(ctx, c, &milvuspb.DescribeCollectionRequest{}, true, false, "", handle)
	})
	getTestCases = append(getTestCases, rawTestCase{
		path:    path,
		errCode: 65535,
	})
	getTestCasesNeedAuth = append(getTestCasesNeedAuth, rawTestCase{
		path: needAuthPrefix + path,
	})

	for _, testcase := range getTestCases {
		t.Run("get"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, testcase.path, nil)
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			if testcase.errCode != 0 {
				returnBody := &ReturnErrMsg{}
				err := json.Unmarshal(w.Body.Bytes(), returnBody)
				assert.Nil(t, err)
				assert.Equal(t, testcase.errCode, returnBody.Code)
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
			fmt.Println(w.Body.String())
		})
	}

	for _, testcase := range getTestCasesNeedAuth {
		t.Run("get"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, testcase.path, nil)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			if testcase.errCode != 0 {
				returnBody := &ReturnErrMsg{}
				err := json.Unmarshal(w.Body.Bytes(), returnBody)
				assert.Nil(t, err)
				assert.Equal(t, testcase.errCode, returnBody.Code)
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
			fmt.Println(w.Body.String())
		})
	}

	path = "/wrapper/grpc/auth"
	app.GET(path, func(c *gin.Context) {
		wrapperProxy(context.Background(), c, &milvuspb.DescribeCollectionRequest{}, true, false, "", handle)
	})
	appNeedAuth.GET(path, func(c *gin.Context) {
		ctx := proxy.NewContextWithMetadata(c, "test", DefaultDbName)
		wrapperProxy(ctx, c, &milvuspb.LoadCollectionRequest{}, true, false, "", handle)
	})
	t.Run("check authorization", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		ginHandler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		returnBody := &ReturnErrMsg{}
		err := json.Unmarshal(w.Body.Bytes(), returnBody)
		assert.Nil(t, err)
		assert.Equal(t, int32(1800), returnBody.Code)
		assert.Equal(t, "user hasn't authenticated", returnBody.Message)
		fmt.Println(w.Body.String())

		paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
		req = httptest.NewRequest(http.MethodGet, needAuthPrefix+path, nil)
		req.SetBasicAuth("test", getDefaultRootPassword())
		w = httptest.NewRecorder()
		ginHandler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)
		err = json.Unmarshal(w.Body.Bytes(), returnBody)
		assert.Nil(t, err)
		assert.Equal(t, int32(65535), returnBody.Code)
		assert.Equal(t, "rpc error: code = PermissionDenied desc = PrivilegeLoad: permission deny to test in the `default` database", returnBody.Message)
		fmt.Println(w.Body.String())
	})
}

type headerTestCase struct {
	path    string
	headers map[string]string
	status  int
}

func TestTimeout(t *testing.T) {
	headerTestCases := []headerTestCase{}
	ginHandler := gin.Default()
	app := ginHandler.Group("")
	path := "/middleware/timeout/5"
	app.POST(path, timeoutMiddleware(func(c *gin.Context) {
		time.Sleep(5 * time.Second)
	}))
	headerTestCases = append(headerTestCases, headerTestCase{
		path: path, // wait 5s
	})
	headerTestCases = append(headerTestCases, headerTestCase{
		path:    path, // timeout 3s
		headers: map[string]string{mhttp.HTTPHeaderRequestTimeout: "3"},
		status:  http.StatusRequestTimeout,
	})
	path = "/middleware/timeout/31"
	app.POST(path, timeoutMiddleware(func(c *gin.Context) {
		time.Sleep(31 * time.Second)
	}))
	headerTestCases = append(headerTestCases, headerTestCase{
		path:   path, // timeout 30s
		status: http.StatusRequestTimeout,
	})
	headerTestCases = append(headerTestCases, headerTestCase{
		path:    path, // wait 32s
		headers: map[string]string{mhttp.HTTPHeaderRequestTimeout: "32"},
	})

	for _, testcase := range headerTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, nil)
			for key, value := range testcase.headers {
				req.Header.Set(key, value)
			}
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			if testcase.status == 0 {
				assert.Equal(t, http.StatusOK, w.Code)
			} else {
				assert.Equal(t, testcase.status, w.Code)
			}
			fmt.Println(w.Body.String())
		})
	}
}

func TestDocInDocOutCreateCollection(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mockProxy := &proxy.Proxy{}
	mockCreateCollection := mockey.Mock((*proxy.Proxy).CreateCollection).Return(commonSuccessStatus, nil).Build()
	defer mockCreateCollection.UnPatch()
	testEngine := initHTTPServerV2(mockProxy, false)
	path := versionalV2(CollectionCategory, CreateAction)

	const baseRequestBody = `{
		"collectionName": "doc_in_doc_out_demo",
		"schema": {
			"autoId": false,
			"enableDynamicField": false,
			"fields": [
				{
					"fieldName": "my_id",
					"dataType": "Int64",
					"isPrimary": true
				},
				{
					"fieldName": "document_content",
					"dataType": "VarChar",
					"elementTypeParams": {
						"max_length": "9000"
					}
				},
				{
					"fieldName": "sparse_vector_1",
					"dataType": "SparseFloatVector"
				}
			],
			"functions": %s
		}
	}`

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(fmt.Sprintf(baseRequestBody, `[
			{
				"name": "bm25_fn_1",
				"type": "BM25",
				"inputFieldNames": ["document_content"],
				"outputFieldNames": ["sparse_vector_1"]
			}
		]`)),
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(fmt.Sprintf(baseRequestBody, `[
			{
				"name": "bm25_fn_1",
				"type": "BM25_",
				"inputFieldNames": ["document_content"],
				"outputFieldNames": ["sparse_vector_1"]
			}
		]`)),
		errMsg:  "Unsupported function type: BM25_",
		errCode: 1100,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(fmt.Sprintf(baseRequestBody, `[
			{
				"name": "bm25_fn_1",
				"inputFieldNames": ["document_content"],
				"outputFieldNames": ["sparse_vector_1"]
			}
		]`)),
		errMsg:  "Unsupported function type:", // unprovided function type is empty string
		errCode: 1100,
	})

	for _, testcase := range postTestCases {
		sendReqAndVerify(t, testEngine, "post"+testcase.path, http.MethodPost, testcase)
	}
}

func TestDocInDocOutCreateCollectionQuickDisallowFunction(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mp := &proxy.Proxy{}
	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(CollectionCategory, CreateAction)

	const baseRequestBody = `{
		"collectionName": "doc_in_doc_out_demo",
		"dimension": 2,
		"idType": "Varchar",
		"schema": {
			"autoId": false,
			"enableDynamicField": false,
			"functions": [
				{
					"name": "bm25_fn_1",
					"type": "BM25",
					"inputFieldNames": ["document_content"],
					"outputFieldNames": ["sparse_vector_1"]
				}
			]
		}
	}`

	testcase := requestBodyTestCase{
		path:        path,
		requestBody: []byte(baseRequestBody),
		errMsg:      "functions are not supported for quickly create collection",
		errCode:     1100,
	}

	sendReqAndVerify(t, testEngine, "post"+testcase.path, http.MethodPost, testcase)
}

func TestDocInDocOutDescribeCollection(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}
	mockDescribeCollection := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateDocInDocOutCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()
	mockGetLoadState := mockey.Mock((*proxy.Proxy).GetLoadState).Return(&DefaultLoadStateResp, nil).Build()
	defer mockGetLoadState.UnPatch()
	mockDescribeIndex := mockey.Mock((*proxy.Proxy).DescribeIndex).Return(&DefaultDescIndexesReqp, nil).Build()
	defer mockDescribeIndex.UnPatch()
	mockListAliases := mockey.Mock((*proxy.Proxy).ListAliases).Return(&milvuspb.ListAliasesResponse{
		Status:  &StatusSuccess,
		Aliases: []string{DefaultAliasName},
	}, nil).Build()
	defer mockListAliases.UnPatch()
	testEngine := initHTTPServerV2(mp, false)
	testcase := requestBodyTestCase{
		path:        versionalV2(CollectionCategory, DescribeAction),
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
	}
	sendReqAndVerify(t, testEngine, testcase.path, http.MethodPost, testcase)
}

func TestDocInDocOutInsert(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mp := &proxy.Proxy{}
	mockDescribeCollection := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateDocInDocOutCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()
	mockInsert := mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	defer mockInsert.UnPatch()

	testEngine := initHTTPServerV2(mp, false)
	testcase := requestBodyTestCase{
		path:        versionalV2(EntityCategory, InsertAction),
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "varchar_field": "some text"}]}`),
	}

	sendReqAndVerify(t, testEngine, testcase.path, http.MethodPost, testcase)
}

func TestDocInDocOutInsertInvalid(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mp := &proxy.Proxy{}
	mockDescribeCollection := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateDocInDocOutCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()
	testEngine := initHTTPServerV2(mp, false)
	// invlaid insert request, will not be sent to proxy

	testcase := requestBodyTestCase{
		path:        versionalV2(EntityCategory, InsertAction),
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": {"1": 0.1}, "varchar_field": "some text"}]}`),
		errCode:     1804,
		errMsg:      "not allowed to provide input data for function output field",
	}

	sendReqAndVerify(t, testEngine, testcase.path, http.MethodPost, testcase)
}

func TestSearchWithRerank(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mp := &proxy.Proxy{}
	mockDescribeCollection := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, true, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()
	mockSearch := mockey.Mock((*proxy.Proxy).Search).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
		TopK:         int64(3),
		OutputFields: []string{FieldWordCount},
		FieldsData:   generateFieldData(),
		Ids:          generateIDs(schemapb.DataType_Int64, 3),
		Scores:       DefaultScores,
	}}, nil).Build()
	defer mockSearch.UnPatch()
	testEngine := initHTTPServerV2(mp, false)
	testcase := requestBodyTestCase{
		path: versionalV2(EntityCategory, SearchAction),
		requestBody: []byte(`{
                                         "collectionName": "book",
                                         "data": [[0.1, 0.2]],
                                         "limit": 4,
                                         "outputFields": ["word_count"],
                                         "functionScore": {
                                             "functions": [{
                                                  "name": "testRank",
                                                  "description": "",
                                                  "type": "Rerank",
                                                  "inputFieldNames": ["FieldWordCount"],
                                                  "params": {"name": "decay"}
                                          }]}}`),
	}
	sendReqAndVerify(t, testEngine, testcase.path, http.MethodPost, testcase)
}

func TestHybridSearchWithRerank(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mp := &proxy.Proxy{}
	mockDescribeCollection := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, true, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()
	mockHybridSearch := mockey.Mock((*proxy.Proxy).HybridSearch).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
		TopK:         int64(3),
		OutputFields: []string{FieldWordCount},
		FieldsData:   generateFieldData(),
		Ids:          generateIDs(schemapb.DataType_Int64, 3),
		Scores:       DefaultScores,
	}}, nil).Build()
	defer mockHybridSearch.UnPatch()
	testEngine := initHTTPServerV2(mp, false)

	queryTestCases := requestBodyTestCase{
		path:        versionalV2(EntityCategory, HybridSearchAction),
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}, {"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}], "functionScore": {"functions": [{"name": "testRank", "type": "Rerank", "inputFieldNames": ["FieldWordCount"], "params": {"name": "decay"}}]}}`),
	}
	sendReqAndVerify(t, testEngine, queryTestCases.path, http.MethodPost, queryTestCases)
}

func TestDocInDocOutSearch(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	mp := &proxy.Proxy{}
	mockDescribeCollection := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateDocInDocOutCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()
	mockSearch := mockey.Mock((*proxy.Proxy).Search).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
		TopK:         int64(3),
		OutputFields: []string{FieldWordCount},
		FieldsData:   generateFieldData(),
		Ids:          generateIDs(schemapb.DataType_Int64, 3),
		Scores:       DefaultScores,
	}}, nil).Build()
	defer mockSearch.UnPatch()
	testEngine := initHTTPServerV2(mp, false)

	testcase := requestBodyTestCase{
		path:        versionalV2(EntityCategory, SearchAction),
		requestBody: []byte(`{"collectionName": "book", "data": ["query data"], "limit": 4, "outputFields": ["word_count"]}`),
	}

	sendReqAndVerify(t, testEngine, testcase.path, http.MethodPost, testcase)
}

func TestCreateIndex(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}
	mockCreateIndex := mockey.Mock((*proxy.Proxy).CreateIndex).Return(commonSuccessStatus, nil).Build()
	defer mockCreateIndex.UnPatch()
	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(IndexCategory, CreateAction)
	// the previous format
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2", "params": {"index_type": "L2", "nlist": 10}}]}`),
	})
	// the current format
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2", "indexType": "L2", "params":{"nlist": 10}}]}`),
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestCompact(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}
	seqManualCompaction := mockey.Sequence(&milvuspb.ManualCompactionResponse{CompactionID: 1}, nil).
		Then(&milvuspb.ManualCompactionResponse{
			Status: &commonpb.Status{
				Code:   1100,
				Reason: "mock",
			},
		}, nil)
	mockey.Mock((*proxy.Proxy).ManualCompaction).Return(seqManualCompaction).Build()

	seqGetCompactionState := mockey.Sequence(&milvuspb.GetCompactionStateResponse{}, nil).
		Then(&milvuspb.GetCompactionStateResponse{
			Status: &commonpb.Status{
				Code:   1100,
				Reason: "mock",
			},
		}, nil)
	mockey.Mock((*proxy.Proxy).GetCompactionState).Return(seqGetCompactionState).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)

	path := versionalV2(CollectionCategory, CompactAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"invalid_name"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(CollectionCategory, CompactionStateAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"invalid_name"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestFlush(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}
	seqFlush := mockey.Sequence(&milvuspb.FlushResponse{}, nil).
		Then(&milvuspb.FlushResponse{
			Status: &commonpb.Status{
				Code:   1100,
				Reason: "mock",
			},
		}, nil)
	mockey.Mock((*proxy.Proxy).Flush).Return(seqFlush).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(CollectionCategory, FlushAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"invalid_name"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestDatabase(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}

	errorStatus := &commonpb.Status{Code: 1100, Reason: "mock"}

	// CreateDatabase: success -> error
	seqCreateDB := mockey.Sequence(commonSuccessStatus, nil).Then(errorStatus, nil)
	mockey.Mock((*proxy.Proxy).CreateDatabase).Return(seqCreateDB).Build()

	// DropDatabase: success -> error
	seqDropDB := mockey.Sequence(commonSuccessStatus, nil).Then(errorStatus, nil)
	mockey.Mock((*proxy.Proxy).DropDatabase).Return(seqDropDB).Build()

	// ListDatabases: success -> error
	seqListDB := mockey.Sequence(&milvuspb.ListDatabasesResponse{DbNames: []string{"a", "b", "c"}, DbIds: []int64{100, 101, 102}}, nil).
		Then(&milvuspb.ListDatabasesResponse{Status: errorStatus}, nil)
	mockey.Mock((*proxy.Proxy).ListDatabases).Return(seqListDB).Build()

	// DescribeDatabase: success -> error
	seqDescDB := mockey.Sequence(&milvuspb.DescribeDatabaseResponse{DbName: "test", DbID: 100}, nil).
		Then(&milvuspb.DescribeDatabaseResponse{Status: errorStatus}, nil)
	mockey.Mock((*proxy.Proxy).DescribeDatabase).Return(seqDescDB).Build()

	// AlterDatabase: 3 pairs of (success -> error) for AlterAction, DropPropertiesAction, AlterPropertiesAction
	seqAlterDB := mockey.Sequence(commonSuccessStatus, nil).Then(errorStatus, nil).
		Then(commonSuccessStatus, nil).Then(errorStatus, nil).
		Then(commonSuccessStatus, nil).Then(errorStatus, nil)
	mockey.Mock((*proxy.Proxy).AlterDatabase).Return(seqAlterDB).Build()

	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(DataBaseCategory, CreateAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"invalid_name"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(DataBaseCategory, DropAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"mock"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(DataBaseCategory, ListAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"mock"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(DataBaseCategory, DescribeAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"mock"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(DataBaseCategory, AlterAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"mock"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(DataBaseCategory, DropPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"mock"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(DataBaseCategory, AlterPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"test"}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName":"mock"}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestColletcionProperties(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}

	errorStatus := &commonpb.Status{Code: 1100, Reason: "mock"}
	// AlterCollection: 2 pairs of (success -> error) for AlterPropertiesAction and DropPropertiesAction
	seqAlterColl := mockey.Sequence(commonSuccessStatus, nil).Then(errorStatus, nil).
		Then(commonSuccessStatus, nil).Then(errorStatus, nil)
	mockey.Mock((*proxy.Proxy).AlterCollection).Return(seqAlterColl).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(CollectionCategory, AlterPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test", "properties":{"mmap": true}}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"mock", "properties":{"mmap": true}}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(CollectionCategory, DropPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test", "propertyKeys":["mmap"]}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"mock", "propertyKeys":["mmap"]}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestIndexProperties(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}

	errorStatus := &commonpb.Status{Code: 1100, Reason: "mock"}
	// AlterIndex: 2 pairs of (success -> error) for AlterPropertiesAction and DropPropertiesAction
	seqAlterIndex := mockey.Sequence(commonSuccessStatus, nil).Then(errorStatus, nil).
		Then(commonSuccessStatus, nil).Then(errorStatus, nil)
	mockey.Mock((*proxy.Proxy).AlterIndex).Return(seqAlterIndex).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(IndexCategory, AlterPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test", "indexName":"test", "properties":{"mmap": true}}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"mock", "indexName":"test", "properties":{"mmap": true}}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	path = versionalV2(IndexCategory, DropPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test","indexName":"test", "propertyKeys":["test"]}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"mock","indexName":"test", "propertyKeys":["test"]}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestCollectionFieldProperties(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}

	errorStatus := &commonpb.Status{Code: 1100, Reason: "mock"}
	// AlterCollectionField: success -> error
	seqAlterField := mockey.Sequence(commonSuccessStatus, nil).Then(errorStatus, nil)
	mockey.Mock((*proxy.Proxy).AlterCollectionField).Return(seqAlterField).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(CollectionFieldCategory, AlterPropertiesAction)
	// success
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"test", "fieldName":"test", "fieldParams":{"max_length": 100}}`),
	})
	// mock fail
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName":"mock", "fieldName":"test", "fieldParams":{"max_length": 100}}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func TestCreateCollection(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	postTestCases := []requestBodyTestCase{}
	mp := &proxy.Proxy{}

	// CreateCollection: 13 times success + 2 times error
	seqCreateColl := mockey.Sequence(commonSuccessStatus, nil).Times(13).Then(commonErrorStatus, nil).Times(2)
	mockey.Mock((*proxy.Proxy).CreateCollection).Return(seqCreateColl).Build()

	// CreateIndex: 6 times success + 2 times error
	seqCreateIndex := mockey.Sequence(commonSuccessStatus, nil).Times(6).Then(commonErrorStatus, nil).Times(2)
	mockey.Mock((*proxy.Proxy).CreateIndex).Return(seqCreateIndex).Build()

	// LoadCollection: 6 times success
	mockey.Mock((*proxy.Proxy).LoadCollection).Return(commonSuccessStatus, nil).Build()

	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	path := versionalV2(CollectionCategory, CreateAction)
	// quickly create collection
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
		errMsg:      "dimension is required for quickly create collection(default metric type: COSINE): invalid parameter[expected=collectionName & dimension][actual=collectionName]",
		errCode:     1100, // ErrParameterInvalid
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "idType": "Varchar",` +
			`"params": {"max_length": "256", "enableDynamicField": "false", "shardsNum": "2", "consistencyLevel": "Strong", "ttlSeconds": "3600"}}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "idType": "Varchar",` +
			`"params": {"max_length": "256", "enableDynamicField": false, "shardsNum": "2", "consistencyLevel": "Strong", "ttlSeconds": "3600"}}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "idType": "Varchar",` +
			`"params": {"max_length": 256, "enableDynamicField": false, "shardsNum": 2, "consistencyLevel": "Strong", "ttlSeconds": 3600}}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "idType": "Varchar",` +
			`"params": {"max_length": 256, "enableDynamicField": false, "shardsNum": 2, "consistencyLevel": "unknown", "ttlSeconds": 3600}}`),
		errMsg:  "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded: invalid parameter[expected=Strong, Session, Bounded, Eventually, Customized][actual=unknown]",
		errCode: 1100, // ErrParameterInvalid
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "idType": "unknown"}`),
		errMsg:      "idType can only be [Int64, VarChar], default: Int64: invalid parameter[expected=Int64, Varchar][actual=unknown]",
		errCode:     1100, // ErrParameterInvalid
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "metricType": "L2"}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": false, "elementTypeParams": {}},
                {"fieldName": "partition_field", "dataType": "VarChar", "isPartitionKey": true, "elementTypeParams": {"max_length": 256}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }, "params": {"partitionsNum": "32"}}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }}`),
		errMsg:  "invalid parameter, data type int64 is invalid(case sensitive).",
		errCode: 1100, // ErrParameterInvalid
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
				{"fieldName": "null_fid", "dataType": "Int64", "nullable": true},
				{"fieldName": "default_fid_bool", "dataType": "Bool", "defaultValue": true},
				{"fieldName": "default_fid_int8", "dataType": "Int8", "defaultValue": 10},
				{"fieldName": "default_fid_int16", "dataType": "Int16", "defaultValue": 10},
				{"fieldName": "default_fid_int32", "dataType": "Int32", "defaultValue": 10},
				{"fieldName": "default_fid_int64", "dataType": "Int64", "defaultValue": 10},
				{"fieldName": "default_fid_float32", "dataType": "Float", "defaultValue": 10},
				{"fieldName": "default_fid_double", "dataType": "Double", "defaultValue": 10},
				{"fieldName": "default_fid_varchar", "dataType": "VarChar", "defaultValue": "a"},
                {"fieldName": "word_count", "dataType": "Array", "elementDataType": "Int64", "elementTypeParams": {"max_capacity": 2}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }}`),
	})
	// dim should not be specified for SparseFloatVector field
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": false, "elementTypeParams": {}},
                {"fieldName": "partition_field", "dataType": "VarChar", "isPartitionKey": true, "elementTypeParams": {"max_length": 256}},
                {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {}}
            ]
        }, "params": {"partitionsNum": "32"}}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Array", "elementDataType": "int64", "elementTypeParams": {}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }}`),
		errMsg:  "invalid parameter, element data type int64 is invalid(case sensitive).",
		errCode: 1100, // ErrParameterInvalid
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64","isClusteringKey":true, "elementTypeParams": {}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }, "indexParams": [{"fieldName": "book_xxx", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "missing required parameters, error: `book_xxx` hasn't defined in schema",
		errCode: 1802,
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
		        "fields": [
		            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "isPartitionKey": true, "elementTypeParams": {}},
		            {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
		        ]
		    }, "params": {"partitionKeyIsolation": "true"}}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "metricType": "L2"}`),
		errMsg:      "",
		errCode:     65535,
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "",
		errCode: 65535,
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "metricType": "L2"}`),
		errMsg:      "",
		errCode:     65535,
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
            "fields": [
                {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
                {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
            ]
        }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "",
		errCode: 65535,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
	        "fields": [
	            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
	            {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
				{"fieldName": "default_fid", "dataType": "Bool", "defaultValue":10, "elementTypeParams": {}},
	            {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
	        ]
	    }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "convert defaultValue fail, err:cannot use \"10\"(type: float64) as bool default value: invalid parameter",
		errCode: 1100,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
	        "fields": [
	            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
	            {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
				{"fieldName": "default_fid", "dataType": "VarChar", "defaultValue":true, "elementTypeParams": {}},
	            {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
	        ]
	    }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "convert defaultValue fail, err:cannot use \"true\"(type: bool) as string default value: invalid parameter",
		errCode: 1100,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
	        "fields": [
	            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
	            {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
				{"fieldName": "default_fid", "dataType": "Int8", "defaultValue":"10", "elementTypeParams": {}},
	            {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
	        ]
	    }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "convert defaultValue fail, err:cannot use \"\"10\"(type: string) as int default value: invalid parameter",
		errCode: 1100,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
	        "fields": [
	            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
	            {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
				{"fieldName": "default_fid", "dataType": "Int64", "defaultValue":"10", "elementTypeParams": {}},
	            {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
	        ]
	    }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "convert defaultValue fail, err:cannot use \"10\"(type: string) as long default value: invalid parameter",
		errCode: 1100,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
	        "fields": [
	            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
	            {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
				{"fieldName": "default_fid", "dataType": "Float", "defaultValue":"10", "elementTypeParams": {}},
	            {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
	        ]
	    }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "convert defaultValue fail, err:cannot use \"10\"(type: string) as float default value: invalid parameter",
		errCode: 1100,
	})

	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "schema": {
	        "fields": [
	            {"fieldName": "book_id", "dataType": "Int64", "isPrimary": true, "elementTypeParams": {}},
	            {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
				{"fieldName": "default_fid", "dataType": "Double", "defaultValue":"10", "elementTypeParams": {}},
	            {"fieldName": "book_intro", "dataType": "SparseFloatVector", "elementTypeParams": {"dim": 2}}
	        ]
	    }, "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]}`),
		errMsg:  "convert defaultValue fail, err:cannot use \"10\"(type: string) as float default value: invalid parameter",
		errCode: 1100,
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path: path,
		requestBody: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "idType": "Varchar",` +
			`"params": {"max_length": 256, "enableDynamicField": 100, "shardsNum": 2, "consistencyLevel": "unknown", "ttlSeconds": 3600}}`),
		errMsg:  "parse enableDynamicField fail, err:strconv.ParseBool: parsing \"100\": invalid syntax",
		errCode: 65535,
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

func versionalV2(category string, action string) string {
	return "/v2/vectordb" + category + action
}

func initHTTPServerV2(proxy types.ProxyComponent, needAuth bool) *gin.Engine {
	h := NewHandlersV2(proxy)
	ginHandler := gin.Default()
	appV2 := ginHandler.Group("/v2/vectordb", genAuthMiddleWare(needAuth))
	h.RegisterRoutesToV2(appV2)

	return ginHandler
}

/**
|          path| ListDatabases | ShowCollections | HasCollection | DescribeCollection | GetLoadState | DescribeIndex | GetCollectionStatistics | GetLoadingProgress |
|collections   |			   |		1		 |				 |					  |				 |				 |						   |					|
|has?coll	   |			   |				 |		1		 |					  |				 |				 |						   |					|
|desc?coll	   |			   |				 |				 |			1		  |		1		 |		1		 |						   |					|
|stats?coll	   |			   |				 |				 |					  |				 |				 |			1			   |					|
|loadState?coll|			   |				 |				 |					  |		1		 |				 |						   |		1			|
|collections   |			   |		1		 |				 |					  |				 |				 |						   |					|
|has/coll/	   |			   |				 |		1		 |					  |				 |				 |						   |					|
|has/coll/default/|			   |				 |		1		 |					  |				 |				 |						   |					|
|has/coll/db/  |		1	   |				 |				 |					  |				 |				 |						   |					|
|desc/coll/	   |			   |				 |				 |			1		  |		1		 |		1		 |						   |					|
|stats/coll/   |			   |				 |				 |					  |				 |				 |			1			   |					|
|loadState/coll|			   |				 |				 |					  |		1		 |				 |						   |		1			|

|          		path| ShowPartitions | HasPartition | GetPartitionStatistics |
|partitions?coll	|		1		 |				|						 |
|has?coll&part		|				 |		1		|						 |
|stats?coll&part	|				 |				|			1			 |
|partitions/coll	|		1		 |				|						 |
|has/coll/part		|				 |		1		|						 |
|stats/coll/part	|				 |				|			1			 |

|		path| ListCredUsers | SelectUser |
|users		|		1		|			 |
|desc?user	|				|	  1		 |
|users		|		1		|			 |
|desc/user	|				|	  1		 |

|		path| SelectRole | SelectGrant |
|roles		|		1	 |			   |
|desc?role	|			 |	  1		   |
|roles		|		1	 |			   |
|desc/role	|			 |	  1		   |

|		path| DescribeCollection | DescribeIndex |
|indexes	|		0			 |		1		 |
|desc?index	|					 |		1		 |
|indexes	|		0			 |		1		 |
|desc/index	|					 |		1		 |

|		path| ListAliases | DescribeAlias |
|aliases	|		1	  |				  |
|desc?alias	|			  |		1		  |
|aliases	|		1	  |				  |
|desc/alias	|			  |		1		  |

*/

func TestMethodGet(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}

	// ShowCollections: 2 different returns
	seqShowColl := mockey.Sequence(&milvuspb.ShowCollectionsResponse{Status: &StatusSuccess}, nil).
		Then(&milvuspb.ShowCollectionsResponse{Status: &StatusSuccess, CollectionNames: []string{DefaultCollectionName}}, nil)
	mockey.Mock((*proxy.Proxy).ShowCollections).Return(seqShowColl).Build()

	// HasCollection: success -> error
	seqHasColl := mockey.Sequence(&milvuspb.BoolResponse{Status: &StatusSuccess, Value: true}, nil).
		Then(&milvuspb.BoolResponse{Status: commonErrorStatus}, nil)
	mockey.Mock((*proxy.Proxy).HasCollection).Return(seqHasColl).Build()

	// DescribeCollection: 2 success + 1 error
	seqDescColl := mockey.Sequence(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Times(2).Then(&milvuspb.DescribeCollectionResponse{Status: commonErrorStatus}, nil)
	mockey.Mock((*proxy.Proxy).DescribeCollection).Return(seqDescColl).Build()

	// GetLoadState: complex sequence
	seqGetLoadState := mockey.Sequence(&milvuspb.GetLoadStateResponse{Status: commonErrorStatus}, nil).
		Then(&DefaultLoadStateResp, nil).Times(4).
		Then(&milvuspb.GetLoadStateResponse{Status: &StatusSuccess, State: commonpb.LoadState_LoadStateNotExist}, nil).
		Then(&milvuspb.GetLoadStateResponse{Status: &StatusSuccess, State: commonpb.LoadState_LoadStateNotLoad}, nil)
	mockey.Mock((*proxy.Proxy).GetLoadState).Return(seqGetLoadState).Build()

	// DescribeIndex: complex sequence
	seqDescIndex := mockey.Sequence(&milvuspb.DescribeIndexResponse{Status: commonErrorStatus}, nil).
		Then(&DefaultDescIndexesReqp, nil).Times(3).
		Then(nil, merr.WrapErrIndexNotFoundForCollection(DefaultCollectionName)).
		Then(&milvuspb.DescribeIndexResponse{Status: merr.Status(merr.WrapErrIndexNotFoundForCollection(DefaultCollectionName))}, nil)
	mockey.Mock((*proxy.Proxy).DescribeIndex).Return(seqDescIndex).Build()

	// GetCollectionStatistics: 2 different returns
	seqGetCollStats := mockey.Sequence(&milvuspb.GetCollectionStatisticsResponse{
		Status: commonSuccessStatus,
		Stats:  []*commonpb.KeyValuePair{{Key: "row_count", Value: "0"}},
	}, nil).Then(&milvuspb.GetCollectionStatisticsResponse{
		Status: commonSuccessStatus,
		Stats:  []*commonpb.KeyValuePair{{Key: "row_count", Value: "abc"}},
	}, nil)
	mockey.Mock((*proxy.Proxy).GetCollectionStatistics).Return(seqGetCollStats).Build()

	// GetLoadingProgress: 3 different returns
	seqGetLoadProgress := mockey.Sequence(&milvuspb.GetLoadingProgressResponse{Status: commonSuccessStatus, Progress: int64(77)}, nil).
		Then(&milvuspb.GetLoadingProgressResponse{Status: commonSuccessStatus, Progress: int64(100)}, nil).
		Then(&milvuspb.GetLoadingProgressResponse{Status: commonErrorStatus}, nil)
	mockey.Mock((*proxy.Proxy).GetLoadingProgress).Return(seqGetLoadProgress).Build()

	// ShowPartitions
	mockey.Mock((*proxy.Proxy).ShowPartitions).Return(&milvuspb.ShowPartitionsResponse{
		Status:         &StatusSuccess,
		PartitionNames: []string{DefaultPartitionName},
	}, nil).Build()

	// HasPartition
	mockey.Mock((*proxy.Proxy).HasPartition).Return(&milvuspb.BoolResponse{Status: &StatusSuccess, Value: true}, nil).Build()

	// GetPartitionStatistics
	mockey.Mock((*proxy.Proxy).GetPartitionStatistics).Return(&milvuspb.GetPartitionStatisticsResponse{
		Status: commonSuccessStatus,
		Stats:  []*commonpb.KeyValuePair{{Key: "row_count", Value: "0"}},
	}, nil).Build()

	// ListCredUsers
	mockey.Mock((*proxy.Proxy).ListCredUsers).Return(&milvuspb.ListCredUsersResponse{
		Status:    &StatusSuccess,
		Usernames: []string{util.UserRoot},
	}, nil).Build()

	// SelectUser
	mockey.Mock((*proxy.Proxy).SelectUser).Return(&milvuspb.SelectUserResponse{
		Status: &StatusSuccess,
		Results: []*milvuspb.UserResult{
			{User: &milvuspb.UserEntity{Name: util.UserRoot}, Roles: []*milvuspb.RoleEntity{{Name: util.RoleAdmin}}},
		},
	}, nil).Build()

	// SelectRole
	mockey.Mock((*proxy.Proxy).SelectRole).Return(&milvuspb.SelectRoleResponse{
		Status:  &StatusSuccess,
		Results: []*milvuspb.RoleResult{{Role: &milvuspb.RoleEntity{Name: util.RoleAdmin}}},
	}, nil).Build()

	// SelectGrant
	mockey.Mock((*proxy.Proxy).SelectGrant).Return(&milvuspb.SelectGrantResponse{
		Status: &StatusSuccess,
		Entities: []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: util.RoleAdmin},
				Object:     &milvuspb.ObjectEntity{Name: "global"},
				ObjectName: "",
				DbName:     util.DefaultDBName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: util.UserRoot},
					Privilege: &milvuspb.PrivilegeEntity{Name: "*"},
				},
			},
		},
	}, nil).Build()

	// ListAliases: error -> empty -> with alias
	seqListAliases := mockey.Sequence(&milvuspb.ListAliasesResponse{Status: commonErrorStatus}, nil).
		Then(&milvuspb.ListAliasesResponse{Status: &StatusSuccess}, nil).
		Then(&milvuspb.ListAliasesResponse{Status: &StatusSuccess, Aliases: []string{DefaultAliasName}}, nil)
	mockey.Mock((*proxy.Proxy).ListAliases).Return(seqListAliases).Build()

	// DescribeAlias
	mockey.Mock((*proxy.Proxy).DescribeAlias).Return(&milvuspb.DescribeAliasResponse{
		Status: &StatusSuccess,
		Alias:  DefaultAliasName,
	}, nil).Build()

	// ListPrivilegeGroups
	mockey.Mock((*proxy.Proxy).ListPrivilegeGroups).Return(&milvuspb.ListPrivilegeGroupsResponse{
		Status:          &StatusSuccess,
		PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{{GroupName: "group1", Privileges: []*milvuspb.PrivilegeEntity{{Name: "*"}}}},
	}, nil).Build()

	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []rawTestCase{}
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, HasAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path:    versionalV2(CollectionCategory, HasAction),
		errMsg:  "",
		errCode: 65535,
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path:    versionalV2(CollectionCategory, DescribeAction),
		errMsg:  "",
		errCode: 65535,
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, StatsAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, StatsAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, LoadStateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, LoadStateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, LoadStateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path:    versionalV2(CollectionCategory, LoadStateAction),
		errCode: 100,
		errMsg:  "collection not found[collection=book]",
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, LoadStateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, HasAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, StatsAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(IndexCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(IndexCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(IndexCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(IndexCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(AliasCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(AliasCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PrivilegeGroupCategory, ListAction),
	})

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader([]byte(`{` +
				`"collectionName": "` + DefaultCollectionName + `",` +
				`"partitionName": "` + DefaultPartitionName + `",` +
				`"indexName": "` + DefaultIndexName + `",` +
				`"userName": "` + util.UserRoot + `",` +
				`"roleName": "` + util.RoleAdmin + `",` +
				`"aliasName": "` + DefaultAliasName + `",` +
				`"privilegeGroupName": "pg"` +
				`}`))
			req := httptest.NewRequest(http.MethodPost, testcase.path, bodyReader)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}
}

var commonSuccessStatus = &commonpb.Status{
	ErrorCode: commonpb.ErrorCode_Success,
	Code:      merr.Code(nil),
	Reason:    "",
}

var commonErrorStatus = &commonpb.Status{
	ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
	Reason:    "",
}

func TestMethodDelete(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := &proxy.Proxy{}

	mockey.Mock((*proxy.Proxy).DropCollection).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).DropPartition).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).DeleteCredential).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).DropRole).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).DropIndex).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).DropAlias).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).DropPrivilegeGroup).Return(commonSuccessStatus, nil).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []rawTestCase{}
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, DropAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, DropAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, DropAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, DropAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(IndexCategory, DropAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(AliasCategory, DropAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PrivilegeGroupCategory, DropAction),
	})
	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "partitionName": "` + DefaultPartitionName +
				`", "userName": "` + util.UserRoot + `", "roleName": "` + util.RoleAdmin + `", "indexName": "` + DefaultIndexName + `", "aliasName": "` + DefaultAliasName + `", "privilegeGroupName": "pg"}`))
			req := httptest.NewRequest(http.MethodPost, testcase.path, bodyReader)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
			fmt.Println(w.Body.String())
		})
	}
}

func TestMethodPost(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := &proxy.Proxy{}

	mockey.Mock((*proxy.Proxy).CreateCollection).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).RenameCollection).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).LoadCollection).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).ReleaseCollection).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).CreatePartition).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).LoadPartitions).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).ReleasePartitions).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).CreateCredential).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).UpdateCredential).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).OperateUserRole).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).CreateRole).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).OperatePrivilege).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).OperatePrivilegeV2).Return(commonSuccessStatus, nil).Build()
	// CreateIndex: 2 success + 1 error
	seqCreateIndex := mockey.Sequence(commonSuccessStatus, nil).Times(2).Then(commonErrorStatus, nil)
	mockey.Mock((*proxy.Proxy).CreateIndex).Return(seqCreateIndex).Build()
	mockey.Mock((*proxy.Proxy).CreateAlias).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).AlterAlias).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).CreatePrivilegeGroup).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).OperatePrivilegeGroup).Return(commonSuccessStatus, nil).Build()
	mockey.Mock((*proxy.Proxy).ImportV2).Return(&internalpb.ImportResponse{
		Status: commonSuccessStatus, JobID: "1234567890",
	}, nil).Build()
	mockey.Mock((*proxy.Proxy).ListImports).Return(&internalpb.ListImportsResponse{
		Status: &StatusSuccess,
		JobIDs: []string{"1", "2", "3", "4"},
		States: []internalpb.ImportJobState{
			internalpb.ImportJobState_Pending,
			internalpb.ImportJobState_Importing,
			internalpb.ImportJobState_Failed,
			internalpb.ImportJobState_Completed,
		},
		Reasons:         []string{"", "", "mock reason", ""},
		Progresses:      []int64{0, 30, 0, 100},
		CollectionNames: []string{"AAA", "BBB", "CCC", "DDD"},
	}, nil).Build()
	mockey.Mock((*proxy.Proxy).GetImportProgress).Return(&internalpb.GetImportProgressResponse{
		Status:   &StatusSuccess,
		State:    internalpb.ImportJobState_Completed,
		Reason:   "",
		Progress: 100,
	}, nil).Build()
	mockey.Mock((*proxy.Proxy).GetSegmentsInfo).Return(&internalpb.GetSegmentsInfoResponse{
		Status: &StatusSuccess,
		SegmentInfos: []*internalpb.SegmentInfo{
			{
				SegmentID:    3,
				CollectionID: 1,
				PartitionID:  2,
				NumRows:      1000,
				VChannel:     "ch-1",
				State:        commonpb.SegmentState_Flushed,
				Level:        commonpb.SegmentLevel_L1,
				IsSorted:     true,
				InsertLogs: []*internalpb.FieldBinlog{
					{FieldID: 0, LogIDs: []int64{1, 5, 9}},
					{FieldID: 1, LogIDs: []int64{2, 6, 10}},
					{FieldID: 100, LogIDs: []int64{3, 7, 11}},
					{FieldID: 101, LogIDs: []int64{4, 8, 12}},
				},
				DeltaLogs: []*internalpb.FieldBinlog{
					{FieldID: 100, LogIDs: []int64{13, 14, 15}},
				},
				StatsLogs: []*internalpb.FieldBinlog{
					{FieldID: 100, LogIDs: []int64{16}},
				},
			},
		},
	}, nil).Build()

	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []rawTestCase{}
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, RenameAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, LoadAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, RefreshLoadAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(CollectionCategory, ReleaseAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, LoadAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PartitionCategory, ReleaseAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, UpdatePasswordAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, GrantRoleAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(UserCategory, RevokeRoleAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, GrantPrivilegeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, RevokePrivilegeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, GrantPrivilegeActionV2),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(RoleCategory, RevokePrivilegeActionV2),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(IndexCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path:    versionalV2(IndexCategory, CreateAction),
		errMsg:  "",
		errCode: 65535,
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(AliasCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(AliasCategory, AlterAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(ImportJobCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(ImportJobCategory, ListAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(ImportJobCategory, GetProgressAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(ImportJobCategory, DescribeAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PrivilegeGroupCategory, CreateAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PrivilegeGroupCategory, AddPrivilegesToGroupAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(PrivilegeGroupCategory, RemovePrivilegesFromGroupAction),
	})
	queryTestCases = append(queryTestCases, rawTestCase{
		path: versionalV2(SegmentCategory, DescribeAction),
	})

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader([]byte(`{` +
				`"collectionName": "` + DefaultCollectionName + `", "newCollectionName": "test", "newDbName": "",` +
				`"partitionName": "` + DefaultPartitionName + `", "partitionNames": ["` + DefaultPartitionName + `"],` +
				`"schema": {"fields": [{"fieldName": "book_id", "dataType": "Int64", "elementTypeParams": {}}, {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}]},` +
				`"indexParams": [{"indexName": "` + DefaultIndexName + `", "fieldName": "book_intro", "metricType": "L2", "params": {"nlist": 30, "index_type": "IVF_FLAT"}}],` +
				`"userName": "` + util.UserRoot + `", "password": "Milvus", "newPassword": "milvus", "roleName": "` + util.RoleAdmin + `",` +
				`"roleName": "` + util.RoleAdmin + `", "objectType": "Global", "objectName": "*", "privilege": "*",` +
				`"privilegeGroupName": "pg", "privileges": ["create", "drop"],` +
				`"aliasName": "` + DefaultAliasName + `",` +
				`"jobId": "1234567890",` +
				`"files": [["book.json"]]` +
				`}`))
			req := httptest.NewRequest(http.MethodPost, testcase.path, bodyReader)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err)
			assert.Equal(t, testcase.errCode, returnBody.Code)
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
			fmt.Println(w.Body.String())
		})
	}
}

func validateTestCases(t *testing.T, testEngine *gin.Engine, queryTestCases []requestBodyTestCase, allowInt64 bool) {
	for i, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader(testcase.requestBody)
			req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, testcase.path), bodyReader)
			if allowInt64 {
				req.Header.Set(HTTPHeaderAllowInt64, "true")
			}
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code, "case %d: ", i, string(testcase.requestBody))
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err, "case %d: ", i)
			assert.Equal(t, testcase.errCode, returnBody.Code, "case: %d, request body: %s ", i, string(testcase.requestBody))
			if testcase.errCode != 0 {
				assert.Contains(t, returnBody.Message, testcase.errMsg, "case: %d, request body: %s", i, string(testcase.requestBody))
			}
			fmt.Println(w.Body.String())
		})
	}
}

func TestDML(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := &proxy.Proxy{}

	// DescribeCollection: 2 + 3 + 6 = 11 success, then 4 error, then 1 success (autoID=true)
	seqDescribeColl := mockey.Sequence(
		&milvuspb.DescribeCollectionResponse{
			CollectionName: DefaultCollectionName,
			Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
			ShardsNum:      ShardNumDefault,
			Status:         &StatusSuccess,
		}, nil,
	).Times(11).Then(
		&milvuspb.DescribeCollectionResponse{Status: commonErrorStatus}, nil,
	).Times(4).Then(
		&milvuspb.DescribeCollectionResponse{
			CollectionName: DefaultCollectionName,
			Schema:         generateCollectionSchema(schemapb.DataType_Int64, true, true),
			ShardsNum:      ShardNumDefault,
			Status:         &StatusSuccess,
		}, nil,
	)
	mockey.Mock((*proxy.Proxy).DescribeCollection).Return(seqDescribeColl).Build()

	// Query: custom logic with RunAndReturn -> use .To()
	mockey.Mock((*proxy.Proxy).Query).To(func(ctx context.Context, req *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		if matchCountRule(req.OutputFields) {
			for _, pair := range req.QueryParams {
				if pair.GetKey() == proxy.LimitKey {
					return nil, errors.New("mock error")
				}
			}
		}
		return &milvuspb.QueryResults{Status: commonSuccessStatus, OutputFields: []string{}, FieldsData: []*schemapb.FieldData{}}, nil
	}).Build()

	// Insert: IntId -> StrId
	seqInsert := mockey.Sequence(
		&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil,
	).Then(
		&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{}}}}}, nil,
	)
	mockey.Mock((*proxy.Proxy).Insert).Return(seqInsert).Build()

	// Upsert: IntId -> StrId -> IntId (for autoID=true case)
	seqUpsert := mockey.Sequence(
		&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil,
	).Then(
		&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{}}}}}, nil,
	).Then(
		&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil,
	)
	mockey.Mock((*proxy.Proxy).Upsert).Return(seqUpsert).Build()

	// Delete
	mockey.Mock((*proxy.Proxy).Delete).Return(&milvuspb.MutationResult{Status: commonSuccessStatus}, nil).Build()

	defer mockey.UnPatchAll()
	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []requestBodyTestCase{}
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        QueryAction,
		requestBody: []byte(`{"collectionName": "book", "filter": "book_id in [2, 4, 6, 8]", "outputFields": ["book_id",  "word_count", "book_intro"], "offset": 1}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        GetAction,
		requestBody: []byte(`{"collectionName": "book", "outputFields": ["book_id",  "word_count", "book_intro"]}`),
		errMsg:      "missing required parameters, error: Key: 'CollectionIDReq.ID' Error:Field validation for 'ID' failed on the 'required' tag",
		errCode:     1802, // ErrMissingRequiredParameters
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        QueryAction,
		requestBody: []byte(`{"collectionName": "book", "filter": "book_id in [2, 4, 6, 8]"}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        QueryAction,
		requestBody: []byte(`{"collectionName": "book", "filter": "", "outputFields": ["count(*)"], "limit": 10}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        QueryAction,
		requestBody: []byte(`{"collectionName": "book", "filter": "", "outputFields":  ["book_id",  "word_count", "book_intro"], "limit": 10, "consistencyLevel": "AAA"}`),
		errMsg:      "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded",
		errCode:     1100, // ErrParameterInvalid
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        GetAction,
		requestBody: []byte(`{"collectionName": "book", "id": [2, 4, 6, 8], "outputFields": ["book_id",  "word_count", "book_intro"], "consistencyLevel": "AAA"}`),
		errMsg:      "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded",
		errCode:     1100, // ErrParameterInvalid
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        InsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        InsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        UpsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        UpsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        DeleteAction,
		requestBody: []byte(`{"collectionName": "book", "filter": "book_id in [0]"}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        DeleteAction,
		requestBody: []byte(`{"collectionName": "book", "id" : [0]}`),
		errMsg:      "missing required parameters, error: Key: 'CollectionFilterReq.Filter' Error:Field validation for 'Filter' failed on the 'required' tag",
		errCode:     1802, // ErrMissingRequiredParameters
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        GetAction,
		requestBody: []byte(`{"collectionName": "book", "id": [2, 4, 6, 8, 0], "outputFields": ["book_id",  "word_count", "book_intro"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        GetAction,
		requestBody: []byte(`{"collectionName": "book", "id": [2, 4, 6, 8, "0"], "outputFields": ["book_id",  "word_count", "book_intro"]}`),
		errMsg:      "",
		errCode:     65535,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        InsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
		errMsg:      "",
		errCode:     65535,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        UpsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
		errMsg:      "",
		errCode:     65535,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        DeleteAction,
		requestBody: []byte(`{"collectionName": "book", "filter": "book_id in [0]"}`),
		errMsg:      "",
		errCode:     65535,
	})
	// upsert when autoid==true
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        UpsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})

	validateTestCases(t, testEngine, queryTestCases, false)
}

func TestAllowInt64(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := &proxy.Proxy{}

	mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []requestBodyTestCase{}
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        InsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        UpsertAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"book_id": 0, "word_count": 0, "book_intro": [0.11825, 0.6]}]}`),
	})

	validateTestCases(t, testEngine, queryTestCases, true)
}

func generateCollectionSchemaWithVectorFields() *schemapb.CollectionSchema {
	collSchema := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	binaryVectorField := generateVectorFieldSchema(schemapb.DataType_BinaryVector)
	binaryVectorField.Name = "binaryVector"
	float16VectorField := generateVectorFieldSchema(schemapb.DataType_Float16Vector)
	float16VectorField.Name = "float16Vector"
	bfloat16VectorField := generateVectorFieldSchema(schemapb.DataType_BFloat16Vector)
	bfloat16VectorField.Name = "bfloat16Vector"
	sparseFloatVectorField := generateVectorFieldSchema(schemapb.DataType_SparseFloatVector)
	sparseFloatVectorField.Name = "sparseFloatVector"
	int8VectorField := generateVectorFieldSchema(schemapb.DataType_Int8Vector)
	int8VectorField.Name = "int8Vector"
	collSchema.Fields = append(collSchema.Fields, binaryVectorField)
	collSchema.Fields = append(collSchema.Fields, float16VectorField)
	collSchema.Fields = append(collSchema.Fields, bfloat16VectorField)
	collSchema.Fields = append(collSchema.Fields, sparseFloatVectorField)
	collSchema.Fields = append(collSchema.Fields, int8VectorField)
	return collSchema
}

func TestFp16Bf16VectorsV2(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := &proxy.Proxy{}
	collSchema := generateCollectionSchemaWithVectorFields()

	mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         collSchema,
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	defer mockey.UnPatchAll()

	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []requestBodyTestCase{}
	for _, path := range []string{InsertAction, UpsertAction} {
		queryTestCases = append(queryTestCases,
			requestBodyTestCase{
				path: path,
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": [3.0],
							"bfloat16Vector": [4.4, 442],
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
				errCode: 1804,
				errMsg:  "fail to deal the insert data, error: []byte size 2 doesn't equal to vector dimension 2 of Float16Vector",
			}, requestBodyTestCase{
				path: path,
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": [3, 3.0],
							"bfloat16Vector": [4.4, 442],
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
			}, requestBodyTestCase{
				path: path,
				// [3, 3] shouble be converted to [float(3), float(3)]
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": [3, 3],
							"bfloat16Vector": [4.4, 442],
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
			}, requestBodyTestCase{
				path: path,
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": "AQIDBA==",
							"bfloat16Vector": "AQIDBA==",
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
			}, requestBodyTestCase{
				path: path,
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": [3, 3.0, 3],
							"bfloat16Vector": [4.4, 44],
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
				errMsg:  "fail to deal the insert data, error: []byte size 6 doesn't equal to vector dimension 2 of Float16Vector",
				errCode: 1804,
			}, requestBodyTestCase{
				path: path,
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": [3, 3.0],
							"bfloat16Vector": [4.4, 442, 44],
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
				errMsg:  "fail to deal the insert data, error: []byte size 6 doesn't equal to vector dimension 2 of BFloat16Vector",
				errCode: 1804,
			}, requestBodyTestCase{
				path: path,
				requestBody: []byte(
					`{
					"collectionName": "book",
					"data": [
						{
							"book_id": 0,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": "AQIDBA==",
							"bfloat16Vector": [4.4, 442],
							"sparseFloatVector": {"1": 0.1, "2": 0.44},
							"int8Vector": [1, 2]
						},
						{
							"book_id": 1,
							"word_count": 0,
							"book_intro": [0.11825, 0.6],
							"binaryVector": "AQ==",
							"float16Vector": [3.1, 3.1],
							"bfloat16Vector": "AQIDBA==",
							"sparseFloatVector": {"3": 1.1, "2": 0.44},
							"int8Vector": [1, 2]
						}
					]
				}`),
			})
	}
	validateTestCases(t, testEngine, queryTestCases, false)
}

func TestSearchV2(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
	mp := &proxy.Proxy{}
	collSchema := generateCollectionSchemaWithVectorFields()

	// DescribeCollection: 11 success (simple schema) -> 15 success (vector fields schema) -> 1 error
	seqDescribeColl := mockey.Sequence(
		&milvuspb.DescribeCollectionResponse{
			CollectionName: DefaultCollectionName,
			Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
			ShardsNum:      ShardNumDefault,
			Status:         &StatusSuccess,
		}, nil,
	).Times(11).Then(
		&milvuspb.DescribeCollectionResponse{
			CollectionName: DefaultCollectionName,
			Schema:         collSchema,
			ShardsNum:      ShardNumDefault,
			Status:         &StatusSuccess,
		}, nil,
	).Times(15).Then(
		&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				Code:   1100,
				Reason: "mock",
			},
		}, nil,
	)
	mockey.Mock((*proxy.Proxy).DescribeCollection).Return(seqDescribeColl).Build()

	// Search: 1 success with data -> 3 empty -> 1 error -> 3 empty
	seqSearch := mockey.Sequence(
		&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
			TopK:         int64(3),
			OutputFields: outputFields,
			FieldsData:   generateFieldData(),
			Ids:          generateIDs(schemapb.DataType_Int64, 3),
			Scores:       DefaultScores,
		}}, nil,
	).Then(
		&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: int64(0)}}, nil,
	).Times(3).Then(
		&milvuspb.SearchResults{Status: &commonpb.Status{
			ErrorCode: 1700, // ErrFieldNotFound
			Reason:    "groupBy field not found in schema: field not found[field=test]",
		}}, nil,
	).Then(
		&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: int64(0)}}, nil,
	).Times(3)
	mockey.Mock((*proxy.Proxy).Search).Return(seqSearch).Build()

	// HybridSearch: 1 success with data -> 5 empty
	seqHybridSearch := mockey.Sequence(
		&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
			TopK:         int64(3),
			OutputFields: outputFields,
			FieldsData:   generateFieldData(),
			Ids:          generateIDs(schemapb.DataType_Int64, 3),
			Scores:       DefaultScores,
		}}, nil,
	).Then(
		&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: int64(0)}}, nil,
	).Times(5)
	mockey.Mock((*proxy.Proxy).HybridSearch).Return(seqHybridSearch).Build()

	defer mockey.UnPatchAll()
	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []requestBodyTestCase{}
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in {list}", "exprParams":{"list": [2, 4, 6, 8]}, "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"],"consistencyLevel": "Strong"}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"radius":0.9}}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"radius":0.9, "range_filter": 0.1}, "groupingField": "word_count"}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"radius":0.9, "range_filter": 0.1}, "groupingField": "test"}`),
		errMsg:      "groupBy field not found in schema: field not found[field=test]",
		errCode:     65535,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [["0.1", "0.2"]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"radius":0.9, "range_filter": 0.1}, "groupingField": "test"}`),
		errMsg:      "can only accept json format request, error: Mismatch type float32 with value",
		errCode:     1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}, {"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}, {"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        HybridSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}, {"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3}], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})
	// annsField
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "annsField": "word_count", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"radius":0.9, "range_filter": 0.1}, "groupingField": "test"}`),
		errMsg:      "can only accept json format request, error: cannot find a vector field named: word_count",
		errCode:     1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [{"data": [[0.1, 0.2]], "annsField": "float_vector1", "metricType": "L2", "limit": 3}, {"data": [[0.1, 0.2]], "annsField": "float_vector2", "metricType": "L2", "limit": 3}], "rerank": {"strategy": "rrf", "params": {"k":  1}}}`),
		errMsg:      "can only accept json format request, error: cannot find a vector field named: float_vector1",
		errCode:     1801,
	})
	// multiple annsFields
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
		errMsg:      "can only accept json format request, error: search without annsField, but already found multiple vector fields: [book_intro, binaryVector,,,]",
		errCode:     1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "annsField": "book_intro", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "annsField": "binaryVector", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
		errMsg:      "can only accept json format request, error: Mismatch type uint8",
		errCode:     1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": ["AQ=="], "annsField": "binaryVector", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "filter": "book_id in {list}", "exprParams":{"list": [2, 4, 6, 8]},"metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": [[0.1, 0.23]], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": [[0.1, 0.43]], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})

	// -2, -1, 1, 3 should be float32
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": [[-2, -1]], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": [[1, 3]], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})
	// invalid fp32 vectors for fp16/bf16
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": [[0.23]], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": [[0.1, 0.43]], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errCode: 1801,
		errMsg:  "can only accept json format request, error: dimension: 2, but length of []float: 1: invalid parameter[expected=Float16Vector][actual=[[0.23]]]",
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": [[0.23, 4.4]], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": [[0.1]], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errCode: 1801,
		errMsg:  "can only accept json format request, error: dimension: 2, but length of []float: 1: invalid parameter[expected=BFloat16Vector][actual=[[0.1]]]",
	})

	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "consistencyLevel":"unknown","rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errMsg:  "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:parameter:'unknown' is incorrect, please check it: invalid parameter",
		errCode: 1100, // ErrParameterInvalid
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2, 0.3]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errMsg:  "can only accept json format request, error: dimension: 2, but length of []float: 3: invalid parameter[expected=FloatVector][actual=[[0.1, 0.2, 0.3]]]",
		errCode: 1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQID"], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errMsg:  "can only accept json format request, error: dimension: 8, bytesLen: 1, but length of []byte: 3: invalid parameter[expected=BinaryVector][actual=\x01\x02\x03]",
		errCode: 1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQID"], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errMsg:  "can only accept json format request, error: dimension: 2, bytesLen: 4, but length of []byte: 3: invalid parameter[expected=Float16Vector][actual=\x01\x02\x03]",
		errCode: 1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQID"], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errMsg:  "can only accept json format request, error: dimension: 2, bytesLen: 4, but length of []byte: 3: invalid parameter[expected=BFloat16Vector][actual=\x01\x02\x03]",
		errCode: 1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [{"1": 0.1}], "annsField": "sparseFloatVector", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "searchParams": {"params":"a"}}`),
		errMsg:      "searchParams.params must be a dict: invalid parameter",
		errCode:     1100, // ErrParameterInvalid
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"],"consistencyLevel": "unknown"}`),
		errMsg:      "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:parameter:'unknown' is incorrect, please check it: invalid parameter",
		errCode:     1100, // ErrParameterInvalid
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": ["AQ=="], "annsField": "binaryVector", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
		errMsg:      "mock",
		errCode:     1100, // ErrParameterInvalid
	})
	validateTestCases(t, testEngine, queryTestCases, false)
}

func TestGetQuotaMetrics(t *testing.T) {
	paramtable.Init()

	mp := &proxy.Proxy{}
	mockGetQuotaMetrics := mockey.Mock((*proxy.Proxy).GetQuotaMetrics).Return(&internalpb.GetQuotaMetricsResponse{
		Status:      &StatusSuccess,
		MetricsInfo: `{"proxy1": "1000", "proxy2": "2000"}`,
	}, nil).Build()
	defer mockGetQuotaMetrics.UnPatch()

	testEngine := initHTTPServerV2(mp, false)
	testcase := requestBodyTestCase{
		path:        DescribeAction,
		requestBody: []byte(`{}`),
	}

	bodyReader := bytes.NewReader(testcase.requestBody)
	req := httptest.NewRequest(http.MethodPost, versionalV2(QuotaCenterCategory, testcase.path), bodyReader)
	w := httptest.NewRecorder()
	testEngine.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "case %d: ", string(testcase.requestBody))
	returnBody := &ReturnErrMsg{}
	err := json.Unmarshal(w.Body.Bytes(), returnBody)
	assert.Nil(t, err)
	assert.Equal(t, testcase.errCode, returnBody.Code, "case: %d, request body: %s ", string(testcase.requestBody))
	if testcase.errCode != 0 {
		assert.Contains(t, returnBody.Message, testcase.errMsg, "case: %d, request body: %s", string(testcase.requestBody))
	}
	fmt.Println(w.Body.String())
}

type AddCollectionFieldSuite struct {
	suite.Suite
	testEngine *gin.Engine
	mp         *proxy.Proxy
}

func (s *AddCollectionFieldSuite) SetupSuite() {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
}

func (s *AddCollectionFieldSuite) TearDownSuite() {
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
}

func (s *AddCollectionFieldSuite) SetupTest() {
	s.mp = &proxy.Proxy{}
	s.testEngine = initHTTPServerV2(s.mp, false)
}

func (s *AddCollectionFieldSuite) TestAddCollectionFieldNormal() {
	addFieldTestCases := []requestBodyTestCase{
		{
			path:        versionalV2(CollectionFieldCategory, AddAction),
			requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Int64", "nullable": true, "elementTypeParams": {}}}`),
		},
		{
			path:        versionalV2(CollectionFieldCategory, AddAction),
			requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "VarChar", "nullable": true, "elementTypeParams": {"max_length": "256"}}}`),
		},
		{
			path:        versionalV2(CollectionFieldCategory, AddAction),
			requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Int64", "nullable": true, "defaultValue": 42}}`),
		},
		{
			path:        versionalV2(CollectionFieldCategory, AddAction),
			requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Array", "elementDataType": "Int64", "nullable": true}}`),
		},
	}
	mockey.Mock((*proxy.Proxy).AddCollectionField).Return(merr.Success(), nil).Build()
	defer mockey.UnPatchAll()
	validateRequestBodyTestCases(s.T(), s.testEngine, addFieldTestCases, false)
}

func (s *AddCollectionFieldSuite) TestAddCollectionFieldFail() {
	s.Run("bad_request", func() {
		addFieldTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "nullable": true, "elementTypeParams": {}}}`),
				errCode:     1802, // missing param
				errMsg:      "missing required parameters, error: Key: 'CollectionFieldReqWithSchema.Schema.DataType' Error:Field validation for 'DataType' failed on the 'required' tag",
			},
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Int64", "nullable": true, "defaultValue": "aaa"}}`),
				errCode:     1100, // invalid param
				errMsg:      "convert defaultValue fail, err: cannot use \"aaa\"(type: string) as long default value: invalid parameter: invalid parameter",
			},
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "LONGLONGLONGLONGTEXT", "nullable": true}}`),
				errCode:     1100, // invalid param
				errMsg:      "data type LONGLONGLONGLONGTEXT is invalid(case sensitive): invalid parameter",
			},
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Array", "nullable": true}}`),
				errCode:     1100, // invalid param
				errMsg:      "element data type  is invalid(case sensitive): invalid parameter",
			},
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Array", "elementDataType": "MYBLOB", "nullable": true}}`),
				errCode:     1100, // missing param
				errMsg:      "element data type MYBLOB is invalid(case sensitive): invalid parameter",
			},
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field"`),
				errCode:     1801, // bad request
				errMsg:      "can only accept json format request, error: unexpected EOF",
			},
		}
		validateRequestBodyTestCases(s.T(), s.testEngine, addFieldTestCases, false)
	})

	s.Run("server_error", func() {
		addFieldTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionFieldCategory, AddAction),
				requestBody: []byte(`{"collectionName": "book", "schema": {"fieldName": "new_field", "dataType": "Int64", "nullable": true, "elementTypeParams": {}}}`),
				errCode:     5,
				errMsg:      "service internal error: mock error",
			},
		}
		mockey.Mock((*proxy.Proxy).AddCollectionField).Return(merr.Status(merr.WrapErrServiceInternal("mock error")), nil).Build()
		defer mockey.UnPatchAll()

		validateRequestBodyTestCases(s.T(), s.testEngine, addFieldTestCases, false)
	})
}

func TestAddCollectionFieldSuite(t *testing.T) {
	suite.Run(t, new(AddCollectionFieldSuite))
}

func TestCollectionFunctionSuite(t *testing.T) {
	suite.Run(t, new(CollectionFunctionSuite))
}

type CollectionFunctionSuite struct {
	suite.Suite
	testEngine *gin.Engine
	mp         *proxy.Proxy
}

func (s *CollectionFunctionSuite) SetupSuite() {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
}

func (s *CollectionFunctionSuite) TearDownSuite() {
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
}

func (s *CollectionFunctionSuite) SetupTest() {
	s.mp = &proxy.Proxy{}
	s.testEngine = initHTTPServerV2(s.mp, false)
}

func (s *CollectionFunctionSuite) TestAddCollectionFunctionNormal() {
	s.Run("success", func() {
		addFunctionTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionCategory, AddFunctionAction),
				requestBody: []byte(`{"dbName": "db", "collectionName": "coll", "function": {"name": "test_function", "type": "TextEmbedding", "inputFieldNames": [], "OutputFieldNames": []}}`),
				errCode:     0,
				errMsg:      "",
			},
		}
		mockey.Mock((*proxy.Proxy).AddCollectionFunction).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Build()
		defer mockey.UnPatchAll()

		validateRequestBodyTestCases(s.T(), s.testEngine, addFunctionTestCases, false)
	})

	s.Run("bad_request", func() {
		addFunctionTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionCategory, AddFunctionAction),
				requestBody: []byte(`{"dbName": "db", "collectionName": "", "function": {"name": "test_function", "type": "BM25", "inputFieldNames": [], "OutputFieldNames": []}}`),
				errCode:     1802,
				errMsg:      "missing required parameters, error: Key: 'CollectionAddFunction.CollectionName' Error:Field validation for 'CollectionName' failed on the 'required' tag",
			},
			{
				path:        versionalV2(CollectionCategory, AddFunctionAction),
				requestBody: []byte(`invalid json`),
				errCode:     1801,
				errMsg:      "can only accept json format request, error: invalid character 'i' looking for beginning of value",
			},
		}
		validateRequestBodyTestCases(s.T(), s.testEngine, addFunctionTestCases, false)
	})
}

func (s *CollectionFunctionSuite) TestAlterCollectionFunctionNormal() {
	s.Run("success", func() {
		alterFunctionTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionCategory, AlterFunctionAction),
				requestBody: []byte(`{"dbName": "db", "collectionName": "coll", "functionName": "test_function", "function": {"name": "test_function", "type": "TextEmbedding", "inputFieldNames": [], "OutputFieldNames": []}}`),
				errCode:     0,
				errMsg:      "",
			},
		}
		mockey.Mock((*proxy.Proxy).AlterCollectionFunction).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Build()
		defer mockey.UnPatchAll()

		validateRequestBodyTestCases(s.T(), s.testEngine, alterFunctionTestCases, false)
	})

	s.Run("bad_request", func() {
		alterFunctionTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionCategory, AlterFunctionAction),
				requestBody: []byte(`{"dbName": "db", "collectionName": "", "functionName": "test_function", "function": {"name": "test_function", "type": "BM25", "inputFieldNames": [], "OutputFieldNames": []}}`),
				errCode:     1802,
				errMsg:      "missing required parameters, error: Key: 'CollectionAlterFunction.CollectionName' Error:Field validation for 'CollectionName' failed on the 'required' tag",
			},
			{
				path:        versionalV2(CollectionCategory, AlterFunctionAction),
				requestBody: []byte(`invalid json`),
				errCode:     1801,
				errMsg:      "can only accept json format request, error: invalid character 'i' looking for beginning of value",
			},
		}
		validateRequestBodyTestCases(s.T(), s.testEngine, alterFunctionTestCases, false)
	})
}

func (s *CollectionFunctionSuite) TestDropCollectionFunctionNormal() {
	s.Run("success", func() {
		addFunctionTestCases := []requestBodyTestCase{
			{
				path:        versionalV2(CollectionCategory, DropFunctionAction),
				requestBody: []byte(`{"dbName": "db", "collectionName": "coll", "functionName": "test"}`),
				errCode:     0,
				errMsg:      "",
			},
		}
		mockey.Mock((*proxy.Proxy).DropCollectionFunction).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Build()
		defer mockey.UnPatchAll()

		validateRequestBodyTestCases(s.T(), s.testEngine, addFunctionTestCases, false)
	})
}
