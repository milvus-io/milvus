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
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	DefaultCollectionName = "book"
	DefaultErrorMessage   = "UNKNOWN ERROR"

	ReturnSuccess     = 0
	ReturnFail        = 1
	ReturnWrongStatus = 2
	ReturnTrue        = 3
	ReturnFalse       = 4

	URIPrefixV1 = "/v1"
)

var StatusSuccess = commonpb.Status{
	ErrorCode: commonpb.ErrorCode_Success,
	Code:      merr.Code(nil),
	Reason:    "",
}

var ErrDefault = errors.New(DefaultErrorMessage)

var DefaultShowCollectionsResp = milvuspb.ShowCollectionsResponse{
	Status:          &StatusSuccess,
	CollectionNames: []string{DefaultCollectionName},
}

var DefaultDescCollectionResp = milvuspb.DescribeCollectionResponse{
	CollectionName: DefaultCollectionName,
	Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
	ShardsNum:      ShardNumDefault,
	Status:         &StatusSuccess,
}

var DefaultLoadStateResp = milvuspb.GetLoadStateResponse{
	Status: &StatusSuccess,
	State:  commonpb.LoadState_LoadStateLoaded,
}

var DefaultDescIndexesReqp = milvuspb.DescribeIndexResponse{
	Status:            &StatusSuccess,
	IndexDescriptions: generateIndexes(),
}

var DefaultTrueResp = milvuspb.BoolResponse{
	Status: &StatusSuccess,
	Value:  true,
}

var DefaultFalseResp = milvuspb.BoolResponse{
	Status: &StatusSuccess,
	Value:  false,
}

func getDefaultRootPassword() string {
	paramtable.Init()
	return paramtable.Get().CommonCfg.DefaultRootPassword.GetValue()
}

func versional(path string) string {
	return URIPrefixV1 + path
}

func initHTTPServer(proxy types.ProxyComponent, needAuth bool) *gin.Engine {
	h := NewHandlersV1(proxy)
	ginHandler := gin.Default()
	ginHandler.Use(func(c *gin.Context) {
		_, err := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		if err != nil {
			if paramtable.Get().HTTPCfg.AcceptTypeAllowInt64.GetAsBool() {
				c.Request.Header.Set(HTTPHeaderAllowInt64, "true")
			} else {
				c.Request.Header.Set(HTTPHeaderAllowInt64, "false")
			}
		}
		c.Next()
	})
	app := ginHandler.Group(URIPrefixV1, genAuthMiddleWare(needAuth))
	NewHandlersV1(h.proxy).RegisterRoutesToV1(app)
	return ginHandler
}

/*
	mock authentication

----|----------------|----------------|----------------

	|    username    |    password    |    result

----|----------------|----------------|----------------

	|""              |""              |fail
	|root            |Milvus          |success
	|root            |{not Milvus}    |fail
	|{not root}      |*               |success

----|----------------|----------------|----------------
*/
func genAuthMiddleWare(needAuth bool) gin.HandlerFunc {
	InitMockGlobalMetaCache()
	proxy.AddRootUserToAdminRole()
	if needAuth {
		return func(c *gin.Context) {
			// proxy.RemoveRootUserFromAdminRole()
			c.Set(ContextUsername, "")
			username, password, ok := ParseUsernamePassword(c)
			if !ok {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			} else if username == util.UserRoot && password != getDefaultRootPassword() {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			} else {
				c.Set(ContextUsername, username)
			}
		}
	}
	return func(c *gin.Context) {
		c.Set(ContextUsername, util.UserRoot)
	}
}

func InitMockGlobalMetaCache() {
	proxy.InitEmptyGlobalCache()
}

func Print(code int32, message string) string {
	return fmt.Sprintf("{\"%s\":%d,\"%s\":\"%s\"}", HTTPReturnCode, code, HTTPReturnMessage, message)
}

func PrintErr(err error) string {
	return Print(merr.Code(err), err.Error())
}

func CheckErrCode(errorStr string, err error) bool {
	prefix := fmt.Sprintf("{\"%s\":%d,\"%s\":\"%s", HTTPReturnCode, merr.Code(err), HTTPReturnMessage, err.Error())
	return strings.HasPrefix(errorStr, prefix)
}

func TestVectorAuthenticate(t *testing.T) {
	paramtable.Init()

	mockProxy := &proxy.Proxy{}
	mockShowCollections := mockey.Mock((*proxy.Proxy).ShowCollections).Return(&DefaultShowCollectionsResp, nil).Build()
	defer mockShowCollections.UnPatch()

	testEngine := initHTTPServer(mockProxy, true)

	t.Run("need authentication", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath), nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrNeedAuthenticate))
	})

	t.Run("username or password incorrect", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath), nil)
		req.SetBasicAuth(util.UserRoot, util.UserRoot)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrNeedAuthenticate))
	})

	t.Run("root's password correct", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath), nil)
		req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "{\"code\":200,\"data\":[\""+DefaultCollectionName+"\"]}", w.Body.String())
	})

	t.Run("username and password both provided", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath), nil)
		req.SetBasicAuth("test", util.UserRoot)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "{\"code\":200,\"data\":[\""+DefaultCollectionName+"\"]}", w.Body.String())
	})
}

func TestVectorListCollection(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}
	err := merr.WrapErrIoFailedReason("cannot create folder")

	testCases := []testCase{
		{
			name:       "show collections fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).ShowCollections).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "show collections fail with status",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).ShowCollections).Return(&milvuspb.ShowCollectionsResponse{
					Status: merr.Status(err),
				}, nil).Build()
			},
			expectedBody: PrintErr(err),
		},
		{
			name:       "list collections success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).ShowCollections).Return(&DefaultShowCollectionsResp, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":[\"" + DefaultCollectionName + "\"]}",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath), nil)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
		})
	}
}

type testCase struct {
	name         string
	mp           types.ProxyComponent
	mockSetup    func()
	exceptCode   int
	expectedBody string
	expectedErr  error
}

func TestVectorCollectionsDescribe(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "[share] describe coll fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "[share] collection not found",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "get load status fail",
			mp:         mp,
			exceptCode: http.StatusOK,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).GetLoadState).Return(nil, ErrDefault).Build()
				mockey.Mock((*proxy.Proxy).DescribeIndex).Return(&DefaultDescIndexesReqp, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamicField\":true,\"fields\":[{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"book_id\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"word_count\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"book_intro\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"" + DefaultIndexName + "\",\"metricType\":\"COSINE\"}],\"load\":\"\",\"shardsNum\":1}}",
		},
		{
			name:       "get indexes fail",
			mp:         mp,
			exceptCode: http.StatusOK,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).GetLoadState).Return(&DefaultLoadStateResp, nil).Build()
				mockey.Mock((*proxy.Proxy).DescribeIndex).Return(nil, ErrDefault).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamicField\":true,\"fields\":[{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"book_id\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"word_count\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"book_intro\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[],\"load\":\"LoadStateLoaded\",\"shardsNum\":1}}",
		},
		{
			name:       "show collection details success",
			mp:         mp,
			exceptCode: http.StatusOK,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).GetLoadState).Return(&DefaultLoadStateResp, nil).Build()
				mockey.Mock((*proxy.Proxy).DescribeIndex).Return(&DefaultDescIndexesReqp, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamicField\":true,\"fields\":[{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"book_id\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"word_count\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"clusteringKey\":false,\"description\":\"\",\"name\":\"book_intro\",\"nullable\":false,\"partitionKey\":false,\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"" + DefaultIndexName + "\",\"metricType\":\"COSINE\"}],\"load\":\"LoadStateLoaded\",\"shardsNum\":1}}",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsDescribePath)+"?collectionName="+DefaultCollectionName, nil)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
		})
	}
	t.Run("need collectionName", func(t *testing.T) {
		testEngine := initHTTPServer(&proxy.Proxy{}, true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsDescribePath)+"?"+DefaultCollectionName, nil)
		req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrMissingRequiredParameters))
	})
}

func TestVectorCreateCollection(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}
	errLimit := merr.WrapErrCollectionNumLimitExceeded("default", 65535)

	testCases := []testCase{
		{
			name:       "create collection fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).CreateCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "create collection fail with status",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).CreateCollection).Return(merr.Status(errLimit), nil).Build()
			},
			expectedBody: PrintErr(errLimit),
		},
		{
			name:       "create index fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).CreateCollection).Return(&StatusSuccess, nil).Build()
				mockey.Mock((*proxy.Proxy).CreateIndex).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "load collection fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).CreateCollection).Return(&StatusSuccess, nil).Build()
				mockey.Mock((*proxy.Proxy).CreateIndex).Return(&StatusSuccess, nil).Build()
				mockey.Mock((*proxy.Proxy).LoadCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "create collection success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).CreateCollection).Return(&StatusSuccess, nil).Build()
				mockey.Mock((*proxy.Proxy).CreateIndex).Return(&StatusSuccess, nil).Build()
				mockey.Mock((*proxy.Proxy).LoadCollection).Return(&StatusSuccess, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{}}",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2}`)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, versional(VectorCollectionsCreatePath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
		})
	}
}

func TestVectorDropCollection(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "[share] check collection fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).HasCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "[share] unexpected error",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).HasCollection).Return(&milvuspb.BoolResponse{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:        "[share] collection not found",
			mp:          mp,
			exceptCode:  200,
			expectedErr: merr.ErrCollectionNotFound,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).HasCollection).Return(&DefaultFalseResp, nil).Build()
			},
		},
		{
			name:       "drop collection fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).HasCollection).Return(&DefaultTrueResp, nil).Build()
				mockey.Mock((*proxy.Proxy).DropCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "drop collection fail with status",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).HasCollection).Return(&DefaultTrueResp, nil).Build()
				mockey.Mock((*proxy.Proxy).DropCollection).Return(merr.Status(errCollNotFound), nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "drop collection success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).HasCollection).Return(&DefaultTrueResp, nil).Build()
				mockey.Mock((*proxy.Proxy).DropCollection).Return(&StatusSuccess, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{}}",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `"}`)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, versional(VectorCollectionsDropPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
		})
	}
}

func TestQuery(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "query fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Query).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "query fail with status",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Query).Return(&milvuspb.QueryResults{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:        "query invalid result",
			mp:          mp,
			exceptCode:  200,
			expectedErr: merr.ErrInvalidSearchResult,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Query).Return(&milvuspb.QueryResults{
					Status:         &StatusSuccess,
					FieldsData:     newFieldData([]*schemapb.FieldData{}, 1000),
					CollectionName: DefaultCollectionName,
					OutputFields:   []string{FieldBookID, FieldWordCount, FieldBookIntro},
				}, nil).Build()
			},
		},
		{
			name:       "query success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Query).Return(&milvuspb.QueryResults{
					Status:         &StatusSuccess,
					FieldsData:     generateFieldData(),
					CollectionName: DefaultCollectionName,
					OutputFields:   []string{FieldBookID, FieldWordCount, FieldBookIntro},
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":[{\"book_id\":1,\"book_intro\":[0.1,0.11],\"word_count\":1000},{\"book_id\":2,\"book_intro\":[0.2,0.22],\"word_count\":2000},{\"book_id\":3,\"book_intro\":[0.3,0.33],\"word_count\":3000}]}\n",
		},
	}

	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")

	for _, tt := range testCases {
		reqs := []*http.Request{genQueryRequest(), genGetRequest()}
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			for _, req := range reqs {
				req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, tt.exceptCode, w.Code)
				if tt.expectedErr != nil {
					assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
				} else {
					assert.Equal(t, tt.expectedBody, w.Body.String())
				}
				resp := map[string]interface{}{}
				err := json.Unmarshal(w.Body.Bytes(), &resp)
				assert.Equal(t, nil, err)
				if resp[HTTPReturnCode] == float64(200) {
					data := resp[HTTPReturnData].([]interface{})
					rows := generateQueryResult64(false)
					for i, row := range data {
						assert.Equal(t, true, compareRow64(row.(map[string]interface{}), rows[i]))
					}
				}
			}
		})
	}
}

func genQueryRequest() *http.Request {
	jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `" , "filter": "book_id in [1,2,3]"}`)
	bodyReader := bytes.NewReader(jsonBody)
	req := httptest.NewRequest(http.MethodPost, versional(VectorQueryPath), bodyReader)
	return req
}

func genGetRequest() *http.Request {
	jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3]}`)
	bodyReader := bytes.NewReader(jsonBody)
	req := httptest.NewRequest(http.MethodPost, versional(VectorGetPath), bodyReader)
	return req
}

func TestDelete(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "[share] describe coll fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "[share] collection not found",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "delete fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Delete).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "delete fail with status",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Delete).Return(&milvuspb.MutationResult{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "delete success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Delete).Return(&milvuspb.MutationResult{
					Status: &StatusSuccess,
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{}}",
		},
	}

	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3]}`)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, versional(VectorDeletePath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, nil, err)
		})
	}
}

func TestDeleteForFilter(t *testing.T) {
	paramtable.Init()
	jsonBodyList := [][]byte{
		[]byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3]}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `" , "filter": "id in [1,2,3]"}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3], "filter": "id in [1,2,3]"}`),
	}
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	for _, jsonBody := range jsonBodyList {
		t.Run("delete success", func(t *testing.T) {
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
			mockey.Mock((*proxy.Proxy).Delete).Return(&milvuspb.MutationResult{
				Status: &StatusSuccess,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, versional(VectorDeletePath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{}}", w.Body.String())
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, nil, err)
		})
	}
}

func TestInsert(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "[share] describe coll fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "[share] collection not found",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "insert fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Insert).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "insert fail with status",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:        "id type invalid",
			mp:          mp,
			exceptCode:  200,
			expectedErr: merr.ErrCheckPrimaryKey,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
					Status: &StatusSuccess,
				}, nil).Build()
			},
		},
		{
			name:       "insert success int64",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
					Status:    &StatusSuccess,
					IDs:       genIDs(schemapb.DataType_Int64),
					InsertCnt: 3,
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[1,2,3]}}",
		},
		{
			name:       "insert success varchar",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
					Status:    &StatusSuccess,
					IDs:       genIDs(schemapb.DataType_VarChar),
					InsertCnt: 3,
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}",
		},
	}

	rows := generateSearchResult(schemapb.DataType_Int64)
	data, _ := json.Marshal(map[string]interface{}{
		HTTPCollectionName: DefaultCollectionName,
		HTTPReturnData:     rows[0],
	})
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, nil, err)
		})
	}

	t.Run("wrong request body", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
		defer mockey.UnPatchAll()
		testEngine := initHTTPServer(mp, true)
		bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "data": {}}`))
		req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
		req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrInvalidInsertData))
		resp := map[string]interface{}{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, nil, err)
	})
}

func TestInsertForDataType(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	schemas := map[string]*schemapb.CollectionSchema{
		"[success]kinds of data type": newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false, true)),
		"[success]with dynamic field": withDynamicField(newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false, true))),
		"[success]with array fields":  withArrayField(newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false, true))),
	}
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			mp := &proxy.Proxy{}
			schemaLocal := schema
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schemaLocal,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(schemapb.DataType_Int64),
				InsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[1,2,3]}}", w.Body.String())
		})
	}
	schemas = map[string]*schemapb.CollectionSchema{}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			mp := &proxy.Proxy{}
			schemaLocal := schema
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schemaLocal,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrInvalidInsertData))
		})
	}
}

func TestReturnInt64(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "false")
	schemas := []schemapb.DataType{
		schemapb.DataType_Int64,
		schemapb.DataType_VarChar,
	}
	idStrs := map[schemapb.DataType]string{
		schemapb.DataType_Int64:   "1,2,3",
		schemapb.DataType_VarChar: "\"1\",\"2\",\"3\"",
	}
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				InsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				UpsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: false, Accept-Type-Allow-Int64: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				InsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			req.Header.Set(HTTPHeaderAllowInt64, "true")
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: false, Accept-Type-Allow-Int64: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				UpsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			req.Header.Set(HTTPHeaderAllowInt64, "true")
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				InsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				UpsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: true, Accept-Type-Allow-Int64: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				InsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			req.Header.Set(HTTPHeaderAllowInt64, "false")
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: true, Accept-Type-Allow-Int64: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false, true))
			mp := &proxy.Proxy{}
			mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Build()
			mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIDs(dataType),
				UpsertCnt: 3,
			}, nil).Build()
			defer mockey.UnPatchAll()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			req.Header.Set(HTTPHeaderAllowInt64, "false")
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}
}

func TestUpsert(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "[share] describe coll fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "[share] collection not found",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "upsert fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Upsert).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "upsert fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "id type invalid",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
					Status: &StatusSuccess,
				}, nil).Build()
			},
			expectedErr: merr.ErrCheckPrimaryKey,
		},
		{
			name:       "upsert success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
					Status:    &StatusSuccess,
					IDs:       genIDs(schemapb.DataType_Int64),
					UpsertCnt: 3,
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[1,2,3]}}",
		},
		{
			name:       "upsert success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
				mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{
					Status:    &StatusSuccess,
					IDs:       genIDs(schemapb.DataType_VarChar),
					UpsertCnt: 3,
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[\"1\",\"2\",\"3\"]}}",
		},
	}

	rows := generateSearchResult(schemapb.DataType_Int64)
	data, _ := json.Marshal(map[string]interface{}{
		HTTPCollectionName: DefaultCollectionName,
		HTTPReturnData:     rows[0],
	})
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, nil, err)
		})
	}

	t.Run("wrong request body", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&DefaultDescCollectionResp, nil).Build()
		defer mockey.UnPatchAll()
		testEngine := initHTTPServer(mp, true)
		bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "data": {}}`))
		req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
		req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrInvalidInsertData))
		resp := map[string]interface{}{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, nil, err)
	})
}

func TestFp16Bf16VectorsV1(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	mp := &proxy.Proxy{}
	collSchema := generateCollectionSchemaWithVectorFields()
	testEngine := initHTTPServer(mp, true)
	queryTestCases := []requestBodyTestCase{}
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	for _, path := range []string{VectorInsertPath, VectorUpsertPath} {
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
				errCode: 200,
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
				errCode: 200,
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
				errCode: 200,
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
				errCode: 200,
			})
	}
	mockey.Mock((*proxy.Proxy).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         collSchema,
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Build()
	mockey.Mock((*proxy.Proxy).Insert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	mockey.Mock((*proxy.Proxy).Upsert).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Build()
	defer mockey.UnPatchAll()
	for i, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader(testcase.requestBody)
			req := httptest.NewRequest(http.MethodPost, versional(testcase.path), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code, "case %d: ", i, string(testcase.requestBody))
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err, "case %d: ", i)
			assert.Equal(t, testcase.errCode, returnBody.Code, "case %d: ", i, string(testcase.requestBody))
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message, "case %d: ", i, string(testcase.requestBody))
			}
			fmt.Println(w.Body.String())
		})
	}
}

func genIDs(dataType schemapb.DataType) *schemapb.IDs {
	return generateIDs(dataType, 3)
}

func TestSearch(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	mp := &proxy.Proxy{}
	errCollNotFound := merr.WrapErrCollectionNotFound(DefaultCollectionName)

	testCases := []testCase{
		{
			name:       "search fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).Search).Return(nil, ErrDefault).Build()
			},
			expectedBody: PrintErr(ErrDefault),
		},
		{
			name:       "search fail",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).Search).Return(&milvuspb.SearchResults{
					Status: merr.Status(errCollNotFound),
				}, nil).Build()
			},
			expectedBody: PrintErr(errCollNotFound),
		},
		{
			name:       "search success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).Search).Return(&milvuspb.SearchResults{
					Status: &StatusSuccess,
					Results: &schemapb.SearchResultData{
						FieldsData: []*schemapb.FieldData{},
						Scores:     []float32{},
						TopK:       0,
					},
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":[]}",
		},
		{
			name:       "search success",
			mp:         mp,
			exceptCode: 200,
			mockSetup: func() {
				mockey.Mock((*proxy.Proxy).Search).Return(&milvuspb.SearchResults{
					Status: &StatusSuccess,
					Results: &schemapb.SearchResultData{
						FieldsData: generateFieldData(),
						Scores:     []float32{0.01, 0.04, 0.09},
						TopK:       3,
					},
				}, nil).Build()
			},
			expectedBody: "{\"code\":200,\"data\":[{\"book_id\":1,\"book_intro\":[0.1,0.11],\"distance\":0.01,\"word_count\":1000},{\"book_id\":2,\"book_intro\":[0.2,0.22],\"distance\":0.04,\"word_count\":2000},{\"book_id\":3,\"book_intro\":[0.3,0.33],\"distance\":0.09,\"word_count\":3000}]}\n",
		},
	}
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockSetup != nil {
				tt.mockSetup()
				defer mockey.UnPatchAll()
			}
			testEngine := initHTTPServer(tt.mp, true)
			rows := []float32{0.0, 0.0}
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				"vector":           rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorSearchPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptCode, w.Code)
			if tt.expectedErr != nil {
				assert.Equal(t, true, CheckErrCode(w.Body.String(), tt.expectedErr))
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, nil, err)
			if resp[HTTPReturnCode] == float64(200) {
				data := resp[HTTPReturnData].([]interface{})
				rows := generateQueryResult64(true)
				for i, row := range data {
					assert.Equal(t, true, compareRow64(row.(map[string]interface{}), rows[i]))
				}
			}
		})
	}

	t.Run("search success with params", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).Search).Return(&milvuspb.SearchResults{
			Status: &StatusSuccess,
			Results: &schemapb.SearchResultData{
				FieldsData: generateFieldData(),
				Scores:     []float32{0.01, 0.04, 0.09},
				TopK:       3,
			},
		}, nil).Build()
		defer mockey.UnPatchAll()
		testEngine := initHTTPServer(mp, true)
		rows := []float32{0.0, 0.0}
		data, _ := json.Marshal(map[string]interface{}{
			HTTPCollectionName: DefaultCollectionName,
			"vector":           rows,
			Params: map[string]float64{
				ParamRadius:      0.9,
				ParamRangeFilter: 0.1,
			},
		})
		bodyReader := bytes.NewReader(data)
		req := httptest.NewRequest(http.MethodPost, versional(VectorSearchPath), bodyReader)
		req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	})
}

func TestHttpRequestFormat(t *testing.T) {
	paramtable.Init()
	errStrs := []error{
		merr.ErrIncorrectParameterFormat,
		merr.ErrMissingRequiredParameters,
		merr.ErrMissingRequiredParameters,
		merr.ErrMissingRequiredParameters,
		merr.ErrIncorrectParameterFormat,
	}
	requestJsons := [][]byte{
		[]byte(`{"collectionName": {"` + DefaultCollectionName + `", "dimension": 2}`),
		[]byte(`{"collName": "` + DefaultCollectionName + `", "dimension": 2}`),
		[]byte(`{"collName": "` + DefaultCollectionName + `", "dim": 2}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `", "vector": [0.0, 0.0], "` + Params + `": {"` + ParamRangeFilter + `": 0.1}}`),
	}
	paths := [][]string{
		{
			versional(VectorCollectionsCreatePath),
			versional(VectorCollectionsDropPath),
			versional(VectorGetPath),
			versional(VectorSearchPath),
			versional(VectorQueryPath),
			versional(VectorInsertPath),
			versional(VectorUpsertPath),
			versional(VectorDeletePath),
		}, {
			versional(VectorCollectionsDropPath),
			versional(VectorGetPath),
			versional(VectorSearchPath),
			versional(VectorQueryPath),
			versional(VectorInsertPath),
			versional(VectorUpsertPath),
			versional(VectorDeletePath),
		}, {
			versional(VectorCollectionsCreatePath),
		}, {
			versional(VectorGetPath),
			versional(VectorSearchPath),
			versional(VectorQueryPath),
			versional(VectorInsertPath),
			versional(VectorUpsertPath),
			versional(VectorDeletePath),
		}, {
			versional(VectorSearchPath),
		},
	}
	mp := &proxy.Proxy{}
	for i, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("request parameters wrong", func(t *testing.T) {
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(requestJsons[i])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, true, CheckErrCode(w.Body.String(), errStrs[i]))
			})
		}
	}
}

func TestAuthorization(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	errorStr := Print(merr.Code(merr.ErrServiceUnavailable), "internal: Milvus Proxy is not ready yet. please wait: service unavailable")
	jsons := map[string][]byte{
		errorStr: []byte(`{"collectionName": "` + DefaultCollectionName + `", "vector": [0.1, 0.2], "filter": "id in [2]", "id": [2], "dimension": 2, "data":[{"book_id":1,"book_intro":[0.1,0.11],"distance":0.01,"word_count":1000},{"book_id":2,"book_intro":[0.2,0.22],"distance":0.04,"word_count":2000},{"book_id":3,"book_intro":[0.3,0.33],"distance":0.09,"word_count":3000}]}`),
	}
	mp := &proxy.Proxy{}
	paths := map[string][]string{
		errorStr: {
			versional(VectorGetPath),
			versional(VectorInsertPath),
			versional(VectorUpsertPath),
			versional(VectorDeletePath),
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			versional(VectorCollectionsCreatePath),
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			versional(VectorCollectionsDropPath),
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			versional(VectorCollectionsDescribePath) + "?collectionName=" + DefaultCollectionName,
		},
	}
	for _, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				testEngine := initHTTPServer(mp, true)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
			})
		}
	}
	paths = map[string][]string{
		errorStr: {
			versional(VectorQueryPath),
			versional(VectorSearchPath),
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
			})
		}
	}
}

func TestDatabaseNotFound(t *testing.T) {
	paramtable.Init()
	mp := &proxy.Proxy{}

	t.Run("list database fail", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).ListDatabases).Return(nil, ErrDefault).Build()
		defer mockey.UnPatchAll()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath)+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), ErrDefault))
	})

	t.Run("list database without success code", func(t *testing.T) {
		err := errors.New("unexpected error")
		mockey.Mock((*proxy.Proxy).ListDatabases).Return(&milvuspb.ListDatabasesResponse{
			Status: merr.Status(err),
		}, nil).Build()
		defer mockey.UnPatchAll()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath)+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), err))
	})

	t.Run("list database success", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).ListDatabases).Return(&milvuspb.ListDatabasesResponse{
			Status:  &StatusSuccess,
			DbNames: []string{"default", "test"},
		}, nil).Build()
		mockey.Mock((*proxy.Proxy).ShowCollections).Return(&milvuspb.ShowCollectionsResponse{
			Status:          &StatusSuccess,
			CollectionNames: nil,
		}, nil).Build()
		defer mockey.UnPatchAll()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath)+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "{\"code\":200,\"data\":[]}", w.Body.String())
	})

	theError := merr.ErrDatabaseNotFound
	paths := map[error][]string{
		theError: {
			versional(VectorCollectionsPath) + "?dbName=test",
			versional(VectorCollectionsDescribePath) + "?dbName=test&collectionName=" + DefaultCollectionName,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("GET dbName", func(t *testing.T) {
				mockey.Mock((*proxy.Proxy).ListDatabases).Return(&milvuspb.ListDatabasesResponse{
					Status:  &StatusSuccess,
					DbNames: []string{"default"},
				}, nil).Build()
				defer mockey.UnPatchAll()
				testEngine := initHTTPServer(mp, true)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("authorization", "Bearer root:Milvus")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, true, CheckErrCode(w.Body.String(), res))
			})
		}
	}

	requestBody := `{"dbName": "test", "collectionName": "` + DefaultCollectionName + `", "vector": [0.1, 0.2], "filter": "id in [2]", "id": [2], "dimension": 2, "data":[{"book_id":1,"book_intro":[0.1,0.11],"distance":0.01,"word_count":1000},{"book_id":2,"book_intro":[0.2,0.22],"distance":0.04,"word_count":2000},{"book_id":3,"book_intro":[0.3,0.33],"distance":0.09,"word_count":3000}]}`
	pathArray := map[string][]string{
		requestBody: {
			versional(VectorCollectionsCreatePath),
			versional(VectorCollectionsDropPath),
			versional(VectorInsertPath),
			versional(VectorUpsertPath),
			versional(VectorDeletePath),
			versional(VectorQueryPath),
			versional(VectorGetPath),
			versional(VectorSearchPath),
		},
	}
	for request, pathArr := range pathArray {
		for _, path := range pathArr {
			t.Run("POST dbName", func(t *testing.T) {
				mockey.Mock((*proxy.Proxy).ListDatabases).Return(&milvuspb.ListDatabasesResponse{
					Status:  &StatusSuccess,
					DbNames: []string{"default"},
				}, nil).Build()
				defer mockey.UnPatchAll()
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader([]byte(request))
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer root:Milvus")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, true, CheckErrCode(w.Body.String(), theError))
			})
		}
	}
}

func TestInterceptor(t *testing.T) {
	h := HandlersV1{}
	v := atomic.NewInt32(0)
	h.interceptors = []RestRequestInterceptor{
		func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error) {
			log.Info("pre1")
			v.Add(1)
			assert.EqualValues(t, 1, v.Load())
			res, err := handler(ctx, req)
			log.Info("post1")
			v.Add(1)
			assert.EqualValues(t, 6, v.Load())
			return res, err
		},
		func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error) {
			log.Info("pre2")
			v.Add(1)
			assert.EqualValues(t, 2, v.Load())
			res, err := handler(ctx, req)
			log.Info("post2")
			v.Add(1)
			assert.EqualValues(t, 5, v.Load())
			return res, err
		},
		func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error) {
			log.Info("pre3")
			v.Add(1)
			assert.EqualValues(t, 3, v.Load())
			res, err := handler(ctx, req)
			log.Info("post3")
			v.Add(1)
			assert.EqualValues(t, 4, v.Load())
			return res, err
		},
	}
	_, _ = h.executeRestRequestInterceptor(context.Background(), nil, &milvuspb.CreateCollectionRequest{}, func(reqCtx context.Context, req any) (any, error) {
		return &commonpb.Status{}, nil
	})
}
