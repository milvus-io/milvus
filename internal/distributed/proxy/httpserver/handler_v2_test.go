package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
		assert.Equal(t, int32(2), returnBody.Code)
		assert.Equal(t, "service unavailable: internal: Milvus Proxy is not ready yet. please wait", returnBody.Message)
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
		headers: map[string]string{HTTPHeaderRequestTimeout: "3"},
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
		headers: map[string]string{HTTPHeaderRequestTimeout: "32"},
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

func TestDatabaseWrapper(t *testing.T) {
	postTestCases := []requestBodyTestCase{}
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
		Status:  &StatusSuccess,
		DbNames: []string{DefaultCollectionName, "exist"},
	}, nil).Twice()
	mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{Status: commonErrorStatus}, nil).Once()
	h := NewHandlersV2(mp)
	ginHandler := gin.Default()
	app := ginHandler.Group("", genAuthMiddleWare(false))
	path := "/wrapper/database"
	app.POST(path, wrapperPost(func() any { return &DefaultReq{} }, h.wrapperCheckDatabase(func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	})))
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName": "exist"}`),
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName": "non-exist"}`),
		errMsg:      "database not found, database: non-exist",
		errCode:     800, // ErrDatabaseNotFound
	})
	postTestCases = append(postTestCases, requestBodyTestCase{
		path:        path,
		requestBody: []byte(`{"dbName": "test"}`),
		errMsg:      "",
		errCode:     65535,
	})

	for _, testcase := range postTestCases {
		t.Run("post"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, testcase.path, bytes.NewReader(testcase.requestBody))
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			if testcase.errCode != 0 {
				returnBody := &ReturnErrMsg{}
				err := json.Unmarshal(w.Body.Bytes(), returnBody)
				assert.Nil(t, err)
				assert.Equal(t, testcase.errCode, returnBody.Code)
				assert.Equal(t, testcase.errMsg, returnBody.Message)
			}
		})
	}

	mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
		Status:  &StatusSuccess,
		DbNames: []string{DefaultCollectionName, "default"},
	}, nil).Once()
	mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
		Status:  &StatusSuccess,
		DbNames: []string{DefaultCollectionName, "test"},
	}, nil).Once()
	mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{Status: commonErrorStatus}, nil).Once()
	rawTestCases := []rawTestCase{
		{
			errMsg:  "database not found, database: test",
			errCode: 800, // ErrDatabaseNotFound
		},
		{},
		{
			errMsg:  "",
			errCode: 65535,
		},
	}
	for _, testcase := range rawTestCases {
		t.Run("post with db"+testcase.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(`{}`)))
			req.Header.Set(HTTPHeaderDBName, "test")
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			fmt.Println(w.Body.String())
			if testcase.errCode != 0 {
				returnBody := &ReturnErrMsg{}
				err := json.Unmarshal(w.Body.Bytes(), returnBody)
				assert.Nil(t, err)
				assert.Equal(t, testcase.errCode, returnBody.Code)
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
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Times(13)
	mp.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Times(6)
	mp.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Times(6)
	mp.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(commonErrorStatus, nil).Twice()
	mp.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(commonErrorStatus, nil).Twice()
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
                {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
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
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
		Status: &StatusSuccess,
	}, nil).Once()
	mp.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
		Status:          &StatusSuccess,
		CollectionNames: []string{DefaultCollectionName},
	}, nil).Once()
	mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
		Status: &StatusSuccess,
		Value:  true,
	}, nil).Once()
	mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{Status: commonErrorStatus}, nil).Once()
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Twice()
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{Status: commonErrorStatus}, nil).Once()
	mp.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadStateResponse{Status: commonErrorStatus}, nil).Once()
	mp.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Times(4)
	mp.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadStateResponse{
		Status: &StatusSuccess,
		State:  commonpb.LoadState_LoadStateNotExist,
	}, nil).Once()
	mp.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadStateResponse{
		Status: &StatusSuccess,
		State:  commonpb.LoadState_LoadStateNotLoad,
	}, nil).Once()
	mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{Status: commonErrorStatus}, nil).Once()
	mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&DefaultDescIndexesReqp, nil).Times(3)
	mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, merr.WrapErrIndexNotFoundForCollection(DefaultCollectionName)).Once()
	mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{
		Status: merr.Status(merr.WrapErrIndexNotFoundForCollection(DefaultCollectionName)),
	}, nil).Once()
	mp.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(&milvuspb.GetCollectionStatisticsResponse{
		Status: commonSuccessStatus,
		Stats: []*commonpb.KeyValuePair{
			{Key: "row_count", Value: "0"},
		},
	}, nil).Once()
	mp.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(&milvuspb.GetCollectionStatisticsResponse{
		Status: commonSuccessStatus,
		Stats: []*commonpb.KeyValuePair{
			{Key: "row_count", Value: "abc"},
		},
	}, nil).Once()
	mp.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadingProgressResponse{
		Status:   commonSuccessStatus,
		Progress: int64(77),
	}, nil).Once()
	mp.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadingProgressResponse{
		Status:   commonSuccessStatus,
		Progress: int64(100),
	}, nil).Once()
	mp.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadingProgressResponse{Status: commonErrorStatus}, nil).Once()
	mp.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:         &StatusSuccess,
		PartitionNames: []string{DefaultPartitionName},
	}, nil).Once()
	mp.EXPECT().HasPartition(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
		Status: &StatusSuccess,
		Value:  true,
	}, nil).Once()
	mp.EXPECT().GetPartitionStatistics(mock.Anything, mock.Anything).Return(&milvuspb.GetPartitionStatisticsResponse{
		Status: commonSuccessStatus,
		Stats: []*commonpb.KeyValuePair{
			{Key: "row_count", Value: "0"},
		},
	}, nil).Once()
	mp.EXPECT().ListCredUsers(mock.Anything, mock.Anything).Return(&milvuspb.ListCredUsersResponse{
		Status:    &StatusSuccess,
		Usernames: []string{util.UserRoot},
	}, nil).Once()
	mp.EXPECT().SelectUser(mock.Anything, mock.Anything).Return(&milvuspb.SelectUserResponse{
		Status: &StatusSuccess,
		Results: []*milvuspb.UserResult{
			{User: &milvuspb.UserEntity{Name: util.UserRoot}, Roles: []*milvuspb.RoleEntity{
				{Name: util.RoleAdmin},
			}},
		},
	}, nil).Once()
	mp.EXPECT().SelectRole(mock.Anything, mock.Anything).Return(&milvuspb.SelectRoleResponse{
		Status: &StatusSuccess,
		Results: []*milvuspb.RoleResult{
			{Role: &milvuspb.RoleEntity{Name: util.RoleAdmin}},
		},
	}, nil).Once()
	mp.EXPECT().SelectGrant(mock.Anything, mock.Anything).Return(&milvuspb.SelectGrantResponse{
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
	}, nil).Once()
	mp.EXPECT().ListAliases(mock.Anything, mock.Anything).Return(&milvuspb.ListAliasesResponse{Status: commonErrorStatus}, nil).Once()
	mp.EXPECT().ListAliases(mock.Anything, mock.Anything).Return(&milvuspb.ListAliasesResponse{
		Status: &StatusSuccess,
	}, nil).Once()
	mp.EXPECT().ListAliases(mock.Anything, mock.Anything).Return(&milvuspb.ListAliasesResponse{
		Status:  &StatusSuccess,
		Aliases: []string{DefaultAliasName},
	}, nil).Once()
	mp.EXPECT().DescribeAlias(mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status: &StatusSuccess,
		Alias:  DefaultAliasName,
	}, nil).Once()

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

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader([]byte(`{` +
				`"collectionName": "` + DefaultCollectionName + `",` +
				`"partitionName": "` + DefaultPartitionName + `",` +
				`"indexName": "` + DefaultIndexName + `",` +
				`"userName": "` + util.UserRoot + `",` +
				`"roleName": "` + util.RoleAdmin + `",` +
				`"aliasName": "` + DefaultAliasName + `"` +
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
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().DropPartition(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().DeleteCredential(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().DropRole(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().DropAlias(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
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
	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "partitionName": "` + DefaultPartitionName +
				`", "userName": "` + util.UserRoot + `", "roleName": "` + util.RoleAdmin + `", "indexName": "` + DefaultIndexName + `", "aliasName": "` + DefaultAliasName + `"}`))
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
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().RenameCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Twice()
	mp.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().CreateCredential(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().UpdateCredential(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().OperateUserRole(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Twice()
	mp.EXPECT().CreateRole(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Twice()
	mp.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Twice()
	mp.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(commonErrorStatus, nil).Once()
	mp.EXPECT().CreateAlias(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().AlterAlias(mock.Anything, mock.Anything).Return(commonSuccessStatus, nil).Once()
	mp.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(&internalpb.ImportResponse{
		Status: commonSuccessStatus, JobID: "1234567890",
	}, nil).Once()
	mp.EXPECT().ListImports(mock.Anything, mock.Anything).Return(&internalpb.ListImportsResponse{
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
	}, nil).Once()
	mp.EXPECT().GetImportProgress(mock.Anything, mock.Anything).Return(&internalpb.GetImportProgressResponse{
		Status:   &StatusSuccess,
		State:    internalpb.ImportJobState_Completed,
		Reason:   "",
		Progress: 100,
	}, nil).Once()
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

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader([]byte(`{` +
				`"collectionName": "` + DefaultCollectionName + `", "newCollectionName": "test", "newDbName": "",` +
				`"partitionName": "` + DefaultPartitionName + `", "partitionNames": ["` + DefaultPartitionName + `"],` +
				`"schema": {"fields": [{"fieldName": "book_id", "dataType": "Int64", "elementTypeParams": {}}, {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": 2}}]},` +
				`"indexParams": [{"indexName": "` + DefaultIndexName + `", "fieldName": "book_intro", "metricType": "L2", "params": {"nlist": 30, "index_type": "IVF_FLAT"}}],` +
				`"userName": "` + util.UserRoot + `", "password": "Milvus", "newPassword": "milvus", "roleName": "` + util.RoleAdmin + `",` +
				`"roleName": "` + util.RoleAdmin + `", "objectType": "Global", "objectName": "*", "privilege": "*",` +
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

func TestDML(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Times(6)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{Status: commonErrorStatus}, nil).Times(4)
	mp.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		if matchCountRule(req.OutputFields) {
			for _, pair := range req.QueryParams {
				if pair.GetKey() == ParamLimit {
					return nil, fmt.Errorf("mock error")
				}
			}
		}
		return &milvuspb.QueryResults{Status: commonSuccessStatus, OutputFields: []string{}, FieldsData: []*schemapb.FieldData{}}, nil
	}).Times(4)
	mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Once()
	mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{}}}}}, nil).Once()
	mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Once()
	mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{}}}}}, nil).Once()
	mp.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus}, nil).Once()
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
		requestBody: []byte(`{"collectionName": "book", "id" : [2, 4, 6, 8, 0], "outputFields": ["book_id",  "word_count", "book_intro"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        GetAction,
		requestBody: []byte(`{"collectionName": "book", "id" : [2, 4, 6, 8, 0], "outputFields": ["book_id",  "word_count", "book_intro"]}`),
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

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader(testcase.requestBody)
			req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, testcase.path), bodyReader)
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

func TestAllowInt64(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	mp := mocks.NewMockProxy(t)
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
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Twice()
	mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Once()
	mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: commonSuccessStatus, UpsertCnt: int64(0), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{}}}}}, nil).Once()

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader(testcase.requestBody)
			req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, testcase.path), bodyReader)
			req.Header.Set(HTTPHeaderAllowInt64, "true")
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

func TestSearchV2(t *testing.T) {
	paramtable.Init()
	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateCollectionSchema(schemapb.DataType_Int64),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Times(12)
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
		TopK:         int64(3),
		OutputFields: outputFields,
		FieldsData:   generateFieldData(),
		Ids:          generateIDs(schemapb.DataType_Int64, 3),
		Scores:       DefaultScores,
	}}, nil).Once()
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: int64(0)}}, nil).Times(3)
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: &commonpb.Status{
		ErrorCode: 1700, // ErrFieldNotFound
		Reason:    "groupBy field not found in schema: field not found[field=test]",
	}}, nil).Once()
	mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{
		TopK:         int64(3),
		OutputFields: outputFields,
		FieldsData:   generateFieldData(),
		Ids:          generateIDs(schemapb.DataType_Int64, 3),
		Scores:       DefaultScores,
	}}, nil).Once()
	mp.EXPECT().HybridSearch(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: int64(0)}}, nil).Times(3)
	collSchema := generateCollectionSchema(schemapb.DataType_Int64)
	binaryVectorField := generateVectorFieldSchema(schemapb.DataType_BinaryVector)
	binaryVectorField.Name = "binaryVector"
	float16VectorField := generateVectorFieldSchema(schemapb.DataType_Float16Vector)
	float16VectorField.Name = "float16Vector"
	bfloat16VectorField := generateVectorFieldSchema(schemapb.DataType_BFloat16Vector)
	bfloat16VectorField.Name = "bfloat16Vector"
	sparseFloatVectorField := generateVectorFieldSchema(schemapb.DataType_SparseFloatVector)
	sparseFloatVectorField.Name = "sparseFloatVector"
	collSchema.Fields = append(collSchema.Fields, binaryVectorField)
	collSchema.Fields = append(collSchema.Fields, float16VectorField)
	collSchema.Fields = append(collSchema.Fields, bfloat16VectorField)
	collSchema.Fields = append(collSchema.Fields, sparseFloatVectorField)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         collSchema,
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Times(10)
	mp.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: int64(0)}}, nil).Times(3)
	testEngine := initHTTPServerV2(mp, false)
	queryTestCases := []requestBodyTestCase{}
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"radius":0.9}}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"], "params": {"range_filter": 0.1}}`),
		errMsg:      "can only accept json format request, error: invalid search params",
		errCode:     1801, // ErrIncorrectParameterFormat
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
		errMsg:      "can only accept json format request, error: Mismatch type float32 with value string \"at index 8: mismatched type with value\\n\\n\\t[\\\"0.1\\\", \\\"0.2\\\"]\\n\\t........^.....\\n\": invalid parameter[expected=FloatVector][actual=[\"0.1\", \"0.2\"]]",
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
		errMsg:      "can only accept json format request, error: Mismatch type uint8 with value number \"at index 7: mismatched type with value\\n\\n\\t[[0.1, 0.2]]\\n\\t.......^....\\n\": invalid parameter[expected=BinaryVector][actual=[[0.1, 0.2]]]",
		errCode:     1801,
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path:        SearchAction,
		requestBody: []byte(`{"collectionName": "book", "data": ["AQ=="], "annsField": "binaryVector", "filter": "book_id in [2, 4, 6, 8]", "limit": 4, "outputFields": ["word_count"]}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
	})
	queryTestCases = append(queryTestCases, requestBodyTestCase{
		path: AdvancedSearchAction,
		requestBody: []byte(`{"collectionName": "hello_milvus", "search": [` +
			`{"data": [[0.1, 0.2, 0.3]], "annsField": "book_intro", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQ=="], "annsField": "binaryVector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "float16Vector", "metricType": "L2", "limit": 3},` +
			`{"data": ["AQIDBA=="], "annsField": "bfloat16Vector", "metricType": "L2", "limit": 3}` +
			`], "rerank": {"strategy": "weighted", "params": {"weights":  [0.9, 0.8]}}}`),
		errMsg:  "can only accept json format request, error: dimension: 2, but length of []float: 3: invalid parameter[expected=FloatVector][actual=[0.1, 0.2, 0.3]]",
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

	for _, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader(testcase.requestBody)
			req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, testcase.path), bodyReader)
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
