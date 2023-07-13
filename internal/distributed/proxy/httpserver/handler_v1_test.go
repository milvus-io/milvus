package httpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	DefaultCollectionName = "book"
	DefaultErrorMessage   = "UNKNOWN ERROR"

	ReturnSuccess     = 0
	ReturnFail        = 1
	ReturnWrongStatus = 2
	ReturnTrue        = 3
	ReturnFalse       = 4

	URIPrefix = "/v1"
)

var StatusSuccess = commonpb.Status{
	ErrorCode: commonpb.ErrorCode_Success,
	Reason:    "",
}

var ErrDefault = errors.New(DefaultErrorMessage)

var DefaultShowCollectionsResp = milvuspb.ShowCollectionsResponse{
	Status:          &StatusSuccess,
	CollectionNames: []string{DefaultCollectionName},
}

var DefaultDescCollectionResp = milvuspb.DescribeCollectionResponse{
	CollectionName: DefaultCollectionName,
	Schema:         generateCollectionSchema(false),
	ShardsNum:      ShardNumDefault,
	Status:         &StatusSuccess}

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

func initHTTPServer(proxy types.ProxyComponent, needAuth bool) *gin.Engine {
	h := NewHandlers(proxy)
	ginHandler := gin.Default()
	app := ginHandler.Group("/v1", genAuthMiddleWare(needAuth))
	NewHandlers(h.proxy).RegisterRoutesToV1(app)
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
	if needAuth {
		return func(c *gin.Context) {
			username, password, ok := ParseUsernamePassword(c)
			if !ok {
				c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": http.StatusProxyAuthRequired, "message": proxy.ErrUnauthenticated().Error()})
			} else if username == util.UserRoot && password != util.DefaultRootPassword {
				c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": http.StatusProxyAuthRequired, "message": proxy.ErrUnauthenticated().Error()})
			} else {
				c.Set(ContextUsername, username)
			}
		}
	}
	return func(c *gin.Context) {
		c.Set(ContextUsername, util.UserRoot)
	}
}

func TestVectorAuthenticate(t *testing.T) {
	mp := mocks.NewProxy(t)
	mp.EXPECT().
		ShowCollections(mock.Anything, mock.Anything).
		Return(&DefaultShowCollectionsResp, nil).
		Twice()

	testEngine := initHTTPServer(mp, true)

	t.Run("need authentication", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":407,\"message\":\"auth check failure, please check username and password are correct\"}")
	})

	t.Run("username or password incorrect", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil)
		req.SetBasicAuth(util.UserRoot, util.UserRoot)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":407,\"message\":\"auth check failure, please check username and password are correct\"}")
	})

	t.Run("root's password correct", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":[\""+DefaultCollectionName+"\"]}")
	})

	t.Run("username and password both provided", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil)
		req.SetBasicAuth("test", util.UserRoot)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":[\""+DefaultCollectionName+"\"]}")
	})
}

func TestVectorListCollection(t *testing.T) {
	testCases := []testCase{}
	mp0 := mocks.NewProxy(t)
	mp0.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "show collections fail",
		mp:           mp0,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"show collections fail\"}",
	})

	reason := "cannot create folder"
	mp1 := mocks.NewProxy(t)
	mp1.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CannotCreateFolder,
			Reason:    reason,
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "show collections fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + reason + "\",\"message\":\"show collections fail\"}",
	})

	mp := mocks.NewProxy(t)
	mp.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&DefaultShowCollectionsResp, nil).Once()
	testCases = append(testCases, testCase{
		name:         "list collections success",
		mp:           mp,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":[\"" + DefaultCollectionName + "\"]}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
		})
	}
}

type testCase struct {
	name         string
	mp           *mocks.Proxy
	exceptCode   int
	expectedBody string
}

func TestVectorCollectionsDescribe(t *testing.T) {
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	mp2, _ = wrapWithDescribeIndex(t, mp2, ReturnSuccess, 1, nil)
	testCases = append(testCases, testCase{
		name:         "get load status fail",
		mp:           mp2,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamic\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"_default_idx_102\",\"metricType\":\"L2\"}],\"load\":\"\",\"shardsNum\":2}}",
	})

	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Once()
	mp3, _ = wrapWithDescribeIndex(t, mp3, ReturnFail, 1, nil)
	testCases = append(testCases, testCase{
		name:         "get indexes fail",
		mp:           mp3,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamic\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[],\"load\":\"LoadStateLoaded\",\"shardsNum\":2}}",
	})

	mp4 := mocks.NewProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Once()
	mp4, _ = wrapWithDescribeIndex(t, mp4, ReturnSuccess, 1, nil)
	testCases = append(testCases, testCase{
		name:         "show collection details success",
		mp:           mp4,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamic\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"_default_idx_102\",\"metricType\":\"L2\"}],\"load\":\"LoadStateLoaded\",\"shardsNum\":2}}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections/describe?collectionName="+DefaultCollectionName, nil)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
		})
	}
	t.Run("need collectionName", func(t *testing.T) {
		testEngine := initHTTPServer(mocks.NewProxy(t), true)
		req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections/describe?"+DefaultCollectionName, nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, 200)
		assert.Equal(t, w.Body.String(), "{\"code\":400,\"message\":\"collectionName is required.\"}")
	})
}

func TestVectorCreateCollection(t *testing.T) {
	testCases := []testCase{}

	mp1 := mocks.NewProxy(t)
	mp1.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"create collection " + DefaultCollectionName + " fail\"}",
	})

	reason := "collection " + DefaultCollectionName + " already exists"
	mp2 := mocks.NewProxy(t)
	mp2.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_CannotCreateFile, // 18
		Reason:    reason,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: "{\"code\":18,\"error\":\"" + reason + "\",\"message\":\"create collection " + DefaultCollectionName + " fail\"}",
	})

	mp3 := mocks.NewProxy(t)
	mp3.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp3.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "create index fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"create index for collection " + DefaultCollectionName + " fail, after the collection was created\"}",
	})

	mp4 := mocks.NewProxy(t)
	mp4.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp4.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp4.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "load collection fail",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"load collection " + DefaultCollectionName + " fail, after the index was created\"}",
	})

	mp5 := mocks.NewProxy(t)
	mp5.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp5.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp5.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	testCases = append(testCases, testCase{
		name:         "create collection success",
		mp:           mp5,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{}}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2}`)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/collections/create", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
		})
	}
}

func TestVectorDropCollection(t *testing.T) {
	testCases := []testCase{}
	_, testCases = wrapWithHasCollection(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithHasCollection(t, nil, ReturnWrongStatus, 1, testCases)
	_, testCases = wrapWithHasCollection(t, nil, ReturnFalse, 1, testCases)

	mp1 := mocks.NewProxy(t)
	mp1, _ = wrapWithHasCollection(t, mp1, ReturnTrue, 1, nil)
	mp1.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"drop collection " + DefaultCollectionName + " fail\"}",
	})

	reason := "collection " + DefaultCollectionName + " already exists"
	mp2 := mocks.NewProxy(t)
	mp2, _ = wrapWithHasCollection(t, mp2, ReturnTrue, 1, nil)
	mp2.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_CollectionNotExists, // 4
		Reason:    reason,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: "{\"code\":4,\"error\":\"" + reason + "\",\"message\":\"drop collection " + DefaultCollectionName + " fail\"}",
	})

	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithHasCollection(t, mp3, ReturnTrue, 1, nil)
	mp3.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{}}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `"}`)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/collections/drop", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
		})
	}
}

func TestQuery(t *testing.T) {
	testCases := []testCase{}

	mp2 := mocks.NewProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, ErrDefault).Twice()
	testCases = append(testCases, testCase{
		name:         "query fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"query fail\"}",
	})

	reason := DefaultCollectionName + " name not found"
	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
			Reason:    reason,
		},
	}, nil).Twice()
	testCases = append(testCases, testCase{
		name:         "query fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: "{\"code\":28,\"message\":\"" + reason + "\"}",
	})

	mp4 := mocks.NewProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status:         &StatusSuccess,
		FieldsData:     generateFieldData(),
		CollectionName: DefaultCollectionName,
		OutputFields:   []string{FieldBookID, FieldWordCount, FieldBookIntro},
	}, nil).Twice()
	testCases = append(testCases, testCase{
		name:         "query success",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":[{\"book_id\":1,\"book_intro\":[0.1,0.11],\"word_count\":1000},{\"book_id\":2,\"book_intro\":[0.2,0.22],\"word_count\":2000},{\"book_id\":3,\"book_intro\":[0.3,0.33],\"word_count\":3000}]}",
	})

	for _, tt := range testCases {
		reqs := []*http.Request{genQueryRequest(), genGetRequest()}
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			for _, req := range reqs {
				req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, tt.exceptCode)
				assert.Equal(t, w.Body.String(), tt.expectedBody)
				resp := map[string]interface{}{}
				err := json.Unmarshal(w.Body.Bytes(), &resp)
				assert.Equal(t, err, nil)
				if resp[HTTPReturnCode] == float64(200) {
					data := resp[HTTPReturnData].([]interface{})
					rows := generateQueryResult64(false)
					for i, row := range data {
						assert.Equal(t, compareRow64(row.(map[string]interface{}), rows[i]), true)
					}
				}
			}
		})
	}
}

func genQueryRequest() *http.Request {
	jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `" , "filter": "book_id in [1,2,3]"}`)
	bodyReader := bytes.NewReader(jsonBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/vector/query", bodyReader)
	return req
}

func genGetRequest() *http.Request {
	jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3]}`)
	bodyReader := bytes.NewReader(jsonBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/vector/get", bodyReader)
	return req
}

func TestDelete(t *testing.T) {
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Delete(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "delete fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"delete fail\"}",
	})

	reason := DefaultCollectionName + " name not found"
	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
			Reason:    reason,
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "delete fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: "{\"code\":28,\"error\":\"" + reason + "\",\"message\":\"delete fail\"}",
	})

	mp4 := mocks.NewProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: &StatusSuccess,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "delete success",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{}}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			jsonBody := []byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3]}`)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/delete", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, err, nil)
		})
	}
}

func TestInsert(t *testing.T) {
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Insert(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "insert fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"insert fail\"}",
	})

	reason := DefaultCollectionName + " name not found"
	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
			Reason:    reason,
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "insert fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: "{\"code\":28,\"error\":\"" + reason + "\",\"message\":\"insert fail\"}",
	})

	mp4 := mocks.NewProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: &StatusSuccess,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "id type invalid",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"message\":\"ids' type neither int or string\"}",
	})

	mp5 := mocks.NewProxy(t)
	mp5, _ = wrapWithDescribeColl(t, mp5, ReturnSuccess, 1, nil)
	mp5.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status:    &StatusSuccess,
		IDs:       getIntIds(),
		InsertCnt: 3,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "insert success",
		mp:           mp5,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[1,2,3]}}",
	})

	mp6 := mocks.NewProxy(t)
	mp6, _ = wrapWithDescribeColl(t, mp6, ReturnSuccess, 1, nil)
	mp6.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status:    &StatusSuccess,
		IDs:       getStrIds(),
		InsertCnt: 3,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "insert success",
		mp:           mp6,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			rows := generateSearchResult()
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/insert", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, err, nil)
		})
	}

	t.Run("wrong request body", func(t *testing.T) {
		mp := mocks.NewProxy(t)
		mp, _ = wrapWithDescribeColl(t, mp, ReturnSuccess, 1, nil)
		testEngine := initHTTPServer(mp, true)
		bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2}`))
		req := httptest.NewRequest(http.MethodPost, "/v1/vector/insert", bodyReader)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, 200)
		assert.Equal(t, w.Body.String(), "{\"code\":400,\"error\":\"data is required\",\"message\":\"checkout your params\"}")
		resp := map[string]interface{}{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, err, nil)
	})
}

func TestInsertForDataType(t *testing.T) {
	schemas := map[string]*schemapb.CollectionSchema{
		"[success]kinds of data type": newCollectionSchema(generateCollectionSchema(false)),
		"[success]use binary vector":  newCollectionSchema(generateCollectionSchema(true)),
		"[success]with dynamic field": withDynamicField(newCollectionSchema(generateCollectionSchema(false))),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			mp := mocks.NewProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       getIntIds(),
				InsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult())
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/insert", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, 200)
			assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[1,2,3]}}")
		})
	}
	schemas = map[string]*schemapb.CollectionSchema{
		"with unsupport field type": withUnsupportField(newCollectionSchema(generateCollectionSchema(false))),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			mp := mocks.NewProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult())
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/insert", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, 200)
			assert.Equal(t, w.Body.String(), "{\"code\":400,\"error\":\"not support fieldName field-array dataType Array\",\"message\":\"checkout your params\"}")
		})
	}
}

func getIntIds() *schemapb.IDs {
	ids := schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{1, 2, 3},
			},
		},
	}
	return &ids
}

func getStrIds() *schemapb.IDs {
	ids := schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{"1", "2", "3"},
			},
		},
	}
	return &ids
}

func TestSearch(t *testing.T) {
	testCases := []testCase{}

	mp2 := mocks.NewProxy(t)
	mp2.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "search fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"search fail\"}",
	})

	reason := DefaultCollectionName + " name not found"
	mp3 := mocks.NewProxy(t)
	mp3.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
			Reason:    reason,
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "search fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: "{\"code\":28,\"error\":\"" + reason + "\",\"message\":\"search fail\"}",
	})

	mp4 := mocks.NewProxy(t)
	mp4.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: &StatusSuccess,
		Results: &schemapb.SearchResultData{
			FieldsData: generateFieldData(),
			Scores:     []float32{0.01, 0.04, 0.09},
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "search success",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":[{\"book_id\":1,\"book_intro\":[0.1,0.11],\"distance\":0.01,\"word_count\":1000},{\"book_id\":2,\"book_intro\":[0.2,0.22],\"distance\":0.04,\"word_count\":2000},{\"book_id\":3,\"book_intro\":[0.3,0.33],\"distance\":0.09,\"word_count\":3000}]}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			rows := []float32{0.0, 0.0}
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				"vector":           rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, "/v1/vector/search", bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, w.Code, tt.exceptCode)
			assert.Equal(t, w.Body.String(), tt.expectedBody)
			resp := map[string]interface{}{}
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.Equal(t, err, nil)
			if resp[HTTPReturnCode] == float64(200) {
				data := resp[HTTPReturnData].([]interface{})
				rows := generateQueryResult64(true)
				for i, row := range data {
					assert.Equal(t, compareRow64(row.(map[string]interface{}), rows[i]), true)
				}
			}
		})
	}
}

type ReturnType int

func wrapWithDescribeColl(t *testing.T, mp *mocks.Proxy, returnType ReturnType, times int, testCases []testCase) (*mocks.Proxy, []testCase) {
	if mp == nil {
		mp = mocks.NewProxy(t)
	}
	var call *mocks.Proxy_DescribeCollection_Call
	var testcase testCase
	switch returnType {
	case ReturnSuccess:
		call = mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&DefaultDescCollectionResp, nil)
		testcase = testCase{
			name:         "[share] describe coll success",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":200,\"data\":{}}",
		}
	case ReturnFail:
		call = mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault)
		testcase = testCase{
			name:         "[share] describe coll fail",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"describe collection " + DefaultCollectionName + " fail\"}",
		}
	case ReturnWrongStatus:
		call = mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
				Reason:    "can't find collection: " + DefaultCollectionName,
			},
		}, nil)
		testcase = testCase{
			name:         "[share] collection not found",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":4,\"message\":\"can't find collection: " + DefaultCollectionName + "\"}",
		}
	}
	if times == 2 {
		call.Twice()
	} else {
		call.Once()
	}
	if testCases != nil {
		testCases = append(testCases, testcase)
	}
	return mp, testCases
}

func wrapWithHasCollection(t *testing.T, mp *mocks.Proxy, returnType ReturnType, times int, testCases []testCase) (*mocks.Proxy, []testCase) {
	if mp == nil {
		mp = mocks.NewProxy(t)
	}
	var call *mocks.Proxy_HasCollection_Call
	var testcase testCase
	switch returnType {
	case ReturnTrue:
		call = mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&DefaultTrueResp, nil)
		testcase = testCase{
			name:         "[share] collection already exists",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":200,\"message\":\"collection " + DefaultCollectionName + " already exist.\"}",
		}
	case ReturnFalse:
		call = mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&DefaultFalseResp, nil)
		testcase = testCase{
			name:         "[share] collection not found",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":400,\"message\":\"can't find collection: " + DefaultCollectionName + "\"}",
		}
	case ReturnFail:
		call = mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault)
		testcase = testCase{
			name:         "[share] check collection fail",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"check collections " + DefaultCollectionName + " exists fail\"}",
		}
	case ReturnWrongStatus:
		reason := "can't find collection: " + DefaultCollectionName
		call = mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError, // 1
				Reason:    reason,
			},
		}, nil)
		testcase = testCase{
			name:         "[share] unexpected error",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":1,\"error\":\"" + reason + "\",\"message\":\"check collections book exists fail\"}",
		}
	}
	if times == 2 {
		call.Twice()
	} else {
		call.Once()
	}
	if testCases != nil {
		testCases = append(testCases, testcase)
	}
	return mp, testCases
}

func TestHttpRequestFormat(t *testing.T) {
	parseErrStr := "{\"code\":400,\"error\":\"invalid character ',' after object key\",\"message\":\"check your parameters conform to the json format\"}"
	collnameErrStr := "{\"code\":400,\"message\":\"collectionName is required.\"}"
	collnameDimErrStr := "{\"code\":400,\"message\":\"collectionName and dimension are both required.\"}"
	dataErrStr := "check and set data"
	jsons := map[string][]byte{
		parseErrStr:       []byte(`{"collectionName": {"` + DefaultCollectionName + `", "dimension": 2}`),
		collnameErrStr:    []byte(`{"collName": "` + DefaultCollectionName + `", "dimension": 2}`),
		collnameDimErrStr: []byte(`{"collName": "` + DefaultCollectionName + `", "dim": 2}`),
		dataErrStr:        []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2}`),
	}
	paths := map[string][]string{
		parseErrStr: {
			URIPrefix + VectorCollectionsCreatePath,
			URIPrefix + VectorCollectionsDropPath,
			URIPrefix + VectorGetPath,
			URIPrefix + VectorSearchPath,
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
		},
		collnameErrStr: {
			URIPrefix + VectorCollectionsDropPath,
			URIPrefix + VectorGetPath,
			URIPrefix + VectorSearchPath,
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
		},
		collnameDimErrStr: {
			URIPrefix + VectorCollectionsCreatePath,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("request parameters wrong", func(t *testing.T) {
				testEngine := initHTTPServer(mocks.NewProxy(t), true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}
}

func TestAuthorization(t *testing.T) {
	proxy.Params.CommonCfg.AuthorizationEnabled = true
	errorStr := "{\"code\":401,\"message\":\"rpc error: code = Unavailable desc = internal: Milvus Proxy is not ready yet. please wait\"}"
	jsons := map[string][]byte{
		errorStr: []byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2, "data":[{"book_id":1,"book_intro":[0.1,0.11],"distance":0.01,"word_count":1000},{"book_id":2,"book_intro":[0.2,0.22],"distance":0.04,"word_count":2000},{"book_id":3,"book_intro":[0.3,0.33],"distance":0.09,"word_count":3000}]}`),
	}
	paths := map[string][]string{
		errorStr: {
			URIPrefix + VectorGetPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				mp, _ = wrapWithDescribeColl(t, mp, ReturnSuccess, 1, nil)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			URIPrefix + VectorCollectionsCreatePath,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			URIPrefix + VectorCollectionsDropPath,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				mp, _ = wrapWithHasCollection(t, mp, ReturnTrue, 1, nil)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			URIPrefix + VectorCollectionsPath,
			URIPrefix + VectorCollectionsDescribePath + "?collectionName=" + DefaultCollectionName,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				testEngine := initHTTPServer(mp, true)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorSearchPath,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

}

func wrapWithDescribeIndex(t *testing.T, mp *mocks.Proxy, returnType int, times int, testCases []testCase) (*mocks.Proxy, []testCase) {
	if mp == nil {
		mp = mocks.NewProxy(t)
	}
	var call *mocks.Proxy_DescribeIndex_Call
	var testcase testCase
	switch returnType {
	case ReturnSuccess:
		call = mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&DefaultDescIndexesReqp, nil)
		testcase = testCase{
			name:         "[share] describe coll success",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":200,\"data\":{}}",
		}
	case ReturnFail:
		call = mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, ErrDefault)
		testcase = testCase{
			name:         "[share] describe index fail",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":400,\"error\":\"" + DefaultErrorMessage + "\",\"message\":\"find index of collection " + DefaultCollectionName + " fail\"}",
		}
	case ReturnWrongStatus:
		reason := "index is not exists"
		call = mp.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IndexNotExist,
			Reason:    reason,
		}}, nil)
		testcase = testCase{
			name:         "[share] describe index fail",
			mp:           mp,
			exceptCode:   200,
			expectedBody: "{\"code\":400,\"error\":\"" + reason + "\",\"message\":\"find index of collection " + DefaultCollectionName + " fail\"}",
		}
	}
	if times == 2 {
		call.Twice()
	} else {
		call.Once()
	}
	if testCases != nil {
		testCases = append(testCases, testcase)
	}
	return mp, testCases
}

func getCollectionSchema(collectionName string) *schemapb.CollectionSchema {
	sch := &schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: false,
	}
	sch.Fields = getFieldSchema()
	return sch
}

func getFieldSchema() []*schemapb.FieldSchema {
	fields := []*schemapb.FieldSchema{
		{
			FieldID:     0,
			Name:        "RowID",
			Description: "RowID field",
			DataType:    schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "f0_tk1",
					Value: "f0_tv1",
				},
			},
		},
		{
			FieldID:     1,
			Name:        "Timestamp",
			Description: "Timestamp field",
			DataType:    schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "f1_tk1",
					Value: "f1_tv1",
				},
			},
		},
		{
			FieldID:     100,
			Name:        "float_vector_field",
			Description: "field 100",
			DataType:    schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "2",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "indexkey",
					Value: "indexvalue",
				},
			},
		},
		{
			FieldID:      101,
			Name:         "int64_field",
			Description:  "field 106",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   []*commonpb.KeyValuePair{},
			IndexParams:  []*commonpb.KeyValuePair{},
			IsPrimaryKey: true,
		},
	}

	return fields
}

func Test_Handles_VectorCollectionsDescribe(t *testing.T) {
	mp := mocks.NewProxy(t)
	h := NewHandlers(mp)
	testEngine := gin.New()
	app := testEngine.Group("/", func(c *gin.Context) {
		username, _, ok := ParseUsernamePassword(c)
		if ok {
			c.Set(ContextUsername, username)
		}
	})
	h.RegisterRoutesToV1(app)

	t.Run("hasn't authenticate", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
	})

	t.Run("auth fail", func(t *testing.T) {
		proxy.Params.CommonCfg.AuthorizationEnabled = true
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		req.SetBasicAuth("test", util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":401,\"message\":\"rpc error: code = Unavailable desc = internal: Milvus Proxy is not ready yet. please wait\"}")
	})

	t.Run("describe collection fail with error", func(t *testing.T) {
		proxy.Params.CommonCfg.AuthorizationEnabled = false
		mp.EXPECT().
			DescribeCollection(mock.Anything, mock.Anything).
			Return(nil, errors.New("error")).
			Once()
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":400,\"error\":\"error\",\"message\":\"describe collection book fail\"}")
	})

	t.Run("describe collection fail with status code", func(t *testing.T) {
		proxy.Params.CommonCfg.AuthorizationEnabled = false
		mp.EXPECT().
			DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil).
			Once()
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":1,\"message\":\"\"}")
	})

	t.Run("get load state and describe index fail with error", func(t *testing.T) {
		mp.EXPECT().
			DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Schema: getCollectionSchema("collectionName"),
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil).
			Once()
		mp.EXPECT().
			GetLoadState(mock.Anything, mock.Anything).
			Return(nil, errors.New("error")).
			Once()
		mp.EXPECT().
			DescribeIndex(mock.Anything, mock.Anything).
			Return(nil, errors.New("error")).
			Once()
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":{\"collectionName\":\"\",\"description\":\"\",\"enableDynamic\":false,\"fields\":[{\"autoId\":false,\"description\":\"RowID field\",\"name\":\"RowID\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"Timestamp field\",\"name\":\"Timestamp\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"field 100\",\"name\":\"float_vector_field\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"},{\"autoId\":false,\"description\":\"field 106\",\"name\":\"int64_field\",\"primaryKey\":true,\"type\":\"Int64\"}],\"indexes\":[],\"load\":\"\",\"shardsNum\":0}}")
	})

	t.Run("get load state and describe index fail with status code", func(t *testing.T) {
		mp.EXPECT().
			DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Schema: getCollectionSchema("collectionName"),
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil).
			Once()
		mp.EXPECT().
			GetLoadState(mock.Anything, mock.Anything).
			Return(&milvuspb.GetLoadStateResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil).
			Once()
		mp.EXPECT().
			DescribeIndex(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeIndexResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil).
			Once()
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":{\"collectionName\":\"\",\"description\":\"\",\"enableDynamic\":false,\"fields\":[{\"autoId\":false,\"description\":\"RowID field\",\"name\":\"RowID\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"Timestamp field\",\"name\":\"Timestamp\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"field 100\",\"name\":\"float_vector_field\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"},{\"autoId\":false,\"description\":\"field 106\",\"name\":\"int64_field\",\"primaryKey\":true,\"type\":\"Int64\"}],\"indexes\":[],\"load\":\"\",\"shardsNum\":0}}")
	})

	t.Run("ok", func(t *testing.T) {
		mp.EXPECT().
			DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Schema: getCollectionSchema("collectionName"),
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil).
			Once()
		mp.EXPECT().
			GetLoadState(mock.Anything, mock.Anything).
			Return(&milvuspb.GetLoadStateResponse{
				State:  commonpb.LoadState_LoadStateLoaded,
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			}, nil).
			Once()
		mp.EXPECT().
			DescribeIndex(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeIndexResponse{
				IndexDescriptions: []*milvuspb.IndexDescription{
					{
						IndexName: "in",
						FieldName: "fn",
						Params: []*commonpb.KeyValuePair{
							{
								Key:   "metric_type",
								Value: "L2",
							},
						},
					},
				},
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil).
			Once()
		req := httptest.NewRequest(http.MethodGet, "/vector/collections/describe?collectionName=book", nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":{\"collectionName\":\"\",\"description\":\"\",\"enableDynamic\":false,\"fields\":[{\"autoId\":false,\"description\":\"RowID field\",\"name\":\"RowID\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"Timestamp field\",\"name\":\"Timestamp\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"field 100\",\"name\":\"float_vector_field\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"},{\"autoId\":false,\"description\":\"field 106\",\"name\":\"int64_field\",\"primaryKey\":true,\"type\":\"Int64\"}],\"indexes\":[],\"load\":\"LoadStateLoaded\",\"shardsNum\":0}}")
	})
}
