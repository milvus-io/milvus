package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/pkg/util/merr"

	"github.com/cockroachdb/errors"

	"github.com/gin-gonic/gin"
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
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			} else if username == util.UserRoot && password != util.DefaultRootPassword {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			} else {
				c.Set(ContextUsername, username)
			}
		}
	}
	return func(c *gin.Context) {
		c.Set(ContextUsername, util.UserRoot)
	}
}

func Print(code int32, message string) string {
	return fmt.Sprintf("{\"%s\":%d,\"%s\":\"%s\"}", HTTPReturnCode, code, HTTPReturnMessage, message)
}

func PrintErr(err error) string {
	return Print(Code(err), err.Error())
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
		assert.Equal(t, w.Code, http.StatusUnauthorized)
		assert.Equal(t, w.Body.String(), PrintErr(merr.ErrNeedAuthenticate))
	})

	t.Run("username or password incorrect", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil)
		req.SetBasicAuth(util.UserRoot, util.UserRoot)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusUnauthorized)
		assert.Equal(t, w.Body.String(), PrintErr(merr.ErrNeedAuthenticate))
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
		expectedBody: PrintErr(ErrDefault),
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
		expectedBody: Print(int32(commonpb.ErrorCode_CannotCreateFolder), reason),
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
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamicField\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"" + DefaultIndexName + "\",\"metricType\":\"L2\"}],\"load\":\"\",\"shardsNum\":1}}",
	})

	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Once()
	mp3, _ = wrapWithDescribeIndex(t, mp3, ReturnFail, 1, nil)
	testCases = append(testCases, testCase{
		name:         "get indexes fail",
		mp:           mp3,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamicField\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[],\"load\":\"LoadStateLoaded\",\"shardsNum\":1}}",
	})

	mp4 := mocks.NewProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Once()
	mp4, _ = wrapWithDescribeIndex(t, mp4, ReturnSuccess, 1, nil)
	testCases = append(testCases, testCase{
		name:         "show collection details success",
		mp:           mp4,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamicField\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"" + DefaultIndexName + "\",\"metricType\":\"L2\"}],\"load\":\"LoadStateLoaded\",\"shardsNum\":1}}",
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
		assert.Equal(t, w.Body.String(), PrintErr(merr.ErrMissingRequiredParameters))
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
		expectedBody: PrintErr(ErrDefault),
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
		expectedBody: Print(int32(commonpb.ErrorCode_CannotCreateFile), reason),
	})

	mp3 := mocks.NewProxy(t)
	mp3.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp3.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "create index fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	mp4 := mocks.NewProxy(t)
	mp4.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp4.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp4.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "load collection fail",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
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
		name:         "drop collection fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	reason := "cannot find collection " + DefaultCollectionName
	mp2 := mocks.NewProxy(t)
	mp2, _ = wrapWithHasCollection(t, mp2, ReturnTrue, 1, nil)
	mp2.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_CollectionNotExists, // 4
		Reason:    reason,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "drop collection fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: Print(int32(commonpb.ErrorCode_CollectionNotExists), reason),
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
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 2, nil)
	mp2.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, ErrDefault).Times(3)
	testCases = append(testCases, testCase{
		name:         "query fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	reason := DefaultCollectionName + " name not found"
	mp3 := mocks.NewProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 2, nil)
	mp3.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNameNotFound, // 28
			Reason:    reason,
		},
	}, nil).Times(3)
	testCases = append(testCases, testCase{
		name:         "query fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: Print(int32(commonpb.ErrorCode_CollectionNameNotFound), reason),
	})

	mp4 := mocks.NewProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 2, nil)
	mp4.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status:         &StatusSuccess,
		FieldsData:     generateFieldData(),
		CollectionName: DefaultCollectionName,
		OutputFields:   []string{FieldBookID, FieldWordCount, FieldBookIntro},
	}, nil).Times(3)
	testCases = append(testCases, testCase{
		name:         "query success",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":[{\"book_id\":1,\"book_intro\":[0.1,0.11],\"word_count\":1000},{\"book_id\":2,\"book_intro\":[0.2,0.22],\"word_count\":2000},{\"book_id\":3,\"book_intro\":[0.3,0.33],\"word_count\":3000}]}",
	})

	for _, tt := range testCases {
		reqs := []*http.Request{genQueryRequest(true), genQueryRequest(false), genGetRequest()}
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

	testCases = []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 2, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 2, testCases)
	for _, tt := range testCases {
		reqs := []*http.Request{genQueryRequest(false), genGetRequest()}
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

func genQueryRequest(withOutputFields bool) *http.Request {
	var jsonBody []byte
	if withOutputFields {
		jsonBody = []byte(`{"collectionName": "` + DefaultCollectionName + `" , "filter": "book_id in [1,2,3]", "outputFields": ["*"]}`)
	} else {
		jsonBody = []byte(`{"collectionName": "` + DefaultCollectionName + `" , "filter": "book_id in [1,2,3]"}`)
	}
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
		expectedBody: PrintErr(ErrDefault),
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
		expectedBody: Print(int32(commonpb.ErrorCode_CollectionNameNotFound), reason),
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
		expectedBody: PrintErr(ErrDefault),
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
		expectedBody: Print(int32(commonpb.ErrorCode_CollectionNameNotFound), reason),
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
		expectedBody: PrintErr(merr.ErrCheckPrimaryKey),
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
				HTTPReturnData:     rows[0],
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
		bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "data": {}}`))
		req := httptest.NewRequest(http.MethodPost, "/v1/vector/insert", bodyReader)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, 200)
		assert.Equal(t, w.Body.String(), PrintErr(merr.ErrInvalidInsertData))
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
			assert.Equal(t, w.Body.String(), PrintErr(merr.ErrInvalidInsertData))
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
		expectedBody: PrintErr(ErrDefault),
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
		expectedBody: Print(int32(commonpb.ErrorCode_CollectionNameNotFound), reason),
	})

	mp4 := mocks.NewProxy(t)
	mp4.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: &StatusSuccess,
		Results: &schemapb.SearchResultData{
			FieldsData: generateFieldData(),
			Scores:     []float32{0.01, 0.04, 0.09},
			TopK:       3,
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
			expectedBody: PrintErr(ErrDefault),
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
			expectedBody: PrintErr(merr.ErrCollectionNotFound),
		}
	case ReturnFail:
		call = mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault)
		testcase = testCase{
			name:         "[share] check collection fail",
			mp:           mp,
			exceptCode:   200,
			expectedBody: PrintErr(ErrDefault),
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
			expectedBody: Print(int32(commonpb.ErrorCode_UnexpectedError), reason),
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
	errStrs := []string{
		PrintErr(merr.ErrIncorrectParameterFormat),
		PrintErr(merr.ErrMissingRequiredParameters),
		PrintErr(merr.ErrMissingRequiredParameters),
		PrintErr(merr.ErrMissingRequiredParameters),
	}
	requestJsons := [][]byte{
		[]byte(`{"collectionName": {"` + DefaultCollectionName + `", "dimension": 2}`),
		[]byte(`{"collName": "` + DefaultCollectionName + `", "dimension": 2}`),
		[]byte(`{"collName": "` + DefaultCollectionName + `", "dim": 2}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `", "dimension": 2}`),
	}
	paths := [][]string{
		{
			URIPrefix + VectorCollectionsCreatePath,
			URIPrefix + VectorCollectionsDropPath,
			URIPrefix + VectorGetPath,
			URIPrefix + VectorSearchPath,
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
		}, {
			URIPrefix + VectorCollectionsDropPath,
			URIPrefix + VectorGetPath,
			URIPrefix + VectorSearchPath,
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
		}, {
			URIPrefix + VectorCollectionsCreatePath,
		}, {
			URIPrefix + VectorGetPath,
			URIPrefix + VectorSearchPath,
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
		},
	}
	for i, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("request parameters wrong", func(t *testing.T) {
				testEngine := initHTTPServer(mocks.NewProxy(t), true)
				bodyReader := bytes.NewReader(requestJsons[i])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, 200)
				assert.Equal(t, w.Body.String(), errStrs[i])
			})
		}
	}
}

func TestAuthorization(t *testing.T) {
	proxy.Params.CommonCfg.AuthorizationEnabled = true
	errorStr := Print(int32(65535), "rpc error: code = Unavailable desc = internal: Milvus Proxy is not ready yet. please wait")
	jsons := map[string][]byte{
		errorStr: []byte(`{"collectionName": "` + DefaultCollectionName + `", "vector": [0.1, 0.2], "filter": "id in [2]", "id": [2], "dimension": 2, "data":[{"book_id":1,"book_intro":[0.1,0.11],"distance":0.01,"word_count":1000},{"book_id":2,"book_intro":[0.2,0.22],"distance":0.04,"word_count":2000},{"book_id":3,"book_intro":[0.3,0.33],"distance":0.09,"word_count":3000}]}`),
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
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, http.StatusForbidden)
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
				assert.Equal(t, w.Code, http.StatusForbidden)
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
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, http.StatusForbidden)
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
				assert.Equal(t, w.Code, http.StatusForbidden)
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
				assert.Equal(t, w.Code, http.StatusForbidden)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

}

func TestDatabaseNotFound(t *testing.T) {
	t.Run("list database fail", func(t *testing.T) {
		mp := mocks.NewProxy(t)
		mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, URIPrefix+VectorCollectionsPath+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), PrintErr(ErrDefault))
	})

	t.Run("list database without success code", func(t *testing.T) {
		mp := mocks.NewProxy(t)
		mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "",
			},
		}, nil).Once()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, URIPrefix+VectorCollectionsPath+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), Print(int32(commonpb.ErrorCode_UnexpectedError), ""))
	})

	t.Run("list database success", func(t *testing.T) {
		mp := mocks.NewProxy(t)
		mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status:  &StatusSuccess,
			DbNames: []string{"default", "test"},
		}, nil).Once()
		mp.EXPECT().
			ShowCollections(mock.Anything, mock.Anything).
			Return(&milvuspb.ShowCollectionsResponse{
				Status:          &StatusSuccess,
				CollectionNames: nil,
			}, nil).Once()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, URIPrefix+VectorCollectionsPath+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, w.Body.String(), "{\"code\":200,\"data\":[]}")
	})

	errorStr := PrintErr(merr.ErrDatabaseNotfound)
	paths := map[string][]string{
		errorStr: {
			URIPrefix + VectorCollectionsPath + "?dbName=test",
			URIPrefix + VectorCollectionsDescribePath + "?dbName=test&collectionName=" + DefaultCollectionName,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("GET dbName", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
					Status:  &StatusSuccess,
					DbNames: []string{"default"},
				}, nil).Once()
				testEngine := initHTTPServer(mp, true)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("authorization", "Bearer root:Milvus")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, http.StatusOK)
				assert.Equal(t, w.Body.String(), res)
			})
		}
	}

	requestBody := `{"dbName": "test", "collectionName": "` + DefaultCollectionName + `", "vector": [0.1, 0.2], "filter": "id in [2]", "id": [2], "dimension": 2, "data":[{"book_id":1,"book_intro":[0.1,0.11],"distance":0.01,"word_count":1000},{"book_id":2,"book_intro":[0.2,0.22],"distance":0.04,"word_count":2000},{"book_id":3,"book_intro":[0.3,0.33],"distance":0.09,"word_count":3000}]}`
	paths = map[string][]string{
		requestBody: {
			URIPrefix + VectorCollectionsCreatePath,
			URIPrefix + VectorCollectionsDropPath,
			URIPrefix + VectorInsertPath,
			URIPrefix + VectorDeletePath,
			URIPrefix + VectorQueryPath,
			URIPrefix + VectorGetPath,
			URIPrefix + VectorSearchPath,
		},
	}
	for request, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("POST dbName", func(t *testing.T) {
				mp := mocks.NewProxy(t)
				mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
					Status:  &StatusSuccess,
					DbNames: []string{"default"},
				}, nil).Once()
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader([]byte(request))
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer root:Milvus")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, w.Code, http.StatusOK)
				assert.Equal(t, w.Body.String(), errorStr)
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
			expectedBody: PrintErr(ErrDefault),
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
			expectedBody: Print(int32(commonpb.ErrorCode_IndexNotExist), reason),
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
