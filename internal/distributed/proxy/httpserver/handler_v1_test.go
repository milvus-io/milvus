package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	Reason:    "",
}

var ErrDefault = errors.New(DefaultErrorMessage)

var DefaultShowCollectionsResp = milvuspb.ShowCollectionsResponse{
	Status:          &StatusSuccess,
	CollectionNames: []string{DefaultCollectionName},
}

var DefaultDescCollectionResp = milvuspb.DescribeCollectionResponse{
	CollectionName: DefaultCollectionName,
	Schema:         generateCollectionSchema(schemapb.DataType_Int64, false),
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

func versional(path string) string {
	return URIPrefixV1 + path
}

func initHTTPServer(proxy types.ProxyComponent, needAuth bool) *gin.Engine {
	h := NewHandlers(proxy)
	ginHandler := gin.Default()
	ginHandler.Use(func(c *gin.Context) {
		_, err := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		if err != nil {
			httpParams := &paramtable.Get().HTTPCfg
			if httpParams.AcceptTypeAllowInt64.GetAsBool() {
				c.Request.Header.Set(HTTPHeaderAllowInt64, "true")
			} else {
				c.Request.Header.Set(HTTPHeaderAllowInt64, "false")
			}
		}
		c.Next()
	})
	app := ginHandler.Group(URIPrefixV1, genAuthMiddleWare(needAuth))
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
			c.Set(ContextUsername, "")
			username, password, ok := ParseUsernamePassword(c)
			if !ok {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			} else if username == util.UserRoot && password != util.DefaultRootPassword {
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

	mp := mocks.NewMockProxy(t)
	mp.EXPECT().
		ShowCollections(mock.Anything, mock.Anything).
		Return(&DefaultShowCollectionsResp, nil).
		Twice()

	testEngine := initHTTPServer(mp, true)

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
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	testCases := []testCase{}
	mp0 := mocks.NewMockProxy(t)
	mp0.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "show collections fail",
		mp:           mp0,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	mp1 := mocks.NewMockProxy(t)
	err := merr.WrapErrIoFailedReason("cannot create folder")
	mp1.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
		Status: merr.Status(err),
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "show collections fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp := mocks.NewMockProxy(t)
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
			req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath), nil)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	mp           *mocks.MockProxy
	exceptCode   int
	expectedBody string
	expectedErr  error
}

func TestVectorCollectionsDescribe(t *testing.T) {
	paramtable.Init()
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewMockProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	mp2, _ = wrapWithDescribeIndex(t, mp2, ReturnSuccess, 1, nil)
	testCases = append(testCases, testCase{
		name:         "get load status fail",
		mp:           mp2,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamic\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"" + DefaultIndexName + "\",\"metricType\":\"L2\"}],\"load\":\"\",\"shardsNum\":1}}",
	})

	mp3 := mocks.NewMockProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Once()
	mp3, _ = wrapWithDescribeIndex(t, mp3, ReturnFail, 1, nil)
	testCases = append(testCases, testCase{
		name:         "get indexes fail",
		mp:           mp3,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamic\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[],\"load\":\"LoadStateLoaded\",\"shardsNum\":1}}",
	})

	mp4 := mocks.NewMockProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().GetLoadState(mock.Anything, mock.Anything).Return(&DefaultLoadStateResp, nil).Once()
	mp4, _ = wrapWithDescribeIndex(t, mp4, ReturnSuccess, 1, nil)
	testCases = append(testCases, testCase{
		name:         "show collection details success",
		mp:           mp4,
		exceptCode:   http.StatusOK,
		expectedBody: "{\"code\":200,\"data\":{\"collectionName\":\"" + DefaultCollectionName + "\",\"description\":\"\",\"enableDynamic\":true,\"fields\":[{\"autoId\":false,\"description\":\"\",\"name\":\"book_id\",\"primaryKey\":true,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"word_count\",\"primaryKey\":false,\"type\":\"Int64\"},{\"autoId\":false,\"description\":\"\",\"name\":\"book_intro\",\"primaryKey\":false,\"type\":\"FloatVector(2)\"}],\"indexes\":[{\"fieldName\":\"book_intro\",\"indexName\":\"" + DefaultIndexName + "\",\"metricType\":\"L2\"}],\"load\":\"LoadStateLoaded\",\"shardsNum\":1}}",
	})

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsDescribePath)+"?collectionName="+DefaultCollectionName, nil)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
		testEngine := initHTTPServer(mocks.NewMockProxy(t), true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsDescribePath)+"?"+DefaultCollectionName, nil)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrMissingRequiredParameters))
	})
}

func TestVectorCreateCollection(t *testing.T) {
	paramtable.Init()
	testCases := []testCase{}

	mp1 := mocks.NewMockProxy(t)
	mp1.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionResourceLimitExceeded()
	mp2 := mocks.NewMockProxy(t)
	mp2.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(merr.Status(err), nil).Once()
	testCases = append(testCases, testCase{
		name:         "create collection fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp3 := mocks.NewMockProxy(t)
	mp3.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp3.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "create index fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	mp4 := mocks.NewMockProxy(t)
	mp4.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp4.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(&StatusSuccess, nil).Once()
	mp4.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "load collection fail",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	mp5 := mocks.NewMockProxy(t)
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
			req := httptest.NewRequest(http.MethodPost, versional(VectorCollectionsCreatePath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	testCases := []testCase{}
	_, testCases = wrapWithHasCollection(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithHasCollection(t, nil, ReturnWrongStatus, 1, testCases)
	_, testCases = wrapWithHasCollection(t, nil, ReturnFalse, 1, testCases)

	mp1 := mocks.NewMockProxy(t)
	mp1, _ = wrapWithHasCollection(t, mp1, ReturnTrue, 1, nil)
	mp1.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "drop collection fail",
		mp:           mp1,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
	mp2 := mocks.NewMockProxy(t)
	mp2, _ = wrapWithHasCollection(t, mp2, ReturnTrue, 1, nil)
	mp2.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(merr.Status(err), nil).Once()
	testCases = append(testCases, testCase{
		name:         "drop collection fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp3 := mocks.NewMockProxy(t)
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
			req := httptest.NewRequest(http.MethodPost, versional(VectorCollectionsDropPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	testCases := []testCase{}

	mp2 := mocks.NewMockProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, ErrDefault).Twice()
	testCases = append(testCases, testCase{
		name:         "query fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
	mp3 := mocks.NewMockProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status: merr.Status(err),
	}, nil).Twice()
	testCases = append(testCases, testCase{
		name:         "query fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp4 := mocks.NewMockProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status:         &StatusSuccess,
		FieldsData:     newFieldData([]*schemapb.FieldData{}, 1000),
		CollectionName: DefaultCollectionName,
		OutputFields:   []string{FieldBookID, FieldWordCount, FieldBookIntro},
	}, nil).Twice()
	testCases = append(testCases, testCase{
		name:        "query fail",
		mp:          mp4,
		exceptCode:  200,
		expectedErr: merr.ErrInvalidSearchResult,
	})

	mp5 := mocks.NewMockProxy(t)
	mp5, _ = wrapWithDescribeColl(t, mp5, ReturnSuccess, 1, nil)
	mp5.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status:         &StatusSuccess,
		FieldsData:     generateFieldData(),
		CollectionName: DefaultCollectionName,
		OutputFields:   []string{FieldBookID, FieldWordCount, FieldBookIntro},
	}, nil).Twice()
	testCases = append(testCases, testCase{
		name:         "query success",
		mp:           mp5,
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
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewMockProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Delete(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "delete fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
	mp3 := mocks.NewMockProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: merr.Status(err),
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "delete fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp4 := mocks.NewMockProxy(t)
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
			req := httptest.NewRequest(http.MethodPost, versional(VectorDeletePath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	jsonBodyList := [][]byte{
		[]byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3]}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `" , "filter": "id in [1,2,3]"}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `" , "id": [1,2,3], "filter": "id in [1,2,3]"}`),
	}
	for _, jsonBody := range jsonBodyList {
		t.Run("delete success", func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			mp, _ = wrapWithDescribeColl(t, mp, ReturnSuccess, 1, nil)
			mp.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status: &StatusSuccess,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			bodyReader := bytes.NewReader(jsonBody)
			req := httptest.NewRequest(http.MethodPost, versional(VectorDeletePath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewMockProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Insert(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "insert fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
	mp3 := mocks.NewMockProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: merr.Status(err),
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "insert fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp4 := mocks.NewMockProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: &StatusSuccess,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:        "id type invalid",
		mp:          mp4,
		exceptCode:  200,
		expectedErr: merr.ErrCheckPrimaryKey,
	})

	mp5 := mocks.NewMockProxy(t)
	mp5, _ = wrapWithDescribeColl(t, mp5, ReturnSuccess, 1, nil)
	mp5.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status:    &StatusSuccess,
		IDs:       genIds(schemapb.DataType_Int64),
		InsertCnt: 3,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "insert success",
		mp:           mp5,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[1,2,3]}}",
	})

	mp6 := mocks.NewMockProxy(t)
	mp6, _ = wrapWithDescribeColl(t, mp6, ReturnSuccess, 1, nil)
	mp6.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status:    &StatusSuccess,
		IDs:       genIds(schemapb.DataType_VarChar),
		InsertCnt: 3,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "insert success",
		mp:           mp6,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}",
	})

	rows := generateSearchResult(schemapb.DataType_Int64)
	data, _ := json.Marshal(map[string]interface{}{
		HTTPCollectionName: DefaultCollectionName,
		HTTPReturnData:     rows[0],
	})
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
		mp := mocks.NewMockProxy(t)
		mp, _ = wrapWithDescribeColl(t, mp, ReturnSuccess, 1, nil)
		testEngine := initHTTPServer(mp, true)
		bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "data": {}}`))
		req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
		"[success]kinds of data type": newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false)),
		"[success]use binary vector":  newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, true)),
		"[success]with dynamic field": withDynamicField(newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false))),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(schemapb.DataType_Int64),
				InsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[1,2,3]}}", w.Body.String())
		})
	}
	schemas = map[string]*schemapb.CollectionSchema{
		"with unsupport field type": withUnsupportField(newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false))),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				InsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				UpsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: false, Accept-Type-Allow-Int64: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				InsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			req.Header.Set(HTTPHeaderAllowInt64, "true")
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: false, Accept-Type-Allow-Int64: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				UpsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				InsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: true", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				UpsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":["+idStrs[dataType]+"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[insert]httpCfg.allow: true, Accept-Type-Allow-Int64: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				InsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorInsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
			req.Header.Set(HTTPHeaderAllowInt64, "false")
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "{\"code\":200,\"data\":{\"insertCount\":3,\"insertIds\":[\"1\",\"2\",\"3\"]}}", w.Body.String())
		})
	}

	for _, dataType := range schemas {
		t.Run("[upsert]httpCfg.allow: true, Accept-Type-Allow-Int64: false", func(t *testing.T) {
			schema := newCollectionSchema(generateCollectionSchema(dataType, false))
			mp := mocks.NewMockProxy(t)
			mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionName: DefaultCollectionName,
				Schema:         schema,
				ShardsNum:      ShardNumDefault,
				Status:         &StatusSuccess,
			}, nil).Once()
			mp.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
				Status:    &StatusSuccess,
				IDs:       genIds(dataType),
				UpsertCnt: 3,
			}, nil).Once()
			testEngine := initHTTPServer(mp, true)
			rows := newSearchResult(generateSearchResult(dataType))
			data, _ := json.Marshal(map[string]interface{}{
				HTTPCollectionName: DefaultCollectionName,
				HTTPReturnData:     rows,
			})
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
	testCases := []testCase{}
	_, testCases = wrapWithDescribeColl(t, nil, ReturnFail, 1, testCases)
	_, testCases = wrapWithDescribeColl(t, nil, ReturnWrongStatus, 1, testCases)

	mp2 := mocks.NewMockProxy(t)
	mp2, _ = wrapWithDescribeColl(t, mp2, ReturnSuccess, 1, nil)
	mp2.EXPECT().Upsert(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "upsert fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
	mp3 := mocks.NewMockProxy(t)
	mp3, _ = wrapWithDescribeColl(t, mp3, ReturnSuccess, 1, nil)
	mp3.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: merr.Status(err),
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "upsert fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp4 := mocks.NewMockProxy(t)
	mp4, _ = wrapWithDescribeColl(t, mp4, ReturnSuccess, 1, nil)
	mp4.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status: &StatusSuccess,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:        "id type invalid",
		mp:          mp4,
		exceptCode:  200,
		expectedErr: merr.ErrCheckPrimaryKey,
	})

	mp5 := mocks.NewMockProxy(t)
	mp5, _ = wrapWithDescribeColl(t, mp5, ReturnSuccess, 1, nil)
	mp5.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status:    &StatusSuccess,
		IDs:       genIds(schemapb.DataType_Int64),
		UpsertCnt: 3,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "upsert success",
		mp:           mp5,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[1,2,3]}}",
	})

	mp6 := mocks.NewMockProxy(t)
	mp6, _ = wrapWithDescribeColl(t, mp6, ReturnSuccess, 1, nil)
	mp6.EXPECT().Upsert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
		Status:    &StatusSuccess,
		IDs:       genIds(schemapb.DataType_VarChar),
		UpsertCnt: 3,
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "upsert success",
		mp:           mp6,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":{\"upsertCount\":3,\"upsertIds\":[\"1\",\"2\",\"3\"]}}",
	})

	rows := generateSearchResult(schemapb.DataType_Int64)
	data, _ := json.Marshal(map[string]interface{}{
		HTTPCollectionName: DefaultCollectionName,
		HTTPReturnData:     rows[0],
	})
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testEngine := initHTTPServer(tt.mp, true)
			bodyReader := bytes.NewReader(data)
			req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
		mp := mocks.NewMockProxy(t)
		mp, _ = wrapWithDescribeColl(t, mp, ReturnSuccess, 1, nil)
		testEngine := initHTTPServer(mp, true)
		bodyReader := bytes.NewReader([]byte(`{"collectionName": "` + DefaultCollectionName + `", "data": {}}`))
		req := httptest.NewRequest(http.MethodPost, versional(VectorUpsertPath), bodyReader)
		req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), merr.ErrInvalidInsertData))
		resp := map[string]interface{}{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, nil, err)
	})
}

func genIds(dataType schemapb.DataType) *schemapb.IDs {
	return generateIds(dataType, 3)
}

func TestSearch(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.HTTPCfg.AcceptTypeAllowInt64.Key, "true")
	testCases := []testCase{}

	mp2 := mocks.NewMockProxy(t)
	mp2.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
	testCases = append(testCases, testCase{
		name:         "search fail",
		mp:           mp2,
		exceptCode:   200,
		expectedBody: PrintErr(ErrDefault),
	})

	err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
	mp3 := mocks.NewMockProxy(t)
	mp3.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: merr.Status(err),
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "search fail",
		mp:           mp3,
		exceptCode:   200,
		expectedBody: PrintErr(err),
	})

	mp4 := mocks.NewMockProxy(t)
	mp4.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: &StatusSuccess,
		Results: &schemapb.SearchResultData{
			FieldsData: []*schemapb.FieldData{},
			Scores:     []float32{},
			TopK:       0,
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "search success",
		mp:           mp4,
		exceptCode:   200,
		expectedBody: "{\"code\":200,\"data\":[]}",
	})

	mp5 := mocks.NewMockProxy(t)
	mp5.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
		Status: &StatusSuccess,
		Results: &schemapb.SearchResultData{
			FieldsData: generateFieldData(),
			Scores:     []float32{0.01, 0.04, 0.09},
			TopK:       3,
		},
	}, nil).Once()
	testCases = append(testCases, testCase{
		name:         "search success",
		mp:           mp5,
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
			req := httptest.NewRequest(http.MethodPost, versional(VectorSearchPath), bodyReader)
			req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
}

type ReturnType int

func wrapWithDescribeColl(t *testing.T, mp *mocks.MockProxy, returnType ReturnType, times int, testCases []testCase) (*mocks.MockProxy, []testCase) {
	if mp == nil {
		mp = mocks.NewMockProxy(t)
	}
	var call *mocks.MockProxy_DescribeCollection_Call
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
		err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
		call = mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil)
		testcase = testCase{
			name:         "[share] collection not found",
			mp:           mp,
			exceptCode:   200,
			expectedBody: PrintErr(err),
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

func wrapWithHasCollection(t *testing.T, mp *mocks.MockProxy, returnType ReturnType, times int, testCases []testCase) (*mocks.MockProxy, []testCase) {
	if mp == nil {
		mp = mocks.NewMockProxy(t)
	}
	var call *mocks.MockProxy_HasCollection_Call
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
			name:        "[share] collection not found",
			mp:          mp,
			exceptCode:  200,
			expectedErr: merr.ErrCollectionNotFound,
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
		err := merr.WrapErrCollectionNotFound(DefaultCollectionName)
		call = mp.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil)
		testcase = testCase{
			name:         "[share] unexpected error",
			mp:           mp,
			exceptCode:   200,
			expectedBody: PrintErr(err),
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
	paramtable.Init()
	errStrs := []error{
		merr.ErrIncorrectParameterFormat,
		merr.ErrMissingRequiredParameters,
		merr.ErrMissingRequiredParameters,
		merr.ErrMissingRequiredParameters,
	}
	requestJsons := [][]byte{
		[]byte(`{"collectionName": {"` + DefaultCollectionName + `", "dimension": 2}`),
		[]byte(`{"collName": "` + DefaultCollectionName + `", "dimension": 2}`),
		[]byte(`{"collName": "` + DefaultCollectionName + `", "dim": 2}`),
		[]byte(`{"collectionName": "` + DefaultCollectionName + `"}`),
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
		},
	}
	for i, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("request parameters wrong", func(t *testing.T) {
				testEngine := initHTTPServer(mocks.NewMockProxy(t), true)
				bodyReader := bytes.NewReader(requestJsons[i])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.SetBasicAuth(util.UserRoot, util.DefaultRootPassword)
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
				mp := mocks.NewMockProxy(t)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
				assert.Equal(t, res, w.Body.String())
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
				mp := mocks.NewMockProxy(t)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
				assert.Equal(t, res, w.Body.String())
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
				mp := mocks.NewMockProxy(t)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
				assert.Equal(t, res, w.Body.String())
			})
		}
	}

	paths = map[string][]string{
		errorStr: {
			versional(VectorCollectionsPath),
			versional(VectorCollectionsDescribePath) + "?collectionName=" + DefaultCollectionName,
		},
	}
	for res, pathArr := range paths {
		for _, path := range pathArr {
			t.Run("proxy is not ready", func(t *testing.T) {
				mp := mocks.NewMockProxy(t)
				testEngine := initHTTPServer(mp, true)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
				assert.Equal(t, res, w.Body.String())
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
				mp := mocks.NewMockProxy(t)
				testEngine := initHTTPServer(mp, true)
				bodyReader := bytes.NewReader(jsons[res])
				req := httptest.NewRequest(http.MethodPost, path, bodyReader)
				req.Header.Set("authorization", "Bearer test:test")
				w := httptest.NewRecorder()
				testEngine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusForbidden, w.Code)
				assert.Equal(t, res, w.Body.String())
			})
		}
	}
}

func TestDatabaseNotFound(t *testing.T) {
	paramtable.Init()

	t.Run("list database fail", func(t *testing.T) {
		mp := mocks.NewMockProxy(t)
		mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, ErrDefault).Once()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath)+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), ErrDefault))
	})

	t.Run("list database without success code", func(t *testing.T) {
		mp := mocks.NewMockProxy(t)
		err := errors.New("unexpected error")
		mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status: merr.Status(err),
		}, nil).Once()
		testEngine := initHTTPServer(mp, true)
		req := httptest.NewRequest(http.MethodGet, versional(VectorCollectionsPath)+"?dbName=test", nil)
		req.Header.Set("authorization", "Bearer root:Milvus")
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, true, CheckErrCode(w.Body.String(), err))
	})

	t.Run("list database success", func(t *testing.T) {
		mp := mocks.NewMockProxy(t)
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
				mp := mocks.NewMockProxy(t)
				mp.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
					Status:  &StatusSuccess,
					DbNames: []string{"default"},
				}, nil).Once()
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
				mp := mocks.NewMockProxy(t)
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
				assert.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, true, CheckErrCode(w.Body.String(), theError))
			})
		}
	}
}

func wrapWithDescribeIndex(t *testing.T, mp *mocks.MockProxy, returnType int, times int, testCases []testCase) (*mocks.MockProxy, []testCase) {
	if mp == nil {
		mp = mocks.NewMockProxy(t)
	}
	var call *mocks.MockProxy_DescribeIndex_Call
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
