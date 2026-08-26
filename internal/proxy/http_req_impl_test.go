package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGetProjectedConfigs(t *testing.T) {
	// getConfigs serves whatever projection the caller passed in, verbatim.
	// Redaction belongs to the config.Manager that owns the keys: only it knows
	// which are declared, and the hook table's keys are unknown to the main one.
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	// The key is one the deleted hideSensitive blacklist did match, so restoring
	// that blacklist would break this assertion rather than leave it green.
	getProjectedConfigs(func() map[string]string {
		return map[string]string{"my.password": "handed-to-us-in-the-clear"}
	})(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "handed-to-us-in-the-clear",
		"the handler must not second-guess its caller's projection")
}

func TestConfigRoutesDoNotReuseDataPlaneAuthorization(t *testing.T) {
	params := paramtable.Get()
	authKey := params.CommonCfg.AuthorizationEnabled.Key
	defer params.Reset(authKey)
	require.NoError(t, params.Save(authKey, "true"))

	// Management-plane authentication is controlled independently by the
	// companion management-auth change. Reusing the data-plane switch here would
	// turn an existing monitoring/RBAC caller into a 401 or 403 after upgrade.
	router := gin.New()
	(&Proxy{}).RegisterRestRouter(router)
	for _, path := range []string{mhttp.ClusterConfigsPath, mhttp.HookConfigsPath} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code, path)
	}
}

func TestGetConfigsRedactsUnknownEnvironment(t *testing.T) {
	const sentinelValue = "proxy-config-view-sentinel"
	t.Setenv("MILVUS_CONF_SERVICE_TOKEN", sentinelValue)
	t.Setenv("DATABASE_URL", sentinelValue)

	base := paramtable.NewBaseTable(paramtable.SkipRemote(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	params := &paramtable.ComponentParam{}
	params.Init(base)
	// Set it here rather than relying on a milvus.yaml being discoverable from
	// this package's working directory; otherwise the assertions below would
	// pass or fail for reasons unrelated to the projection.
	const declaredSecret = "declared-credential-sentinel"
	require.NoError(t, params.Save(params.MinioCfg.SecretAccessKey.Key, declaredSecret))

	// EnvSource imports the whole process environment, so the sentinel really
	// is in the manager; the view is what must not carry it.
	foundRaw := false
	for _, value := range base.Manager().GetConfigs() {
		if strings.Contains(value, sentinelValue) {
			foundRaw = true
			break
		}
	}
	require.True(t, foundRaw, "sentinel was never imported, the test proves nothing")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	getProjectedConfigs(params.GetConfigsView)(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, w.Body.String(), sentinelValue)
	// Neither the value nor the variable name survives: the list of environment
	// variables in the pod is itself worth withholding.
	assert.NotContains(t, w.Body.String(), "DATABASE_URL")
	assert.NotContains(t, w.Body.String(), "MILVUS_CONF_SERVICE_TOKEN")
	// Declared credentials are still named, and masked. Sources lowercase every
	// key, so the projection carries the lowered spelling, not the declared one.
	assert.NotContains(t, w.Body.String(), declaredSecret)
	// Save writes the overlay under the separator-free identity, so that is the
	// spelling the projection is guaranteed to carry whether or not a
	// milvus.yaml was discoverable from this package's working directory.
	assert.Contains(t, w.Body.String(),
		strings.NewReplacer(".", "", "_", "", "/", "").Replace(strings.ToLower(params.MinioCfg.SecretAccessKey.Key)))
	assert.Contains(t, w.Body.String(), sensitiveMark)
}

func TestGetClusterInfo(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	node := getMockProxyRequestMetrics()
	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	handler := getClusterInfo(node)
	handler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "system_info")
}

func TestGetConnectedClients(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	clientInfo := &commonpb.ClientInfo{
		SdkType:    "Golang",
		SdkVersion: "1.0",
	}

	connection.GetManager().Register(context.TODO(), 1000, clientInfo)
	getConnectedClients(c)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Golang")
}

func TestGetDependencies(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	paramtable.Get().Save(paramtable.Get().MQCfg.Type.Key, "unknown")
	paramtable.Get().Reset(paramtable.Get().MQCfg.Type.Key)
	paramtable.Get().Save(paramtable.Get().EtcdCfg.Endpoints.Key, "")
	paramtable.Get().Reset(paramtable.Get().EtcdCfg.Endpoints.Key)

	getDependencies(c)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "mq")
	assert.Contains(t, w.Body.String(), "metastore")
}

func TestBuildReqParams(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/?key1=value1&key2=value2,value3", nil)

	params := buildReqParams(c, "test_metric")
	assert.Equal(t, "test_metric", params[metricsinfo.MetricTypeKey])
	assert.Equal(t, "value1", params["key1"])
	assert.Equal(t, "value2,value3", params["key2"])
}

func TestGetQueryComponentMetrics(t *testing.T) {
	t.Run("get metrics failed", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?key=value", nil)
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		proxy := &Proxy{mixCoord: mixc}
		handler := getQueryComponentMetrics(proxy, "system_info")
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("ok", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?key=value", nil)
		mixc := mocks.NewMockMixCoordClient(t)
		mixc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Response: "test_response",
		}, nil)
		proxy := &Proxy{mixCoord: mixc}
		handler := getQueryComponentMetrics(proxy, "test_metric")
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test_response")
	})
}

func TestListCollection(t *testing.T) {
	t.Run("list collections successfully", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default", nil)

		mockMixCoordClient := mocks.NewMockMixCoordClient(t)
		mockMixCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status:                &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIds:         []int64{1, 2},
			CollectionNames:       []string{"collection1", "collection2"},
			CreatedUtcTimestamps:  []uint64{1633046400000, 1633132800000},
			InMemoryPercentages:   []int64{100, 100},
			QueryServiceAvailable: []bool{true, true},
		}, nil)

		mockMixCoordClient.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:                &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:         []int64{1},
			InMemoryPercentages:   []int64{100, 100},
			QueryServiceAvailable: []bool{true, true},
		}, nil)

		proxy := &Proxy{mixCoord: mockMixCoordClient}
		handler := listCollection(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "collection1")
		assert.Contains(t, w.Body.String(), "collection2")
	})

	t.Run("list collections with error in RC response", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default", nil)

		mockMixCoordClient := mocks.NewMockMixCoordClient(t)
		mockMixCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		proxy := &Proxy{mixCoord: mockMixCoordClient}
		handler := listCollection(proxy)
		handler(c)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("list collections with error in QC response", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default", nil)

		mockRoortCoordClient := mocks.NewMockMixCoordClient(t)
		mockRoortCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		mockRoortCoordClient.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		proxy := &Proxy{mixCoord: mockRoortCoordClient}
		handler := listCollection(proxy)
		handler(c)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})
}

func TestDescribeCollection(t *testing.T) {
	t.Run("describe collection successfully", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default&collection_name=collection1", nil)

		mockMixCoord := mocks.NewMockMixCoordClient(t)
		mockMixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:               &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionID:         1,
			CollectionName:       "collection1",
			CreatedUtcTimestamp:  1633046400000,
			ShardsNum:            2,
			ConsistencyLevel:     commonpb.ConsistencyLevel_Strong,
			Aliases:              []string{"alias1"},
			Properties:           []*commonpb.KeyValuePair{{Key: "key", Value: "value"}},
			VirtualChannelNames:  []string{"vchan1"},
			PhysicalChannelNames: []string{"pchan1"},
			NumPartitions:        1,
			Schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  1,
						Name:     "field1",
						DataType: schemapb.DataType_Int32,
					},
				},
			},
		}, nil)

		mockMixCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)

		proxy := &Proxy{mixCoord: mockMixCoord}
		handler := describeCollection(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "collection1")
		assert.Contains(t, w.Body.String(), "alias1")
	})

	t.Run("describe collection with error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default&collection_name=collection1", nil)

		mockMixCoord := mocks.NewMockMixCoordClient(t)
		mockMixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		proxy := &Proxy{mixCoord: mockMixCoord}
		handler := describeCollection(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("missing collection_name", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default", nil)

		mockMixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mockMixCoord}
		handler := describeCollection(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "collection_name is required")
	})
}

func TestListDatabase(t *testing.T) {
	t.Run("list databases successfully", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		mockProxy := mocks.NewMockProxy(t)
		mockProxy.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status:           &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			DbNames:          []string{"db1", "db2"},
			CreatedTimestamp: []uint64{1633046400000, 1633132800000},
		}, nil)

		handler := listDatabase(mockProxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "db1")
		assert.Contains(t, w.Body.String(), "db2")
	})

	t.Run("list databases with error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		mockProxy := mocks.NewMockProxy(t)
		mockProxy.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		handler := listDatabase(mockProxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})
}

func TestDescribeDatabase(t *testing.T) {
	t.Run("describe database successfully", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=db1", nil)

		mockProxy := mocks.NewMockProxy(t)
		mockProxy.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&milvuspb.DescribeDatabaseResponse{
			Status:           &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			DbName:           "db1",
			DbID:             1,
			CreatedTimestamp: 1633046400000,
			Properties:       []*commonpb.KeyValuePair{{Key: "key", Value: "value"}},
		}, nil)

		handler := describeDatabase(mockProxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "db1")
		assert.Contains(t, w.Body.String(), "key")
		assert.Contains(t, w.Body.String(), "value")
	})

	t.Run("describe database with error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=db1", nil)

		mockProxy := mocks.NewMockProxy(t)
		mockProxy.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		handler := describeDatabase(mockProxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("missing db_name", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		mockProxy := mocks.NewMockProxy(t)

		handler := describeDatabase(mockProxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "db_name is required")
	})
}
