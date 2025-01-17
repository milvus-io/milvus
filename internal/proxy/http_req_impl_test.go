package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestGetConfigs(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	configs := map[string]string{"key": "value"}
	handler := getConfigs(configs)
	handler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "key")
	assert.Contains(t, w.Body.String(), "value")
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
		qc := mocks.NewMockQueryCoordClient(t)
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		proxy := &Proxy{queryCoord: qc}
		handler := getQueryComponentMetrics(proxy, "system_info")
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("ok", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?key=value", nil)
		qc := mocks.NewMockQueryCoordClient(t)
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Response: "test_response",
		}, nil)
		proxy := &Proxy{queryCoord: qc}
		handler := getQueryComponentMetrics(proxy, "test_metric")
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test_response")
	})
}

func TestGetDataComponentMetrics(t *testing.T) {
	t.Run("get metrics failed", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?key=value", nil)
		dc := mocks.NewMockDataCoordClient(t)
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		proxy := &Proxy{dataCoord: dc}
		handler := getDataComponentMetrics(proxy, "system_info")
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("ok", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?key=value", nil)
		dc := mocks.NewMockDataCoordClient(t)
		dc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Response: "test_response",
		}, nil)
		proxy := &Proxy{dataCoord: dc}
		handler := getDataComponentMetrics(proxy, "test_metric")
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

		mockRoortCoordClient := mocks.NewMockRootCoordClient(t)
		mockRoortCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status:                &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIds:         []int64{1, 2},
			CollectionNames:       []string{"collection1", "collection2"},
			CreatedUtcTimestamps:  []uint64{1633046400000, 1633132800000},
			InMemoryPercentages:   []int64{100, 100},
			QueryServiceAvailable: []bool{true, true},
		}, nil)

		mockQueryCoordClient := mocks.NewMockQueryCoordClient(t)
		mockQueryCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:                &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:         []int64{1},
			InMemoryPercentages:   []int64{100, 100},
			QueryServiceAvailable: []bool{true, true},
		}, nil)

		proxy := &Proxy{queryCoord: mockQueryCoordClient, rootCoord: mockRoortCoordClient}
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

		mockRoortCoordClient := mocks.NewMockRootCoordClient(t)
		mockRoortCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		proxy := &Proxy{rootCoord: mockRoortCoordClient}
		handler := listCollection(proxy)
		handler(c)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("list collections with error in QC response", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default", nil)

		mockRoortCoordClient := mocks.NewMockRootCoordClient(t)
		mockRoortCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		mockQueryCoordClient := mocks.NewMockQueryCoordClient(t)
		mockQueryCoordClient.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		proxy := &Proxy{queryCoord: mockQueryCoordClient, rootCoord: mockRoortCoordClient}
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

		mockRootCoord := mocks.NewMockRootCoordClient(t)
		mockRootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
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

		mockRootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)

		proxy := &Proxy{rootCoord: mockRootCoord}
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

		mockRootCoord := mocks.NewMockRootCoordClient(t)
		mockRootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, errors.New("error"))

		proxy := &Proxy{rootCoord: mockRootCoord}
		handler := describeCollection(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "error")
	})

	t.Run("missing collection_name", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?db_name=default", nil)

		mockRootCoord := mocks.NewMockRootCoordClient(t)
		proxy := &Proxy{rootCoord: mockRootCoord}
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
