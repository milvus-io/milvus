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
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/connection"
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
