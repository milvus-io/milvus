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

package proxy

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var contentType = "application/json"

func getConfigs(configs map[string]string) gin.HandlerFunc {
	return func(c *gin.Context) {
		bs, err := json.Marshal(configs)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}
		c.Data(http.StatusOK, contentType, bs)
	}
}

func getClusterInfo(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		resp, err := node.metricsCacheManager.GetSystemInfoMetrics()
		// fetch metrics from remote and update local cache if getting metrics failed from local cache
		if err != nil {
			var err1 error
			resp, err1 = getSystemInfoMetrics(c, req, node)
			if err1 != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					mhttp.HTTPReturnMessage: err1.Error(),
				})
				return
			}
			node.metricsCacheManager.UpdateSystemInfoMetrics(resp)
		}

		if !merr.Ok(resp.GetStatus()) {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: resp.Status.Reason,
			})
			return
		}
		c.Data(http.StatusOK, contentType, []byte(resp.GetResponse()))
	}
}

func getConnectedClients(c *gin.Context) {
	clients := connection.GetManager().List()
	ret, err := json.Marshal(clients)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			mhttp.HTTPReturnMessage: err.Error(),
		})
		return
	}
	c.Data(http.StatusOK, contentType, ret)
}

func getDependencies(c *gin.Context) {
	dependencies := make(map[string]interface{})
	dependencies["mq"] = dependency.HealthCheck(paramtable.Get().MQCfg.Type.GetValue())
	etcdConfig := &paramtable.Get().EtcdCfg
	dependencies["metastore"] = etcd.HealthCheck(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdEnableAuth.GetAsBool(),
		etcdConfig.EtcdAuthUserName.GetValue(),
		etcdConfig.EtcdAuthPassword.GetValue(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	ret, err := json.Marshal(dependencies)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			mhttp.HTTPReturnMessage: err.Error(),
		})
		return
	}
	c.Data(http.StatusOK, contentType, ret)
}

// buildReqParams fetch all parameters from query parameter of URL, add them into a map data structure.
// put key and value from query parameter into map, concatenate values with separator if values size is greater than 1
func buildReqParams(c *gin.Context, metricsType string) map[string]interface{} {
	ret := make(map[string]interface{})
	ret[metricsinfo.MetricTypeKey] = metricsType

	queryParams := c.Request.URL.Query()
	for key, values := range queryParams {
		if len(values) > 1 {
			ret[key] = strings.Join(values, metricsinfo.MetricRequestParamsSeparator)
		} else {
			ret[key] = values[0]
		}
	}
	return ret
}

func getQueryComponentMetrics(node *Proxy, metricsType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		params := buildReqParams(c, metricsType)
		req, err := metricsinfo.ConstructGetMetricsRequest(params)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		resp, err := node.queryCoord.GetMetrics(c, req)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}
		c.Data(http.StatusOK, contentType, []byte(resp.GetResponse()))
	}
}

func getDataComponentMetrics(node *Proxy, metricsType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		params := buildReqParams(c, metricsType)
		req, err := metricsinfo.ConstructGetMetricsRequest(params)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		resp, err := node.dataCoord.GetMetrics(c, req)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}
		c.Data(http.StatusOK, contentType, []byte(resp.GetResponse()))
	}
}
