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
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	contentType        = "application/json"
	defaultDB          = "default"
	httpDBName         = "db_name"
	HTTPCollectionName = "collection_name"
	UnknownData        = "unknown"
)

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

func getSlowQuery(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		slowQueries := node.slowQueries.Values()
		ret, err := json.Marshal(slowQueries)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}
		c.Data(http.StatusOK, contentType, ret)
	}
}

// buildReqParams fetch all parameters from query parameter of URL, add them into a map data structure.
// put key and value from query parameter into map, concatenate values with separator if values size is greater than 1
func buildReqParams(c *gin.Context, metricsType string, customParams ...*commonpb.KeyValuePair) map[string]interface{} {
	ret := make(map[string]interface{})
	ret[metricsinfo.MetricTypeKey] = metricsType

	for _, kv := range customParams {
		ret[kv.Key] = kv.Value
	}

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

func getQueryComponentMetrics(node *Proxy, metricsType string, customParams ...*commonpb.KeyValuePair) gin.HandlerFunc {
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

func getDataComponentMetrics(node *Proxy, metricsType string, customParams ...*commonpb.KeyValuePair) gin.HandlerFunc {
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

// The Get request should be used to get the query parameters, not the body, such as Javascript
// fetch API only support GET request with query parameter.
func listCollection(node *Proxy) gin.HandlerFunc {
	rootCoord := node.rootCoord
	queryCoord := node.queryCoord
	return func(c *gin.Context) {
		dbName := c.Query(httpDBName)
		if len(dbName) == 0 {
			dbName = defaultDB
		}

		rootCollectionListResp, err := rootCoord.ShowCollections(c, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			DbName: dbName,
		})

		if err := merr.CheckRPCCall(rootCollectionListResp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		queryCollectionListResp, err := queryCoord.ShowLoadCollections(c, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
		})

		if err := merr.CheckRPCCall(queryCollectionListResp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		collectionID2Offset := make(map[int64]int, len(queryCollectionListResp.CollectionIDs))
		for collectionID, offset := range queryCollectionListResp.CollectionIDs {
			collectionID2Offset[offset] = collectionID
		}

		// Convert the response to Collections struct
		collections := &metricsinfo.Collections{
			CollectionIDs: lo.Map(rootCollectionListResp.CollectionIds, func(t int64, i int) string {
				return strconv.FormatInt(t, 10)
			}),
			CollectionNames: rootCollectionListResp.CollectionNames,
			CreatedUtcTimestamps: lo.Map(rootCollectionListResp.CreatedUtcTimestamps, func(t uint64, i int) string {
				return typeutil.TimestampToString(t)
			}),
			InMemoryPercentages: lo.Map(rootCollectionListResp.CollectionIds, func(collectionID int64, i int) string {
				offset, ok := collectionID2Offset[collectionID]
				if !ok {
					return UnknownData
				}

				loadPercentage := queryCollectionListResp.InMemoryPercentages[offset]
				return strconv.FormatInt(loadPercentage, 10)
			}),
			QueryServiceAvailable: lo.Map(rootCollectionListResp.CollectionIds, func(collectionID int64, i int) bool {
				offset, ok := collectionID2Offset[collectionID]
				if !ok {
					return false
				}

				return queryCollectionListResp.QueryServiceAvailable[offset]
			}),
		}

		// Marshal the collections struct to JSON
		collectionsJSON, err := json.Marshal(collections)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		c.Data(http.StatusOK, contentType, collectionsJSON)
	}
}

func describeCollection(node *Proxy) gin.HandlerFunc {
	rootCoord := node.rootCoord
	return func(c *gin.Context) {
		dbName := c.Query(httpDBName)
		collectionName := c.Query(HTTPCollectionName)
		if len(dbName) == 0 {
			dbName = defaultDB
		}
		if len(collectionName) == 0 {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				mhttp.HTTPReturnMessage: HTTPCollectionName + " is required",
			})
			return
		}

		describeCollectionResp, err := rootCoord.DescribeCollection(c, &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DescribeCollection,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		})
		if err := merr.CheckRPCCall(describeCollectionResp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		describePartitionResp, err := rootCoord.ShowPartitions(c, &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowPartitions,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		})

		if err := merr.CheckRPCCall(describePartitionResp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		// Convert the response to Collection struct
		collection := &metricsinfo.Collection{
			CollectionID:         strconv.FormatInt(describeCollectionResp.CollectionID, 10),
			CollectionName:       describeCollectionResp.CollectionName,
			CreatedTime:          typeutil.TimestampToString(describeCollectionResp.CreatedUtcTimestamp),
			ShardsNum:            int(describeCollectionResp.ShardsNum),
			ConsistencyLevel:     describeCollectionResp.ConsistencyLevel.String(),
			Aliases:              describeCollectionResp.Aliases,
			Properties:           funcutil.KeyValuePair2Map(describeCollectionResp.Properties),
			DBName:               dbName,
			NumPartitions:        int(describeCollectionResp.NumPartitions),
			VirtualChannelNames:  describeCollectionResp.VirtualChannelNames,
			PhysicalChannelNames: describeCollectionResp.PhysicalChannelNames,
			PartitionInfos:       metricsinfo.NewPartitionInfos(describePartitionResp),
			EnableDynamicField:   describeCollectionResp.Schema.EnableDynamicField,
			Fields:               metricsinfo.NewFields(describeCollectionResp.GetSchema()),
		}

		// Marshal the collection struct to JSON
		collectionJSON, err := json.Marshal(collection)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		c.Data(http.StatusOK, contentType, collectionJSON)
	}
}

func listDatabase(node types.ProxyComponent) gin.HandlerFunc {
	return func(c *gin.Context) {
		showDatabaseResp, err := node.ListDatabases(c, &milvuspb.ListDatabasesRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ListDatabases,
			},
		})
		if err := merr.CheckRPCCall(showDatabaseResp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		// Convert the response to Databases struct
		databases := metricsinfo.NewDatabases(showDatabaseResp)

		// Marshal the databases struct to JSON
		databasesJSON, err := json.Marshal(databases)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		c.Data(http.StatusOK, contentType, databasesJSON)
	}
}

func describeDatabase(node types.ProxyComponent) gin.HandlerFunc {
	return func(c *gin.Context) {
		dbName := c.Query(httpDBName)
		if len(dbName) == 0 {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				mhttp.HTTPReturnMessage: httpDBName + " is required",
			})
			return
		}

		describeDatabaseResp, err := node.DescribeDatabase(c, &milvuspb.DescribeDatabaseRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DescribeDatabase,
			},
			DbName: dbName,
		})
		if err := merr.CheckRPCCall(describeDatabaseResp, err); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		// Convert the response to Database struct
		database := metricsinfo.NewDatabase(describeDatabaseResp)

		// Marshal the database struct to JSON
		databaseJSON, err := json.Marshal(database)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		c.Data(http.StatusOK, contentType, databaseJSON)
	}
}
