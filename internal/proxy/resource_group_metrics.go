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
	"context"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

// observeResourceGroupSQLatency records a finished search or query against the
// resource group the request was scoped to (extension.WithQueryResourceGroup).
// An unscoped request - every request on a deployment that does not pin
// queries to a resource group - emits nothing, so the series exist only where
// the scope does.
func observeResourceGroupSQLatency(ctx context.Context, queryType, dbName, collectionName string, latencyMs int64) {
	resourceGroup := extension.QueryResourceGroupFromContext(ctx)
	if resourceGroup == "" {
		return
	}
	metrics.ProxyResourceGroupSQLatency.WithLabelValues(
		paramtable.GetStringNodeID(),
		queryType,
		dbName,
		collectionName,
		resourceGroup,
	).Observe(float64(latencyMs))
}

// observeResourceGroupSearchVectors counts the vectors a finished search scoped
// to a resource group searched, against that group. Like the latency above, an
// unscoped request emits nothing.
func observeResourceGroupSearchVectors(ctx context.Context, dbName, collectionName string, numVectors int64) {
	resourceGroup := extension.QueryResourceGroupFromContext(ctx)
	if resourceGroup == "" {
		return
	}
	metrics.ProxyResourceGroupSearchVectors.WithLabelValues(
		paramtable.GetStringNodeID(),
		dbName,
		collectionName,
		resourceGroup,
	).Add(float64(numVectors))
}

// observeResourceGroupRequest counts a request the hook scoped to a resource
// group, once as total and once under the status and cause its result maps to,
// the way the request stats interceptor counts every request in
// ProxyFunctionCall. It has to run here, on the context the hook's Before
// returned: the stats interceptor wraps the hook and never sees that context,
// so it cannot tell which resource group served the request. Requests the hook
// answered itself (Mock) or refused (Before) were never scoped and are not
// counted.
func observeResourceGroupRequest(ctx context.Context, fullMethod string, req, resp any, err error) {
	resourceGroup := extension.QueryResourceGroupFromContext(ctx)
	if resourceGroup == "" {
		return
	}
	method := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	if method == "" {
		return
	}
	dbName, _ := requestutil.GetDbNameFromRequest(req)
	collectionName, _ := requestutil.GetCollectionNameFromRequest(req)
	db, _ := dbName.(string)
	collection, _ := collectionName.(string)
	nodeID := paramtable.GetStringNodeID()
	status, cause := requestutil.ParseMetricLabel(resp, err)
	metrics.ProxyResourceGroupFunctionCall.WithLabelValues(
		nodeID, method, metrics.TotalLabel, metrics.CauseNA, db, collection, resourceGroup).Inc()
	metrics.ProxyResourceGroupFunctionCall.WithLabelValues(
		nodeID, method, status, cause, db, collection, resourceGroup).Inc()
}
