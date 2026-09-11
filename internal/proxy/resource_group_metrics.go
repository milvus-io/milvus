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

	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
