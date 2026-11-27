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
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

type requestMetricsKey struct{}

// RequestMetrics carries the hook's attribution back to the outer request
// counter. It is separate from the immutable context value used for routing.
type RequestMetrics struct {
	resourceGroup atomic.Value
}

// WithRequestMetrics starts an independent attribution scope for one incoming
// gRPC or REST request, including requests refused before the hook runs.
func WithRequestMetrics(ctx context.Context) (context.Context, *RequestMetrics) {
	m := &RequestMetrics{}
	m.resourceGroup.Store(extension.QueryResourceGroupFromContext(ctx))
	return context.WithValue(ctx, requestMetricsKey{}, m), m
}

// ResourceGroup returns the admitted query's group, or empty for unscoped requests.
func (m *RequestMetrics) ResourceGroup() string {
	return m.resourceGroup.Load().(string)
}

// Record counts one returned request or attempt. Capture db and collection
// before execution, and call Record after the hook has selected its group.
func (m *RequestMetrics) Record(method, db, collection string, resp any, err error) (label, cause string) {
	label, cause = requestutil.ParseMetricLabel(resp, err)
	nodeID, resourceGroup := paramtable.GetStringNodeID(), m.ResourceGroup()
	metrics.ProxyFunctionCall.WithLabelValues(nodeID, method, metrics.TotalLabel, metrics.CauseNA, db, collection, resourceGroup).Inc()
	metrics.ProxyFunctionCall.WithLabelValues(nodeID, method, label, cause, db, collection, resourceGroup).Inc()
	return label, cause
}

func setRequestResourceGroup(ctx, admitted context.Context) {
	m, _ := ctx.Value(requestMetricsKey{}).(*RequestMetrics)
	if m == nil {
		return
	}
	// REST may make additional unscoped metadata calls inside the same request.
	// They must not erase the query's attribution.
	if rg := extension.QueryResourceGroupFromContext(admitted); rg != "" {
		m.resourceGroup.Store(rg)
	}
}
