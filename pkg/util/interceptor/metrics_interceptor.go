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

package interceptor

import (
	"context"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// nodeIDLabels stamps every RPC sample with the current process's node id so
// counters and histograms can be sliced per server in Prometheus.
// metrics.RegisterGRPCMetrics must have been called before any of the
// New*Metrics*Interceptor constructors below.
func nodeIDLabels(context.Context) prometheus.Labels {
	return prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}
}

// NewMetricsServerUnaryInterceptor returns a unary server interceptor that records
// per-method counters and latency histograms, labeled with the current node id.
func NewMetricsServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return metrics.GRPCServerMetric.UnaryServerInterceptor(grpcprom.WithLabelsFromContext(nodeIDLabels))
}

// NewMetricsStreamServerInterceptor is the stream counterpart of NewMetricsServerUnaryInterceptor.
func NewMetricsStreamServerInterceptor() grpc.StreamServerInterceptor {
	return metrics.GRPCServerMetric.StreamServerInterceptor(grpcprom.WithLabelsFromContext(nodeIDLabels))
}

// NewMetricsClientUnaryInterceptor returns a unary client interceptor that records
// per-method counters and latency histograms for outgoing RPCs.
func NewMetricsClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return metrics.GRPCClientMetric.UnaryClientInterceptor(grpcprom.WithLabelsFromContext(nodeIDLabels))
}

// NewMetricsClientStreamInterceptor is the stream counterpart of NewMetricsClientUnaryInterceptor.
func NewMetricsClientStreamInterceptor() grpc.StreamClientInterceptor {
	return metrics.GRPCClientMetric.StreamClientInterceptor(grpcprom.WithLabelsFromContext(nodeIDLabels))
}
