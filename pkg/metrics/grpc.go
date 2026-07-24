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

package metrics

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TODO: otel provides a grpc metrics collector, migrate once we switch from
	// the prometheus exporter to otel meters.
	GRPCServerMetric = grpcprom.NewServerMetrics(
		grpcprom.WithServerCounterOptions(
			grpcprom.WithNamespace(milvusNamespace),
		),
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramNamespace(milvusNamespace),
		),
		grpcprom.WithContextLabels(NodeIDLabelName),
	)
	GRPCClientMetric = grpcprom.NewClientMetrics(
		grpcprom.WithClientCounterOptions(
			grpcprom.WithNamespace(milvusNamespace),
		),
		grpcprom.WithClientHandlingTimeHistogram(
			grpcprom.WithHistogramNamespace(milvusNamespace),
		),
	)
)

// RegisterGRPCMetrics registers the shared server/client gRPC metrics collectors.
// Must be called exactly once for a given registry.
func RegisterGRPCMetrics(r prometheus.Registerer) {
	r.MustRegister(GRPCServerMetric)
	r.MustRegister(GRPCClientMetric)
}
