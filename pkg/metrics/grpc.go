package metrics

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TODO: otel has provided a grpc metrics collector, we should use it instead of this.
	// But current we use the prometheus exporter, so we need to refactor it before using otel meter.
	GRPCServerMetric *grpcprom.ServerMetrics
	GRPCClientMetric *grpcprom.ClientMetrics
)

// RegisterGRPCMetrics registers the grpc metrics.
func RegisterGRPCMetrics(r prometheus.Registerer) {
	GRPCServerMetric = grpcprom.NewServerMetrics(
		grpcprom.WithServerCounterOptions(
			grpcprom.WithNamespace(milvusNamespace),
		),
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramNamespace(milvusNamespace),
		),
		grpcprom.WithContextLabels(NodeIDLabelName),
	)
	r.MustRegister(GRPCServerMetric)

	GRPCClientMetric = grpcprom.NewClientMetrics(
		grpcprom.WithClientCounterOptions(
			grpcprom.WithNamespace(milvusNamespace),
		),
		grpcprom.WithClientHandlingTimeHistogram(
			grpcprom.WithHistogramNamespace(milvusNamespace),
		),
	)
	r.MustRegister(GRPCClientMetric)
}
