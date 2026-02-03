package interceptor

import (
	"context"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// NewMetricsServerUnaryInterceptor is a grpc interceptor that adds metrics to the request.
func NewMetricsServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return metrics.GRPCServerMetric.UnaryServerInterceptor(
		grpcprom.WithLabelsFromContext(func(ctx context.Context) prometheus.Labels {
			return prometheus.Labels{
				metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
			}
		}),
	)
}

// NewMetricsStreamServerInterceptor is a grpc interceptor that adds metrics to the request.
func NewMetricsStreamServerInterceptor() grpc.StreamServerInterceptor {
	return metrics.GRPCServerMetric.StreamServerInterceptor(
		grpcprom.WithLabelsFromContext(func(ctx context.Context) prometheus.Labels {
			return prometheus.Labels{
				metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
			}
		}),
	)
}

// NewMetricsClientUnaryInterceptor is a grpc interceptor that adds metrics to the request.
func NewMetricsClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return metrics.GRPCClientMetric.UnaryClientInterceptor(
		grpcprom.WithLabelsFromContext(func(ctx context.Context) prometheus.Labels {
			return prometheus.Labels{
				metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
			}
		}),
	)
}

// NewMetricsClientStreamInterceptor is a grpc interceptor that adds metrics to the request.
func NewMetricsClientStreamInterceptor() grpc.StreamClientInterceptor {
	return metrics.GRPCClientMetric.StreamClientInterceptor(
		grpcprom.WithLabelsFromContext(func(ctx context.Context) prometheus.Labels {
			return prometheus.Labels{
				metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
			}
		}),
	)
}
