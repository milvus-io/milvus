package manager

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/balancer"
	logserviceinterceptor "github.com/milvus-io/milvus/internal/util/logserviceutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// NewManagerClient creates a new manager client.
func DialContext(ctx context.Context, etcdCli *clientv3.Client) ManagerClient {
	// TODO: grpc configuration.
	rb := resolver.NewSessionBuilder(etcdCli, typeutil.LogNodeRole)
	conn := lazyconn.NewLazyGRPCConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.LogNodeRole,
			grpc.WithBlock(),
			grpc.WithResolvers(rb),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(),
			grpc.WithChainUnaryInterceptor(
				otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
				logserviceinterceptor.NewLogServiceUnaryClientInterceptor(),
			),
			grpc.WithChainStreamInterceptor(
				otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
				interceptor.ClusterInjectionStreamClientInterceptor(),
				logserviceinterceptor.NewLogServiceStreamClientInterceptor(),
			),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 5 * time.Second,
			}),
			grpc.WithReturnConnectionError(),
			grpc.WithDefaultServiceConfig(`{
				"loadBalancingConfig": [{"`+balancer.ServerIDPickerBalancerName+`":{}}]
			}`),
		)
	})
	return &managerClientImpl{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		rb:       rb,
		conn:     conn,
	}
}
