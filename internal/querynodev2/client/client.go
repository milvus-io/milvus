package client

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/tracer"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Client provides service discovery and gRPC connections to QueryNodes.
// Wraps etcd Session Service Discovery, following the same pattern as
// StreamingNode's ManagerClient.
type Client interface {
	// WatchNodeChanged returns a channel that signals QueryNode membership changes.
	WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

	// GetAllQueryNodes fetches all discovered QueryNode info.
	GetAllQueryNodes(ctx context.Context) (map[int64]qviews.QueryNode, error)

	// GetViewSyncClient returns a ViewSyncServiceClient for making streaming RPCs.
	// Use contextutil.WithPickServerID to route to specific nodes.
	GetViewSyncClient(ctx context.Context) (viewpb.ViewSyncServiceClient, error)

	// Close closes the client and releases resources.
	Close()
}

// NewClient creates a new QueryNode client using etcd session discovery.
func NewClient(etcdCli *clientv3.Client) Client {
	role := sessionutil.GetSessionPrefixByRole(typeutil.QueryNodeRole)
	rb := resolver.NewSessionBuilder(etcdCli, discoverer.OptSDPrefix(role), discoverer.OptSDVersionRange(">=2.6.0-dev"))
	dialTimeout := paramtable.Get().QueryNodeGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.QueryNodeRole,
			dialOptions...,
		)
	})
	return &clientImpl{
		lifetime: typeutil.NewLifetime(),
		stopped:  make(chan struct{}),
		rb:       rb,
		service:  lazygrpc.WithServiceCreator(conn, viewpb.NewViewSyncServiceClient),
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().QueryNodeGrpcClientCfg
	tlsCfg := &paramtable.Get().InternalTLSCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": "milvus.proto.view.ViewSyncService"},
				},
				"waitForReady": true,
				"retryPolicy":  retryPolicy,
			},
		},
	}
	defaultServiceConfigJSON, err := json.Marshal(defaultServiceConfig)
	if err != nil {
		panic(err)
	}
	creds, err := tlsCfg.GetClientCreds(context.Background())
	if err != nil {
		panic(err)
	}
	dialOptions := cfg.GetDialOptionsFromConfig()
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(
			otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionUnaryClientInterceptor(),
			streamingserviceinterceptor.NewStreamingServiceUnaryClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionStreamClientInterceptor(),
			streamingserviceinterceptor.NewStreamingServiceStreamClientInterceptor(),
		),
		grpc.WithReturnConnectionError(),
		grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)),
	)
	return dialOptions
}
