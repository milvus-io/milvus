package manager

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ManagerClient is the client to manage wal instances in all streamingnode.
// ManagerClient wraps the Session Service Discovery.
// Provides the ability to assign and remove wal instances for the channel on streaming node.
type ManagerClient interface {
	// WatchNodeChanged returns a channel that receive the signal that a streaming node change.
	WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

	// CollectAllStatus collects status of all streamingnode, such as load balance attributes.
	CollectAllStatus(ctx context.Context) (map[int64]*types.StreamingNodeStatus, error)

	// Assign a wal instance for the channel on streaming node of given server id.
	Assign(ctx context.Context, pchannel types.PChannelInfoAssigned) error

	// Remove the wal instance for the channel on streaming node of given server id.
	Remove(ctx context.Context, pchannel types.PChannelInfoAssigned) error

	// Close closes the manager client.
	// It close the underlying connection, stop the node watcher and release all resources.
	Close()
}

// NewManagerClient creates a new manager client.
func NewManagerClient(etcdCli *clientv3.Client) ManagerClient {
	role := sessionutil.GetSessionPrefixByRole(typeutil.StreamingNodeRole)
	rb := resolver.NewSessionBuilder(etcdCli, role)
	dialTimeout := paramtable.Get().StreamingNodeGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.StreamingNodeRole,
			dialOptions...,
		)
	})
	return &managerClientImpl{
		lifetime: typeutil.NewLifetime(),
		stopped:  make(chan struct{}),
		rb:       rb,
		service:  lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeManagerServiceClient),
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingNodeGrpcClientCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": "milvus.proto.streaming.StreamingNodeManagerService"},
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
	dialOptions := cfg.GetDialOptionsFromConfig()
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
