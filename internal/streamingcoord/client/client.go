package client

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/broadcast"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Client = (*clientImpl)(nil)

// AssignmentService is the interface of assignment service.
type AssignmentService interface {
	// AssignmentDiscover is used to watches the assignment discovery.
	types.AssignmentDiscoverWatcher

	// UpdateReplicateConfiguration updates the replicate configuration to the milvus cluster.
	UpdateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration) error

	// GetReplicateConfiguration returns the replicate configuration of the milvus cluster.
	GetReplicateConfiguration(ctx context.Context) (*replicateutil.ConfigHelper, error)

	// GetLatestAssignments returns the latest assignment discovery result.
	GetLatestAssignments(ctx context.Context) (*types.VersionedStreamingNodeAssignments, error)

	// UpdateWALBalancePolicy is used to update the WAL balance policy.
	// Return the WAL balance policy after the update.
	// Deprecated: This function is deprecated and will be removed in the future.
	UpdateWALBalancePolicy(ctx context.Context, req *types.UpdateWALBalancePolicyRequest) (*types.UpdateWALBalancePolicyResponse, error)
}

// BroadcastService is the interface of broadcast service.
type BroadcastService interface {
	// Broadcast sends a broadcast message to the streaming service.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Ack sends a broadcast ack to the streaming service.
	Ack(ctx context.Context, msg message.ImmutableMessage) error
}

// Client is the interface of log service client.
type Client interface {
	// Broadcast access broadcast service.
	// Broadcast service will always be available.
	// When streaming service is enabled, the broadcast will use the streaming service.
	// When streaming service is disabled, the broadcast will use legacy msgstream.
	Broadcast() BroadcastService

	// Assignment access assignment service.
	// Assignment service will only be available when streaming service is enabled.
	Assignment() AssignmentService

	// Close close the client.
	Close()
}

// NewClient creates a new client.
func NewClient(etcdCli *clientv3.Client) Client {
	// StreamingCoord is deployed on DataCoord node.
	role := sessionutil.GetSessionPrefixByRole(typeutil.MixCoordRole)
	rb := resolver.NewSessionExclusiveBuilder(etcdCli, role, ">=2.6.0-dev")
	dialTimeout := paramtable.Get().StreamingCoordGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.MixCoordRole,
			dialOptions...,
		)
	})
	assignmentService := lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingCoordAssignmentServiceClient)
	assignmentServiceImpl := assignment.NewAssignmentService(assignmentService)
	broadcastService := lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingCoordBroadcastServiceClient)
	return &clientImpl{
		conn:              conn,
		rb:                rb,
		assignmentService: assignmentServiceImpl,
		broadcastService:  broadcast.NewGRPCBroadcastService(broadcastService),
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingCoordGrpcClientCfg
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
					{"service": "milvus.proto.streaming.StreamingCoordAssignmentService"},
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
