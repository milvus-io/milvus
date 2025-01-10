package client

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/broadcast"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ Client = (*clientImpl)(nil)

// AssignmentService is the interface of assignment service.
type AssignmentService interface {
	// AssignmentDiscover is used to watches the assignment discovery.
	types.AssignmentDiscoverWatcher
}

// BroadcastService is the interface of broadcast service.
type BroadcastService interface {
	// Broadcast sends a broadcast message to the streaming service.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)
}

// Client is the interface of log service client.
type Client interface {
	Broadcast() BroadcastService

	// Assignment access assignment service.
	Assignment() AssignmentService

	// Close close the client.
	Close()
}

// NewClient creates a new client.
func NewClient(etcdCli *clientv3.Client) Client {
	// StreamingCoord is deployed on DataCoord node.
	role := sessionutil.GetSessionPrefixByRole(typeutil.RootCoordRole)
	rb := resolver.NewSessionExclusiveBuilder(etcdCli, role)
	dialTimeout := paramtable.Get().StreamingCoordGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.RootCoordRole,
			dialOptions...,
		)
	})
	assignmentService := lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingCoordAssignmentServiceClient)
	broadcastService := lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingCoordBroadcastServiceClient)
	return &clientImpl{
		conn:              conn,
		rb:                rb,
		assignmentService: assignment.NewAssignmentService(assignmentService),
		broadcastService:  broadcast.NewBroadcastService(util.MustSelectWALName(), broadcastService),
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingCoordGrpcClientCfg
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
