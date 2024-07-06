package handler

import (
	"context"
	"encoding/json"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ HandlerClient = (*handlerClientImpl)(nil)

type (
	Producer = producer.Producer
	Consumer = consumer.Consumer
)

// HandlerClient is the interface that wraps streamingpb.StreamingNodeHandlerServiceClient.
type HandlerClient interface {
	// CreateProducer creates a producer.
	// Producer is a stream client, it will be available until context canceled or active close.
	CreateProducer(ctx context.Context, opts *options.ProducerOptions) (Producer, error)

	// CreateConsumer creates a consumer.
	// Consumer is a stream client, it will be available until context canceled or active close.
	CreateConsumer(ctx context.Context, opts *options.ConsumerOptions) (Consumer, error)

	// Close closes the handler client.
	Close()
}

// NewHandlerClient creates a new handler client.
func NewHandlerClient(w types.AssignmentDiscoverWatcher) HandlerClient {
	rb := resolver.NewChannelAssignmentBuilder(w)
	dialTimeout := paramtable.Get().StreamingNodeGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.ChannelAssignmentResolverScheme+":///"+typeutil.StreamingNodeRole,
			dialOptions..., // TODO: we should use dynamic service config in future by add it to resolver.
		)
	})
	watcher := assignment.NewWatcher(rb.Resolver())
	return &handlerClientImpl{
		lifetime:              lifetime.NewLifetime(lifetime.Working),
		service:               lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeHandlerServiceClient),
		rb:                    rb,
		watcher:               watcher,
		sharedProducers:       make(map[string]*typeutil.WeakReference[Producer]),
		sharedProducerKeyLock: lock.NewKeyLock[string](),
		newProducer:           producer.CreateProducer,
		newConsumer:           consumer.CreateConsumer,
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingNodeGrpcClientCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{balancer.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": "milvus.proto.streaming.StreamingNodeHandlerService"},
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
