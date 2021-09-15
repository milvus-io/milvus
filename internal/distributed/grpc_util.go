package distributed

import (
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/util/trace"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const EtcdServicePrefix = "milvus/services/"
const resolverPrefix = "etcd:///milvus/services/"
const balancerString = `{"loadBalancingPolicy":"%s"}`

// BuildConnections creates connections with grpc server with resolver.
// Connections has a lot of subconns.
// With specific balancer, user can choose which subconn to use.
func BuildConnections(etcdCli *clientv3.Client, endpoints string, opts ...BuildOption) (*grpc.ClientConn, error) {
	c := newDefaultBuildConfig()
	for _, opt := range opts {
		opt(c)
	}

	etcdResolver, err := resolver.NewBuilder(etcdCli)
	if err != nil {
		return nil, err
	}

	grpcOpts := []grpc.DialOption{
		grpc.WithResolvers(etcdResolver), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.maxCallRecvMsgSize),
			grpc.MaxCallSendMsgSize(c.maxCallSendMsgSize)),
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(c.retryTimes),
					grpc_retry.WithCodes(c.retryCodes...),
				),
				grpc_opentracing.UnaryClientInterceptor(c.traceOpts...),
			)),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				grpc_retry.StreamClientInterceptor(
					grpc_retry.WithMax(c.retryTimes),
					grpc_retry.WithCodes(c.retryCodes...),
				),
				grpc_opentracing.StreamClientInterceptor(c.traceOpts...),
			)),
	}

	if len(c.balancer) != 0 {
		grpcOpts = append(grpcOpts, grpc.WithDefaultServiceConfig(fmt.Sprintf(balancerString, c.balancer)))
	}

	conn, err := grpc.Dial(resolverPrefix+endpoints, grpcOpts...)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

type buildConfig struct {
	retryTimes         uint
	retryCodes         []codes.Code
	maxCallRecvMsgSize int
	maxCallSendMsgSize int
	traceOpts          []grpc_opentracing.Option
	balancer           string
}

type BuildOption func(*buildConfig)

func newDefaultBuildConfig() *buildConfig {
	return &buildConfig{
		retryTimes:         3,
		retryCodes:         []codes.Code{codes.Unavailable, codes.Aborted},
		maxCallRecvMsgSize: 100 * 1024 * 1024,
		maxCallSendMsgSize: 100 * 1024 * 1024,
		traceOpts:          trace.GetInterceptorOpts(),
	}
}

// WithRetryTimes configures the number of retries for grpc calls.
func WithRetryTimes(retryTimes uint) BuildOption {
	return func(c *buildConfig) {
		c.retryTimes = retryTimes
	}
}

// WithRetryCodes configures which code should be retried.
func WithRetryCodes(retryCodes ...codes.Code) BuildOption {
	return func(c *buildConfig) {
		c.retryCodes = retryCodes
	}
}

// WithMaxCallRecvMsgSize configures the max receive msg size when call method.
func WithMaxCallRecvMsgSize(maxCallRecvMsgSize int) BuildOption {
	return func(c *buildConfig) {
		c.maxCallRecvMsgSize = maxCallRecvMsgSize
	}
}

// WithMaxCallSendMsgSize configures the max send msg size when call method.
func WithMaxCallSendMsgSize(maxCallSendMsgSize int) BuildOption {
	return func(c *buildConfig) {
		c.maxCallSendMsgSize = maxCallSendMsgSize
	}
}

// WithMaxCallSendMsgSize sets opentracing configuration.
func WithTraceOpts(opts ...grpc_opentracing.Option) BuildOption {
	return func(c *buildConfig) {
		c.traceOpts = append(c.traceOpts, opts...)
	}
}

// WithMaxCallSendMsgSize sets which strategy to use in client.
func WithBalancer(balancer string) BuildOption {
	return func(c *buildConfig) {
		c.balancer = balancer
	}
}

type CallConfig struct {
	Timeout int
	Address string
}

type CallOption func(*CallConfig)

func NewCallConfig() *CallConfig {
	return &CallConfig{
		Timeout: 0,
	}
}

// WithTimeout sets timeout of a grpc call. Default -1 represents no timeout.
func WithTimeout(timeout int) CallOption {
	return func(c *CallConfig) {
		c.Timeout = timeout
	}
}

// WithAddress sets which target to call. Only cluster with specific balancer
// make sense.
func WithAddress(address string) CallOption {
	return func(c *CallConfig) {
		c.Address = address
	}
}
