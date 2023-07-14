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

package grpcclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/generic"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// GrpcClient abstracts client of grpc
type GrpcClient[T interface {
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}] interface {
	SetRole(string)
	GetRole() string
	SetGetAddrFunc(func() (string, error))
	EnableEncryption()
	SetNewGrpcClientFunc(func(cc *grpc.ClientConn) T)
	GetGrpcClient(ctx context.Context) (T, error)
	ReCall(ctx context.Context, caller func(client T) (any, error)) (any, error)
	Call(ctx context.Context, caller func(client T) (any, error)) (any, error)
	Close() error
	SetNodeID(int64)
	GetNodeID() int64
}

// ClientBase is a base of grpc client
type ClientBase[T interface {
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}] struct {
	getAddrFunc   func() (string, error)
	newGrpcClient func(cc *grpc.ClientConn) T

	grpcClient             T
	encryption             bool
	conn                   *grpc.ClientConn
	grpcClientMtx          sync.RWMutex
	role                   string
	ClientMaxSendSize      int
	ClientMaxRecvSize      int
	CompressionEnabled     bool
	RetryServiceNameConfig string

	DialTimeout      time.Duration
	KeepAliveTime    time.Duration
	KeepAliveTimeout time.Duration

	MaxAttempts       int
	InitialBackoff    float32
	MaxBackoff        float32
	BackoffMultiplier float32
	NodeID            atomic.Int64

	sf singleflight.Group
}

// SetRole sets role of client
func (c *ClientBase[T]) SetRole(role string) {
	c.role = role
}

// GetRole returns role of client
func (c *ClientBase[T]) GetRole() string {
	return c.role
}

// SetGetAddrFunc sets getAddrFunc of client
func (c *ClientBase[T]) SetGetAddrFunc(f func() (string, error)) {
	c.getAddrFunc = f
}

func (c *ClientBase[T]) EnableEncryption() {
	c.encryption = true
}

// SetNewGrpcClientFunc sets newGrpcClient of client
func (c *ClientBase[T]) SetNewGrpcClientFunc(f func(cc *grpc.ClientConn) T) {
	c.newGrpcClient = f
}

// GetGrpcClient returns grpc client
func (c *ClientBase[T]) GetGrpcClient(ctx context.Context) (T, error) {
	c.grpcClientMtx.RLock()

	if !generic.IsZero(c.grpcClient) {
		defer c.grpcClientMtx.RUnlock()
		return c.grpcClient, nil
	}
	c.grpcClientMtx.RUnlock()

	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()

	if !generic.IsZero(c.grpcClient) {
		return c.grpcClient, nil
	}

	err := c.connect(ctx)
	if err != nil {
		return generic.Zero[T](), err
	}

	return c.grpcClient, nil
}

func (c *ClientBase[T]) resetConnection(client T) {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if generic.IsZero(c.grpcClient) {
		return
	}
	if !generic.Equal(client, c.grpcClient) {
		return
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.grpcClient = generic.Zero[T]()
}

func (c *ClientBase[T]) connect(ctx context.Context) error {
	addr, err := c.getAddrFunc()
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client address", zap.Error(err))
		return err
	}

	opts := tracer.GetInterceptorOpts()
	dialContext, cancel := context.WithTimeout(ctx, c.DialTimeout)
	// refer to https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto
	retryPolicy := fmt.Sprintf(`{
		"methodConfig": [{
		  "name": [{"service": "%s"}],
		  "retryPolicy": {
			  "MaxAttempts": %d,
			  "InitialBackoff": "%fs",
			  "MaxBackoff": "%fs",
			  "BackoffMultiplier": %f,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }
		}]}`, c.RetryServiceNameConfig, c.MaxAttempts, c.InitialBackoff, c.MaxBackoff, c.BackoffMultiplier)

	var conn *grpc.ClientConn
	compress := None
	if c.CompressionEnabled {
		compress = Zstd
	}
	if c.encryption {
		conn, err = grpc.DialContext(
			dialContext,
			addr,
			//grpc.WithInsecure(),
			// #nosec G402
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
			grpc.WithBlock(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(c.ClientMaxRecvSize),
				grpc.MaxCallSendMsgSize(c.ClientMaxSendSize),
				grpc.UseCompressor(compress),
			),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				otelgrpc.UnaryClientInterceptor(opts...),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(opts...),
				interceptor.ClusterInjectionStreamClientInterceptor(),
			)),
			grpc.WithDefaultServiceConfig(retryPolicy),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                c.KeepAliveTime,
				Timeout:             c.KeepAliveTimeout,
				PermitWithoutStream: true,
			}),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: c.DialTimeout,
			}),
			grpc.WithPerRPCCredentials(&Token{Value: crypto.Base64Encode(util.MemberCredID)}),
		)
	} else {
		conn, err = grpc.DialContext(
			dialContext,
			addr,
			grpc.WithInsecure(),
			//grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
			grpc.WithBlock(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(c.ClientMaxRecvSize),
				grpc.MaxCallSendMsgSize(c.ClientMaxSendSize),
				grpc.UseCompressor(compress),
			),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				otelgrpc.UnaryClientInterceptor(opts...),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(opts...),
				interceptor.ClusterInjectionStreamClientInterceptor(),
			)),
			grpc.WithDefaultServiceConfig(retryPolicy),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                c.KeepAliveTime,
				Timeout:             c.KeepAliveTimeout,
				PermitWithoutStream: true,
			}),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: c.DialTimeout,
			}),
			grpc.WithPerRPCCredentials(&Token{Value: crypto.Base64Encode(util.MemberCredID)}),
		)
	}

	cancel()
	if err != nil {
		return wrapErrConnect(addr, err)
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}

	c.conn = conn
	c.grpcClient = c.newGrpcClient(c.conn)
	return nil
}

func (c *ClientBase[T]) callOnce(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	client, err := c.GetGrpcClient(ctx)
	if err != nil {
		return generic.Zero[T](), err
	}

	ret, err := caller(client)
	if err == nil {
		return ret, nil
	}

	if !funcutil.CheckCtxValid(ctx) {
		// start bg check in case of https://github.com/milvus-io/milvus/issues/22435
		go c.bgHealthCheck(client)
		return generic.Zero[T](), err
	}
	if IsCrossClusterRoutingErr(err) {
		log.Ctx(ctx).Warn("CrossClusterRoutingErr, start to reset connection",
			zap.String("role", c.GetRole()),
			zap.Error(err),
		)
		c.resetConnection(client)
		return ret, merr.ErrServiceUnavailable // For concealing ErrCrossClusterRouting from the client
	}
	if !funcutil.IsGrpcErr(err) {
		log.Ctx(ctx).Warn("ClientBase:isNotGrpcErr", zap.Error(err))
		return generic.Zero[T](), err
	}
	log.Ctx(ctx).Info("ClientBase grpc error, start to reset connection",
		zap.String("role", c.GetRole()),
		zap.Error(err),
	)
	c.resetConnection(client)
	return ret, err
}

// Call does a grpc call
func (c *ClientBase[T]) Call(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return generic.Zero[T](), ctx.Err()
	}

	ret, err := c.callOnce(ctx, caller)
	if err != nil {
		traceErr := fmt.Errorf("err: %w\n, %s", err, tracer.StackTrace())
		log.Ctx(ctx).Warn("ClientBase Call grpc first call get error",
			zap.String("role", c.GetRole()),
			zap.Error(traceErr),
		)
		return generic.Zero[T](), traceErr
	}
	return ret, err
}

// ReCall does the grpc call twice
func (c *ClientBase[T]) ReCall(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return generic.Zero[T](), ctx.Err()
	}

	ret, err := c.callOnce(ctx, caller)
	if err == nil {
		return ret, nil
	}

	traceErr := fmt.Errorf("err: %w\n, %s", err, tracer.StackTrace())
	log.Ctx(ctx).Warn("ClientBase ReCall grpc first call get error ", zap.String("role", c.GetRole()), zap.Error(traceErr))

	if !funcutil.CheckCtxValid(ctx) {
		return generic.Zero[T](), ctx.Err()
	}

	ret, err = c.callOnce(ctx, caller)
	if err != nil {
		traceErr = fmt.Errorf("err: %w\n, %s", err, tracer.StackTrace())
		log.Ctx(ctx).Warn("ClientBase ReCall grpc second call get error", zap.String("role", c.GetRole()), zap.Error(traceErr))
		return generic.Zero[T](), traceErr
	}
	return ret, err
}

func (c *ClientBase[T]) bgHealthCheck(client T) {
	c.sf.Do("healthcheck", func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), paramtable.Get().CommonCfg.SessionTTL.GetAsDuration(time.Second))
		defer cancel()

		_, err := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
		if err != nil {
			c.resetConnection(client)
		}

		return struct{}{}, nil
	})
}

// Close close the client connection
func (c *ClientBase[T]) Close() error {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SetNodeID set ID role of client
func (c *ClientBase[T]) SetNodeID(nodeID int64) {
	c.NodeID.Store(nodeID)
}

// GetNodeID returns ID of client
func (c *ClientBase[T]) GetNodeID() int64 {
	return c.NodeID.Load()
}

func IsCrossClusterRoutingErr(err error) bool {
	// GRPC utilizes `status.Status` to encapsulate errors,
	// hence it is not viable to employ the `errors.Is` for assessment.
	return strings.Contains(err.Error(), merr.ErrCrossClusterRouting.Error())
}
