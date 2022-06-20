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
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/backoff"

	"github.com/milvus-io/milvus/internal/util"

	"github.com/milvus-io/milvus/internal/util/crypto"

	grpcopentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// GrpcClient abstracts client of grpc
type GrpcClient interface {
	SetRole(string)
	GetRole() string
	SetGetAddrFunc(func() (string, error))
	SetNewGrpcClientFunc(func(cc *grpc.ClientConn) interface{})
	GetGrpcClient(ctx context.Context) (interface{}, error)
	ReCall(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error)
	Call(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error)
	Close() error
}

// ClientBase is a base of grpc client
type ClientBase struct {
	getAddrFunc   func() (string, error)
	newGrpcClient func(cc *grpc.ClientConn) interface{}

	grpcClient             interface{}
	conn                   *grpc.ClientConn
	grpcClientMtx          sync.RWMutex
	role                   string
	ClientMaxSendSize      int
	ClientMaxRecvSize      int
	RetryServiceNameConfig string

	DialTimeout      time.Duration
	KeepAliveTime    time.Duration
	KeepAliveTimeout time.Duration

	MaxAttempts       int
	InitialBackoff    float32
	MaxBackoff        float32
	BackoffMultiplier float32
}

// SetRole sets role of client
func (c *ClientBase) SetRole(role string) {
	c.role = role
}

// GetRole returns role of client
func (c *ClientBase) GetRole() string {
	return c.role
}

// SetGetAddrFunc sets getAddrFunc of client
func (c *ClientBase) SetGetAddrFunc(f func() (string, error)) {
	c.getAddrFunc = f
}

// SetNewGrpcClientFunc sets newGrpcClient of client
func (c *ClientBase) SetNewGrpcClientFunc(f func(cc *grpc.ClientConn) interface{}) {
	c.newGrpcClient = f
}

// GetGrpcClient returns grpc client
func (c *ClientBase) GetGrpcClient(ctx context.Context) (interface{}, error) {
	c.grpcClientMtx.RLock()

	if c.grpcClient != nil {
		defer c.grpcClientMtx.RUnlock()
		return c.grpcClient, nil
	}
	c.grpcClientMtx.RUnlock()

	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()

	if c.grpcClient != nil {
		return c.grpcClient, nil
	}

	err := c.connect(ctx)
	if err != nil {
		return nil, err
	}

	return c.grpcClient, nil
}

func (c *ClientBase) resetConnection(client interface{}) {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.grpcClient == nil {
		return
	}

	if client != c.grpcClient {
		return
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.grpcClient = nil
}

func (c *ClientBase) connect(ctx context.Context) error {
	addr, err := c.getAddrFunc()
	if err != nil {
		log.Error("failed to get client address", zap.Error(err))
		return err
	}

	opts := trace.GetInterceptorOpts()
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

	conn, err := grpc.DialContext(
		dialContext,
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.ClientMaxRecvSize),
			grpc.MaxCallSendMsgSize(c.ClientMaxSendSize),
		),
		grpc.WithUnaryInterceptor(grpcopentracing.UnaryClientInterceptor(opts...)),
		grpc.WithStreamInterceptor(grpcopentracing.StreamClientInterceptor(opts...)),
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

func (c *ClientBase) callOnce(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error) {
	client, err := c.GetGrpcClient(ctx)
	if err != nil {
		return nil, err
	}

	ret, err2 := caller(client)
	if err2 == nil {
		return ret, nil
	}

	if !funcutil.CheckCtxValid(ctx) {
		return nil, err2
	}
	if !funcutil.IsGrpcErr(err2) {
		log.Debug("ClientBase:isNotGrpcErr", zap.Error(err2))
		return nil, err2
	}
	log.Debug(c.GetRole()+" ClientBase grpc error, start to reset connection", zap.Error(err2))
	c.resetConnection(client)
	return ret, err2
}

// Call does a grpc call
func (c *ClientBase) Call(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err := c.callOnce(ctx, caller)
	if err != nil {
		traceErr := fmt.Errorf("err: %w\n, %s", err, trace.StackTrace())
		log.Error("ClientBase Call grpc first call get error", zap.String("role", c.GetRole()), zap.Error(traceErr))
		return nil, traceErr
	}
	return ret, err
}

// ReCall does the grpc call twice
func (c *ClientBase) ReCall(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err := c.callOnce(ctx, caller)
	if err == nil {
		return ret, nil
	}

	traceErr := fmt.Errorf("err: %w\n, %s", err, trace.StackTrace())
	log.Warn(c.GetRole()+" ClientBase ReCall grpc first call get error ", zap.Error(traceErr))

	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err = c.callOnce(ctx, caller)
	if err != nil {
		traceErr = fmt.Errorf("err: %w\n, %s", err, trace.StackTrace())
		log.Error("ClientBase ReCall grpc second call get error", zap.String("role", c.GetRole()), zap.Error(traceErr))
		return nil, traceErr
	}
	return ret, err
}

// Close close the client connection
func (c *ClientBase) Close() error {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
