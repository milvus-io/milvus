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

package mock

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type GRPCClientBase struct {
	getAddrFunc   func() (string, error)
	newGrpcClient func(cc *grpc.ClientConn) interface{}

	grpcClient       interface{}
	conn             *grpc.ClientConn
	grpcClientMtx    sync.RWMutex
	GetGrpcClientErr error
	role             string
}

func (c *GRPCClientBase) SetGetAddrFunc(f func() (string, error)) {
	c.getAddrFunc = f
}

func (c *GRPCClientBase) GetRole() string {
	return c.role
}

func (c *GRPCClientBase) SetRole(role string) {
	c.role = role
}

func (c *GRPCClientBase) SetNewGrpcClientFunc(f func(cc *grpc.ClientConn) interface{}) {
	c.newGrpcClient = f
}

func (c *GRPCClientBase) GetGrpcClient(ctx context.Context) (interface{}, error) {
	c.grpcClientMtx.RLock()
	defer c.grpcClientMtx.RUnlock()
	c.connect(ctx)
	return c.grpcClient, c.GetGrpcClientErr
}

func (c *GRPCClientBase) resetConnection(client interface{}) {
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

func (c *GRPCClientBase) connect(ctx context.Context, retryOptions ...retry.Option) error {
	c.grpcClient = c.newGrpcClient(c.conn)
	return nil
}

func (c *GRPCClientBase) callOnce(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error) {
	client, err := c.GetGrpcClient(ctx)
	if err != nil {
		return nil, err
	}

	ret, err2 := caller(client)
	if err2 == nil {
		return ret, nil
	}
	if err2 == context.Canceled || err2 == context.DeadlineExceeded {
		return nil, err2
	}

	c.resetConnection(client)
	return ret, err2
}

func (c *GRPCClientBase) Call(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err := c.callOnce(ctx, caller)
	if err != nil {
		traceErr := fmt.Errorf("err: %s\n, %s", err.Error(), trace.StackTrace())
		log.Error("GRPCClientBase Call grpc first call get error ", zap.Error(traceErr))
		return nil, traceErr
	}
	return ret, err
}

func (c *GRPCClientBase) ReCall(ctx context.Context, caller func(client interface{}) (interface{}, error)) (interface{}, error) {
	// omit ctx check in mock first time to let each function has failed context
	ret, err := c.callOnce(ctx, caller)
	if err == nil {
		return ret, nil
	}

	traceErr := fmt.Errorf("err: %s\n, %s", err.Error(), trace.StackTrace())
	log.Warn("GRPCClientBase client grpc first call get error ", zap.Error(traceErr))

	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err = c.callOnce(ctx, caller)
	if err != nil {
		traceErr = fmt.Errorf("err: %s\n, %s", err.Error(), trace.StackTrace())
		log.Error("GRPCClientBase client grpc second call get error ", zap.Error(traceErr))
		return nil, traceErr
	}
	return ret, err
}

func (c *GRPCClientBase) Close() error {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
