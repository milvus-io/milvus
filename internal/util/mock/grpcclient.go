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
	"crypto/x509"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/generic"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type GRPCClientBase[T any] struct {
	getAddrFunc   func() (string, error)
	newGrpcClient func(cc *grpc.ClientConn) T

	grpcClient       T
	cpInternalTLS    *x509.CertPool
	cpInternalSNI    string
	conn             *grpc.ClientConn
	grpcClientMtx    sync.RWMutex
	GetGrpcClientErr error
	role             string
	nodeID           int64
	sess             *sessionutil.Session
}

func (c *GRPCClientBase[T]) SetGetAddrFunc(f func() (string, error)) {
	c.getAddrFunc = f
}

func (c *GRPCClientBase[T]) GetRole() string {
	return c.role
}

func (c *GRPCClientBase[T]) SetRole(role string) {
	c.role = role
}

func (c *GRPCClientBase[T]) EnableEncryption() {
}

func (c *GRPCClientBase[T]) SetInternalTLSCertPool(cp *x509.CertPool) {
	c.cpInternalTLS = cp
}

func (c *GRPCClientBase[T]) SetInternalTLSServerName(cp string) {
	c.cpInternalSNI = cp
}

func (c *GRPCClientBase[T]) SetNewGrpcClientFunc(f func(cc *grpc.ClientConn) T) {
	c.newGrpcClient = f
}

func (c *GRPCClientBase[T]) GetGrpcClient(ctx context.Context) (T, error) {
	c.grpcClientMtx.RLock()
	defer c.grpcClientMtx.RUnlock()
	c.connect(ctx)
	return c.grpcClient, c.GetGrpcClientErr
}

func (c *GRPCClientBase[T]) resetConnection(client T) {
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

func (c *GRPCClientBase[T]) connect(ctx context.Context, retryOptions ...retry.Option) error {
	c.grpcClient = c.newGrpcClient(c.conn)
	return nil
}

func (c *GRPCClientBase[T]) callOnce(ctx context.Context, caller func(client T) (any, error)) (any, error) {
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

func (c *GRPCClientBase[T]) Call(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err := c.callOnce(ctx, caller)
	if err != nil {
		log.Error("GRPCClientBase[T] Call grpc first call get error ", zap.Error(err))
		return nil, err
	}
	return ret, err
}

func (c *GRPCClientBase[T]) ReCall(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	// omit ctx check in mock first time to let each function has failed context
	ret, err := c.callOnce(ctx, caller)
	if err == nil {
		return ret, nil
	}

	traceErr := fmt.Errorf("err: %s\n, %s", err.Error(), tracer.StackTrace())
	log.Warn("GRPCClientBase[T] client grpc first call get error ", zap.Error(traceErr))

	if !funcutil.CheckCtxValid(ctx) {
		return nil, ctx.Err()
	}

	ret, err = c.callOnce(ctx, caller)
	if err != nil {
		traceErr = fmt.Errorf("err: %s\n, %s", err.Error(), tracer.StackTrace())
		log.Error("GRPCClientBase[T] client grpc second call get error ", zap.Error(traceErr))
		return nil, traceErr
	}
	return ret, err
}

func (c *GRPCClientBase[T]) Close() error {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *GRPCClientBase[T]) GetNodeID() int64 {
	return c.nodeID
}

func (c *GRPCClientBase[T]) SetNodeID(nodeID int64) {
	c.nodeID = nodeID
}

func (c *GRPCClientBase[T]) SetSession(sess *sessionutil.Session) {
	c.sess = sess
}
