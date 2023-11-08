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
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/generic"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	SetSession(sess *sessionutil.Session)
}

// ClientBase is a base of grpc client
type ClientBase[T interface {
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}] struct {
	getAddrFunc   func() (string, error)
	newGrpcClient func(cc *grpc.ClientConn) T

	grpcClient             T
	encryption             bool
	addr                   atomic.String
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

	MaxAttempts    int
	InitialBackoff float64
	MaxBackoff     float64
	// resetInterval is the minimal duration to reset connection
	minResetInterval time.Duration
	lastReset        atomic.Time
	// sessionCheckInterval is the minmal duration to check session, preventing too much etcd pulll
	minSessionCheckInterval time.Duration
	lastSessionCheck        atomic.Time

	// counter for canceled or deadline exceeded
	ctxCounter     atomic.Int32
	maxCancelError int32

	NodeID atomic.Int64
	sess   sessionutil.SessionInterface
}

func NewClientBase[T interface {
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}](config *paramtable.GrpcClientConfig, serviceName string,
) *ClientBase[T] {
	return &ClientBase[T]{
		ClientMaxRecvSize:       config.ClientMaxRecvSize.GetAsInt(),
		ClientMaxSendSize:       config.ClientMaxSendSize.GetAsInt(),
		DialTimeout:             config.DialTimeout.GetAsDuration(time.Millisecond),
		KeepAliveTime:           config.KeepAliveTime.GetAsDuration(time.Millisecond),
		KeepAliveTimeout:        config.KeepAliveTimeout.GetAsDuration(time.Millisecond),
		RetryServiceNameConfig:  serviceName,
		MaxAttempts:             config.MaxAttempts.GetAsInt(),
		InitialBackoff:          config.InitialBackoff.GetAsFloat(),
		MaxBackoff:              config.MaxBackoff.GetAsFloat(),
		CompressionEnabled:      config.CompressionEnabled.GetAsBool(),
		minResetInterval:        config.MinResetInterval.GetAsDuration(time.Millisecond),
		minSessionCheckInterval: config.MinSessionCheckInterval.GetAsDuration(time.Millisecond),
		maxCancelError:          config.MaxCancelError.GetAsInt32(),
	}
}

// SetRole sets role of client
func (c *ClientBase[T]) SetRole(role string) {
	c.role = role
}

// GetRole returns role of client
func (c *ClientBase[T]) GetRole() string {
	return c.role
}

// GetAddr returns address of client
func (c *ClientBase[T]) GetAddr() string {
	return c.addr.Load()
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
	if time.Since(c.lastReset.Load()) < c.minResetInterval {
		return
	}
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if time.Since(c.lastReset.Load()) < c.minResetInterval {
		return
	}
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
	c.addr.Store("")
	c.grpcClient = generic.Zero[T]()
	c.lastReset.Store(time.Now())
}

func (c *ClientBase[T]) connect(ctx context.Context) error {
	addr, err := c.getAddrFunc()
	if err != nil {
		log.Ctx(ctx).Warn("failed to get client address", zap.Error(err))
		return err
	}

	opts := tracer.GetInterceptorOpts()
	dialContext, cancel := context.WithTimeout(ctx, c.DialTimeout)

	var conn *grpc.ClientConn
	compress := None
	if c.CompressionEnabled {
		compress = Zstd
	}
	if c.encryption {
		conn, err = grpc.DialContext(
			dialContext,
			addr,
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
				interceptor.ServerIDInjectionUnaryClientInterceptor(c.GetNodeID()),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(opts...),
				interceptor.ClusterInjectionStreamClientInterceptor(),
				interceptor.ServerIDInjectionStreamClientInterceptor(c.GetNodeID()),
			)),
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
			grpc.FailOnNonTempDialError(true),
			grpc.WithReturnConnectionError(),
			grpc.WithDisableRetry(),
		)
	} else {
		conn, err = grpc.DialContext(
			dialContext,
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(c.ClientMaxRecvSize),
				grpc.MaxCallSendMsgSize(c.ClientMaxSendSize),
				grpc.UseCompressor(compress),
			),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				otelgrpc.UnaryClientInterceptor(opts...),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
				interceptor.ServerIDInjectionUnaryClientInterceptor(c.GetNodeID()),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(opts...),
				interceptor.ClusterInjectionStreamClientInterceptor(),
				interceptor.ServerIDInjectionStreamClientInterceptor(c.GetNodeID()),
			)),
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
			grpc.FailOnNonTempDialError(true),
			grpc.WithReturnConnectionError(),
			grpc.WithDisableRetry(),
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
	c.addr.Store(addr)
	c.ctxCounter.Store(0)
	c.grpcClient = c.newGrpcClient(c.conn)
	return nil
}

func (c *ClientBase[T]) verifySession(ctx context.Context) error {
	if !funcutil.CheckCtxValid(ctx) {
		return nil
	}

	log := log.Ctx(ctx).With(zap.String("clientRole", c.GetRole()))
	if time.Since(c.lastSessionCheck.Load()) < c.minSessionCheckInterval {
		log.Debug("skip session check, verify too frequent")
		return nil
	}
	c.lastSessionCheck.Store(time.Now())
	if c.sess != nil {
		sessions, _, getSessionErr := c.sess.GetSessions(c.GetRole())
		if getSessionErr != nil {
			// Only log but not handle this error as it is an auxiliary logic
			log.Warn("fail to get session", zap.Error(getSessionErr))
		}
		if coordSess, exist := sessions[c.GetRole()]; exist {
			if c.GetNodeID() != coordSess.ServerID {
				log.Warn("server id mismatch, may connected to a old server, start to reset connection",
					zap.Int64("client_node", c.GetNodeID()),
					zap.Int64("current_node", coordSess.ServerID))
				return merr.WrapErrNodeNotMatch(c.GetNodeID(), coordSess.ServerID)
			}
		} else {
			return merr.WrapErrNodeNotFound(c.GetNodeID(), "session not found", c.GetRole())
		}
	}
	return nil
}

func (c *ClientBase[T]) needResetCancel() (needReset bool) {
	val := c.ctxCounter.Add(1)
	if val > c.maxCancelError {
		c.ctxCounter.Store(0)
		return true
	}
	return false
}

func (c *ClientBase[T]) checkGrpcErr(ctx context.Context, err error) (needRetry, needReset bool, retErr error) {
	log := log.Ctx(ctx).With(zap.String("clientRole", c.GetRole()))
	// Unknown err
	if !funcutil.IsGrpcErr(err) {
		log.Warn("fail to grpc call because of unknown error", zap.Error(err))
		return false, false, err
	}

	// grpc err
	log.Warn("call received grpc error", zap.Error(err))
	switch {
	case funcutil.IsGrpcErr(err, codes.Canceled, codes.DeadlineExceeded):
		// canceled or deadline exceeded
		return true, c.needResetCancel(), err
	case funcutil.IsGrpcErr(err, codes.Unimplemented):
		return false, false, merr.WrapErrServiceUnimplemented(err)
	case IsServerIDMismatchErr(err):
		if ok, err := c.checkNodeSessionExist(ctx); !ok {
			// if session doesn't exist, no need to retry for datanode/indexnode/querynode
			return false, false, err
		}
		return true, true, err
	case IsCrossClusterRoutingErr(err):
		return true, true, err
	default:
		return true, true, err
	}
}

func (c *ClientBase[T]) checkNodeSessionExist(ctx context.Context) (bool, error) {
	switch c.GetRole() {
	case typeutil.DataNodeRole, typeutil.IndexNodeRole, typeutil.QueryNodeRole, typeutil.ProxyRole:
		err := c.verifySession(ctx)
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Warn("failed to verify node session", zap.Error(err))
			// stop retry
			return false, err
		}
	}
	return true, nil
}

func (c *ClientBase[T]) call(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	log := log.Ctx(ctx).With(zap.String("client_role", c.GetRole()))
	var (
		ret       any
		clientErr error
		client    T
	)

	client, clientErr = c.GetGrpcClient(ctx)
	if clientErr != nil {
		log.Warn("fail to get grpc client", zap.Error(clientErr))
	}

	resetClientFunc := func() {
		c.resetConnection(client)
		client, clientErr = c.GetGrpcClient(ctx)
		if clientErr != nil {
			log.Warn("fail to get grpc client in the retry state", zap.Error(clientErr))
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := retry.Do(ctx, func() error {
		if generic.IsZero(client) {
			if ok, err := c.checkNodeSessionExist(ctx); !ok {
				// if session doesn't exist, no need to reset connection for datanode/indexnode/querynode
				return retry.Unrecoverable(err)
			}

			err := errors.Wrap(clientErr, "empty grpc client")
			log.Warn("grpc client is nil, maybe fail to get client in the retry state", zap.Error(err))
			resetClientFunc()
			return err
		}
		var err error
		ret, err = caller(client)
		if err != nil {
			var needRetry, needReset bool
			needRetry, needReset, err = c.checkGrpcErr(ctx, err)
			if !needRetry {
				// stop retry
				err = retry.Unrecoverable(err)
			}
			if needReset {
				log.Warn("start to reset connection because of specific reasons", zap.Error(err))
				resetClientFunc()
			} else {
				// err occurs but no need to reset connection, try to verify session
				err := c.verifySession(ctx)
				if err != nil {
					log.Warn("failed to verify session, reset connection", zap.Error(err))
					resetClientFunc()
				}
			}
			return err
		}
		// reset counter
		c.ctxCounter.Store(0)

		var status *commonpb.Status
		switch res := ret.(type) {
		case *commonpb.Status:
			status = res
		case interface{ GetStatus() *commonpb.Status }:
			status = res.GetStatus()
		default:
			// it will directly return the result
			log.Warn("unknown return type", zap.Any("return", ret))
			return nil
		}

		if status == nil {
			log.Warn("status is nil, please fix it", zap.Stack("stack"))
			return nil
		}

		err = merr.Error(status)
		if err != nil && merr.IsRetryableErr(err) {
			return err
		}
		return nil
	}, retry.Attempts(uint(c.MaxAttempts)),
		// Because the previous InitialBackoff and MaxBackoff were float, and the unit was s.
		// For compatibility, this is multiplied by 1000.
		retry.Sleep(time.Duration(c.InitialBackoff*1000)*time.Millisecond),
		retry.MaxSleepTime(time.Duration(c.MaxBackoff*1000)*time.Millisecond))
	// default value list: MaxAttempts 10, InitialBackoff 0.2s, MaxBackoff 10s
	// and consume 52.8s if all retry failed
	if err != nil {
		// make the error more friendly to user
		if IsCrossClusterRoutingErr(err) {
			err = merr.ErrServiceUnavailable
		}

		return generic.Zero[T](), err
	}

	return ret, nil
}

// Call does a grpc call
func (c *ClientBase[T]) Call(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	if !funcutil.CheckCtxValid(ctx) {
		return generic.Zero[T](), ctx.Err()
	}

	ret, err := c.call(ctx, caller)
	if err != nil {
		traceErr := errors.Wrapf(err, "stack trace: %s", tracer.StackTrace())
		log.Ctx(ctx).Warn("ClientBase Call grpc call get error",
			zap.String("role", c.GetRole()),
			zap.String("address", c.GetAddr()),
			zap.Error(traceErr),
		)
		return generic.Zero[T](), traceErr
	}
	return ret, err
}

// ReCall does the grpc call twice
func (c *ClientBase[T]) ReCall(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	// All retry operations are done in `call` function.
	return c.Call(ctx, caller)
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

// SetSession set session role of client
func (c *ClientBase[T]) SetSession(sess *sessionutil.Session) {
	c.sess = sess
}

func IsCrossClusterRoutingErr(err error) bool {
	// GRPC utilizes `status.Status` to encapsulate errors,
	// hence it is not viable to employ the `errors.Is` for assessment.
	return strings.Contains(err.Error(), merr.ErrServiceCrossClusterRouting.Error())
}

func IsServerIDMismatchErr(err error) bool {
	// GRPC utilizes `status.Status` to encapsulate errors,
	// hence it is not viable to employ the `errors.Is` for assessment.
	return strings.Contains(err.Error(), merr.ErrNodeNotMatch.Error())
}
