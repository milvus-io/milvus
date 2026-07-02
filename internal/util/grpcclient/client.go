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
	"crypto/x509"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/tracer"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/generic"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type GrpcComponent interface {
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}

// clientConnWrapper is the wrapper for client & conn.
type clientConnWrapper[T GrpcComponent] struct {
	client T
	conn   *grpc.ClientConn
	mut    sync.RWMutex
}

func (c *clientConnWrapper[T]) Pin() {
	c.mut.RLock()
}

func (c *clientConnWrapper[T]) Unpin() {
	c.mut.RUnlock()
}

func (c *clientConnWrapper[T]) Close() error {
	if c.conn != nil {
		c.mut.Lock()
		defer c.mut.Unlock()
		return c.conn.Close()
	}
	return nil
}

// GrpcClient abstracts client of grpc
type GrpcClient[T GrpcComponent] interface {
	SetRole(string)
	GetRole() string
	SetGetAddrFunc(func() (string, error))
	EnableEncryption()
	SetInternalTLSCertPool(cp *x509.CertPool)
	SetInternalTLSServerName(cp string)
	SetNewGrpcClientFunc(func(cc *grpc.ClientConn) T)
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

	// grpcClientPool holds up to poolSize connections used round-robin, to avoid a single
	// HTTP/2 connection's max-concurrent-streams bottleneck under high concurrency. It is
	// grown lazily one connection per call (see GetGrpcClient): the first caller dials
	// connection #1, the next dials #2, and so on until poolSize is reached; later callers
	// just reuse. dialMtx serializes growth so only one connection is dialed at a time.
	//
	// Lock order: dialMtx BEFORE grpcClientMtx, never the reverse. Anything that
	// invalidates the whole pool (resetConnection, Close) must take dialMtx first so an
	// in-flight dial lands in the pool before the pool is torn down — otherwise that
	// dial would re-add a live connection to a pool that was already drained. Never
	// call resetConnection/Close/GetGrpcClient while holding either lock.
	grpcClientPool        []*clientConnWrapper[T]
	roundRobinIdx         atomic.Uint64
	dialMtx               sync.Mutex
	poolSize              int
	encryption            bool
	cpInternalTLS         *x509.CertPool
	addr                  atomic.String
	internalTLSServerName string

	// conn                   *grpc.ClientConn
	grpcClientMtx sync.RWMutex
	role          string
	isNode        bool // pre-calculated is node flag

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
		poolSize:                config.ConnectionPoolSize.GetAsInt(),
	}
}

// SetRole sets role of client
func (c *ClientBase[T]) SetRole(role string) {
	c.role = role
	if strings.HasPrefix(role, typeutil.DataNodeRole) ||
		strings.HasPrefix(role, typeutil.IndexNodeRole) ||
		strings.HasPrefix(role, typeutil.QueryNodeRole) ||
		strings.HasPrefix(role, typeutil.ProxyRole) {
		c.isNode = true
	}
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

func (c *ClientBase[T]) SetInternalTLSCertPool(cp *x509.CertPool) {
	c.cpInternalTLS = cp
}

func (c *ClientBase[T]) SetInternalTLSServerName(cp string) {
	c.internalTLSServerName = cp
}

// SetNewGrpcClientFunc sets newGrpcClient of client
func (c *ClientBase[T]) SetNewGrpcClientFunc(f func(cc *grpc.ClientConn) T) {
	c.newGrpcClient = f
}

// GetGrpcClient returns one grpc client from the connection pool, chosen round-robin.
//
// The pool is grown lazily, one connection per call, instead of dialing poolSize
// connections up front: the first caller dials connection #1, the next #2, ... until
// the pool reaches poolSize, after which callers just reuse it. Dialing happens
// OUTSIDE grpcClientMtx (each dial uses grpc.WithBlock and can block up to DialTimeout),
// and growth is serialized by dialMtx so only one connection is dialed at a time. A
// caller that finds the pool already warm but cannot grab the grow lock reuses an
// existing connection rather than waiting on a dial — so there is no thundering herd of
// concurrent dials and no redundant connections to discard.
func (c *ClientBase[T]) GetGrpcClient(ctx context.Context) (*clientConnWrapper[T], error) {
	size := c.poolSize
	if size <= 0 {
		size = 1
	}

	// Fast path: pool already at target size → round-robin reuse, no lock contention.
	c.grpcClientMtx.RLock()
	n := len(c.grpcClientPool)
	if n >= size {
		w := c.pickLocked()
		c.grpcClientMtx.RUnlock()
		return w, nil
	}
	c.grpcClientMtx.RUnlock()

	// Pool is below target size. Become the sole grower (dialMtx). If the pool is already
	// warm and another caller is growing it, reuse an existing connection instead of waiting.
	if n > 0 && !c.dialMtx.TryLock() {
		c.grpcClientMtx.RLock()
		if len(c.grpcClientPool) > 0 {
			w := c.pickLocked()
			c.grpcClientMtx.RUnlock()
			return w, nil
		}
		c.grpcClientMtx.RUnlock()
		c.dialMtx.Lock() // pool drained meanwhile; block and (re)establish it ourselves
	} else if n == 0 {
		c.dialMtx.Lock() // empty pool: serialize so a single caller dials connection #1
	}
	defer c.dialMtx.Unlock()

	// Re-check under dialMtx: another grower may have advanced or filled the pool.
	c.grpcClientMtx.RLock()
	n = len(c.grpcClientPool)
	addr := c.addr.Load()
	if n >= size && n > 0 {
		w := c.pickLocked()
		c.grpcClientMtx.RUnlock()
		return w, nil
	}
	c.grpcClientMtx.RUnlock()

	// Connections in a pool all target the same address; resolve it only for the first one.
	if n == 0 || addr == "" {
		a, err := c.getAddrFunc()
		if err != nil {
			mlog.Warn(ctx, "failed to get client address", mlog.Err(err))
			return nil, err
		}
		addr = a
	}

	conn, err := c.dialOne(ctx, addr)
	if err != nil {
		// Prefer reusing an existing connection over failing the call outright.
		c.grpcClientMtx.RLock()
		if len(c.grpcClientPool) > 0 {
			w := c.pickLocked()
			c.grpcClientMtx.RUnlock()
			return w, nil
		}
		c.grpcClientMtx.RUnlock()
		return nil, wrapErrConnect(addr, err)
	}
	w := &clientConnWrapper[T]{client: c.newGrpcClient(conn), conn: conn}

	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if len(c.grpcClientPool) == 0 {
		c.addr.Store(addr)
		c.ctxCounter.Store(0)
	}
	c.grpcClientPool = append(c.grpcClientPool, w)
	return c.pickLocked(), nil
}

// pickLocked returns the next pool connection round-robin.
// The caller must hold grpcClientMtx (read or write).
func (c *ClientBase[T]) pickLocked() *clientConnWrapper[T] {
	idx := c.roundRobinIdx.Add(1)
	return c.grpcClientPool[int(idx)%len(c.grpcClientPool)]
}

func (c *ClientBase[T]) resetConnection(wrapper *clientConnWrapper[T], forceReset bool) {
	if !forceReset && time.Since(c.lastReset.Load()) < c.minResetInterval {
		return
	}
	// Take dialMtx first (see lock-order comment on the field): wait for an in-flight
	// dial to land so the connection it adds is torn down with the rest of the pool
	// instead of surviving as a stale entry pointing at the pre-reset address.
	c.dialMtx.Lock()
	defer c.dialMtx.Unlock()
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if !forceReset && time.Since(c.lastReset.Load()) < c.minResetInterval {
		return
	}
	if len(c.grpcClientPool) == 0 {
		return
	}
	// Only reset if wrapper belongs to the current pool; otherwise the pool was already
	// replaced by a concurrent caller and this is a stale wrapper.
	found := false
	for _, w := range c.grpcClientPool {
		if w == wrapper {
			found = true
			break
		}
	}
	if !found {
		return
	}
	// wrapper close may block waiting pending request finish, so close the whole pool async.
	go func(pool []*clientConnWrapper[T], role, addr string) {
		for _, w := range pool {
			w.Close()
		}
		// detached background close; no request context by design.
		mlog.Info(context.Background(), "previous client pool closed", mlog.String("role", role), mlog.String("addr", addr))
	}(c.grpcClientPool, c.role, c.addr.Load())
	c.addr.Store("")
	c.grpcClientPool = nil
	c.lastReset.Store(time.Now())
}

// dialOne establishes a single grpc connection to addr, used to grow the pool.
func (c *ClientBase[T]) dialOne(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	dialContext, cancel := context.WithTimeout(ctx, c.DialTimeout)
	defer cancel()

	var conn *grpc.ClientConn
	var err error
	compress := None
	if c.CompressionEnabled {
		compress = Zstd
	}
	if c.encryption {
		mlog.Debug(ctx, "Running in internalTLS mode with encryption enabled")
		conn, err = grpc.DialContext(
			dialContext,
			addr,
			// #nosec G402
			grpc.WithTransportCredentials(credentials.NewTLS(
				&tls.Config{
					RootCAs:    c.cpInternalTLS,
					ServerName: c.internalTLSServerName,
				},
			)),
			grpc.WithBlock(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(c.ClientMaxRecvSize),
				grpc.MaxCallSendMsgSize(c.ClientMaxSendSize),
				grpc.UseCompressor(compress),
			),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				mlog.UnaryClientInterceptor(),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
				interceptor.ServerIDInjectionUnaryClientInterceptor(c.GetNodeID()),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				mlog.StreamClientInterceptor(),
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
			grpc.FailOnNonTempDialError(true),
			grpc.WithReturnConnectionError(),
			grpc.WithDisableRetry(),
			grpc.WithStatsHandler(tracer.GetDynamicOtelGrpcClientStatsHandler()),
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
				mlog.UnaryClientInterceptor(),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
				interceptor.ServerIDInjectionUnaryClientInterceptor(c.GetNodeID()),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				mlog.StreamClientInterceptor(),
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
			grpc.FailOnNonTempDialError(true),
			grpc.WithReturnConnectionError(),
			grpc.WithDisableRetry(),
			grpc.WithStatsHandler(tracer.GetDynamicOtelGrpcClientStatsHandler()),
		)
	}

	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *ClientBase[T]) verifySession(ctx context.Context) error {
	if !funcutil.CheckCtxValid(ctx) {
		return nil
	}

	log := mlog.With(mlog.String("clientRole", c.GetRole()))
	if time.Since(c.lastSessionCheck.Load()) < c.minSessionCheckInterval {
		log.Debug(ctx, "skip session check, verify too frequent")
		return nil
	}
	c.lastSessionCheck.Store(time.Now())
	if c.sess != nil {
		sessions, _, getSessionErr := c.sess.GetSessions(ctx, c.GetRole())
		if getSessionErr != nil {
			// Only log but not handle this error as it is an auxiliary logic
			log.Warn(ctx, "fail to get session", mlog.Err(getSessionErr))
			return getSessionErr
		}
		if coordSess, exist := sessions[c.GetRole()]; exist {
			if c.GetNodeID() != coordSess.ServerID {
				log.Warn(ctx, "server id mismatch, may connected to a old server, start to reset connection",
					mlog.Int64("client_node", c.GetNodeID()),
					mlog.Int64("current_node", coordSess.ServerID))
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

func (c *ClientBase[T]) checkGrpcErr(ctx context.Context, err error) (needRetry, needReset, forceReset bool, retErr error) {
	log := mlog.With(mlog.String("clientRole", c.GetRole()))
	// Unknown err
	if !funcutil.IsGrpcErr(err) {
		log.Warn(ctx, "fail to grpc call because of unknown error", mlog.Err(err))
		return false, false, false, err
	}

	// grpc err
	log.Warn(ctx, "call received grpc error", mlog.Err(err))
	switch {
	case IsConnectionClosingErr(err):
		// Connection is being torn down, retry is pointless.
		// Fast-fail and force reconnection for next call.
		return false, true, true, err
	case funcutil.IsGrpcErr(err, codes.Canceled, codes.DeadlineExceeded):
		// canceled or deadline exceeded
		return true, c.needResetCancel(), false, err
	case funcutil.IsGrpcErr(err, codes.Unimplemented):
		// for unimplemented error, reset coord connection to avoid old coord's side effect.
		// old coord's side effect: when coord changed, the connection in coord's client won't reset automatically.
		// so if new interface appear in new coord, will got a unimplemented error
		return false, true, true, merr.WrapErrServiceUnimplemented(err)
	case IsServerIDMismatchErr(err):
		if c.isNode {
			// Node connections: fast-fail to let shard client handle failover.
			// Retrying is futile because NodeID (injected via interceptor at
			// connection time) won't change during retry.
			return false, true, true, err
		}
		// Coord connections: retry with force reset (only one coord instance,
		// getAddrFunc may return new address after reset).
		return true, true, true, err
	case IsCrossClusterRoutingErr(err):
		return true, true, true, err
	case funcutil.IsGrpcErr(err, codes.Unavailable):
		// for unavailable error in coord, force to reset coord connection
		return true, true, !c.isNode, err
	default:
		return true, true, false, err
	}
}

// checkNodeSessionExist checks if the session of the node exists.
// If the session does not exist , it will return false, otherwise it will return true.
func (c *ClientBase[T]) checkNodeSessionExist(ctx context.Context) bool {
	if c.isNode {
		err := c.verifySession(ctx)
		if err != nil {
			mlog.Warn(ctx, "failed to verify node session", mlog.Err(err))
		}
		return !errors.Is(err, merr.ErrNodeNotFound)
	}
	return true
}

func (c *ClientBase[T]) call(ctx context.Context, caller func(client T) (any, error)) (any, error) {
	log := mlog.With(mlog.String("client_role", c.GetRole()))
	var (
		ret       any
		clientErr error
		wrapper   *clientConnWrapper[T]
	)

	wrapper, clientErr = c.GetGrpcClient(ctx)
	if clientErr != nil {
		log.Warn(ctx, "fail to get grpc client", mlog.Err(clientErr))
	}

	resetClientFunc := func(forceReset bool) {
		c.resetConnection(wrapper, forceReset)
		wrapper, clientErr = c.GetGrpcClient(ctx)
		if clientErr != nil {
			log.Warn(ctx, "fail to get grpc client in the retry state", mlog.Err(clientErr))
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := retry.Handle(ctx, func() (bool, error) {
		if wrapper == nil {
			if ok := c.checkNodeSessionExist(ctx); !ok {
				// if session doesn't exist, no need to reset connection for datanode/indexnode/querynode
				return false, merr.ErrNodeNotFound
			}

			err := errors.Wrap(clientErr, "empty grpc client")
			log.Warn(ctx, "grpc client is nil, maybe fail to get client in the retry state", mlog.Err(err))
			resetClientFunc(false)
			return true, err
		}

		wrapper.Pin()
		var err error
		ret, err = caller(wrapper.client)
		wrapper.Unpin()

		if err != nil {
			var needRetry, needReset, forceReset bool
			needRetry, needReset, forceReset, err = c.checkGrpcErr(ctx, err)
			if needReset {
				log.Warn(ctx, "start to reset connection because of specific reasons", mlog.Err(err))
				resetClientFunc(forceReset)
			} else {
				// err occurs but no need to reset connection, try to verify session
				err := c.verifySession(ctx)
				if err != nil {
					log.Warn(ctx, "failed to verify session, reset connection", mlog.Err(err))
					resetClientFunc(forceReset)
				}
			}
			return needRetry, err
		}
		// reset counter
		c.ctxCounter.Store(0)

		var status *commonpb.Status
		switch res := ret.(type) {
		case *commonpb.Status:
			status = res
		case interface{ GetStatus() *commonpb.Status }:
			status = res.GetStatus()
		// streaming call
		case grpc.ClientStream:
			status = merr.Status(nil)
		default:
			// it will directly return the result
			log.Warn(ctx, "unknown return type", mlog.Any("return", ret))
			return false, nil
		}

		if status == nil {
			log.Warn(ctx, "status is nil, please fix it", mlog.Stack("stack"))
			return false, nil
		}

		err = merr.Error(status)
		if err != nil && merr.IsRetryableErr(err) {
			return true, err
		}
		return false, nil
	}, retry.Attempts(retry.MaxAttemptsFromContextOrDefault(ctx, uint(c.MaxAttempts))),
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
		mlog.Warn(ctx, "ClientBase Call grpc call get error",
			mlog.String("role", c.GetRole()),
			mlog.String("address", c.GetAddr()),
			mlog.Err(traceErr),
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
	// Take dialMtx first (see lock-order comment on the field): a dial that is in
	// flight when Close is called must land in the pool before we drain it, or its
	// connection would be appended after the drain and leak forever.
	c.dialMtx.Lock()
	defer c.dialMtx.Unlock()
	// Detach the pool under the lock, then close OUTSIDE it. wrapper.Close takes the
	// wrapper's write lock and blocks until in-flight RPCs (which hold it via Pin)
	// finish; holding grpcClientMtx across that would stall every concurrent
	// GetGrpcClient on connection acquisition until those RPCs drain.
	c.grpcClientMtx.Lock()
	pool := c.grpcClientPool
	c.grpcClientPool = nil
	c.grpcClientMtx.Unlock()

	var err error
	for _, w := range pool {
		if e := w.Close(); e != nil {
			err = e
		}
	}
	return err
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
	if err == nil {
		return false
	}
	// GRPC utilizes `status.Status` to encapsulate errors,
	// hence it is not viable to employ the `errors.Is` for assessment.
	return strings.Contains(err.Error(), merr.ErrServiceCrossClusterRouting.Error())
}

func IsServerIDMismatchErr(err error) bool {
	if err == nil {
		return false
	}
	// GRPC utilizes `status.Status` to encapsulate errors,
	// hence it is not viable to employ the `errors.Is` for assessment.
	return strings.Contains(err.Error(), merr.ErrNodeNotMatch.Error())
}

func IsConnectionClosingErr(err error) bool {
	return errors.Is(err, grpc.ErrClientConnClosing)
}
