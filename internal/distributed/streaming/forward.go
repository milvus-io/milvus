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

package streaming

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrForwardDisabled = errors.New("forward disabled")

// newForwardService creates a new forward service.
func newForwardService(streamingCoordClient client.Client) *forwardServiceImpl {
	fs := &forwardServiceImpl{
		streamingCoordClient: streamingCoordClient,

		mu:                sync.Mutex{},
		isForwardDisabled: false,
		legacyProxy:       nil,
	}
	fs.SetLogger(log.With(log.FieldComponent("forward-service")))
	return fs
}

type ForwardService interface {
	ForwardLegacyProxy(ctx context.Context, request any) (any, error)
}

// forwardServiceImpl is the implementation of FallbackService.
type forwardServiceImpl struct {
	log.Binder

	streamingCoordClient client.Client
	mu                   sync.Mutex
	isForwardDisabled    bool
	legacyProxy          lazygrpc.Service[milvuspb.MilvusServiceClient]
	rb                   resolver.Builder
}

// ForwardLegacyProxy forwards the request to the legacy proxy.
func (fs *forwardServiceImpl) ForwardLegacyProxy(ctx context.Context, request any) (any, error) {
	if err := fs.checkIfForwardDisabledWithLock(ctx); err != nil {
		return nil, err
	}

	return fs.forwardLegacyProxy(ctx, request)
}

// checkIfForwardDisabledWithLock checks if the forward is disabled with lock.
func (fs *forwardServiceImpl) checkIfForwardDisabledWithLock(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.checkIfForwardDisabled(ctx)
}

// forwardLegacyProxy forwards the request to the legacy proxy.
func (fs *forwardServiceImpl) forwardLegacyProxy(ctx context.Context, request any) (any, error) {
	s, err := fs.getLegacyProxyService(ctx)
	if err != nil {
		return nil, err
	}

	var result proto.Message
	switch req := request.(type) {
	case *milvuspb.InsertRequest:
		result, err = s.Insert(ctx, req)
	case *milvuspb.DeleteRequest:
		result, err = s.Delete(ctx, req)
	case *milvuspb.UpsertRequest:
		result, err = s.Upsert(ctx, req)
	case *milvuspb.SearchRequest:
		result, err = s.Search(ctx, req)
	case *milvuspb.HybridSearchRequest:
		result, err = s.HybridSearch(ctx, req)
	case *milvuspb.QueryRequest:
		result, err = s.Query(ctx, req)
	default:
		panic(fmt.Sprintf("unsupported request type: %T", request))
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// checkIfForwardDisabled checks if the forward is disabled.
func (fs *forwardServiceImpl) checkIfForwardDisabled(ctx context.Context) error {
	if fs.isForwardDisabled {
		return ErrForwardDisabled
	}
	v, err := fs.streamingCoordClient.Assignment().GetLatestStreamingVersion(ctx)
	if err != nil {
		return errors.Wrap(err, "when getting latest streaming version")
	}
	if v.GetVersion() != 0 {
		// When streaming version is greater than 0, the forward is disabled,
		// so we return error to indicate caller to use streaming service directly.
		fs.markForwardDisabled()
		return ErrForwardDisabled
	}
	return nil
}

// getLegacyProxyService gets the legacy proxy service.
func (fs *forwardServiceImpl) getLegacyProxyService(ctx context.Context) (milvuspb.MilvusServiceClient, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := fs.checkIfForwardDisabled(ctx); err != nil {
		return nil, err
	}

	fs.initLegacyProxy()
	state, err := fs.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, err
	}
	if len(state.State.Addresses) == 0 {
		// if there's no legacy proxy, the forward is disabled.
		return nil, ErrForwardDisabled
	}
	return fs.legacyProxy.GetService(ctx)
}

// initLegacyProxy initializes the legacy proxy service.
func (fs *forwardServiceImpl) initLegacyProxy() {
	if fs.legacyProxy != nil {
		return
	}
	role := sessionutil.GetSessionPrefixByRole(typeutil.ProxyRole)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	port := paramtable.Get().ProxyGrpcClientCfg.Port.GetAsInt()
	rb := resolver.NewSessionBuilder(etcdCli,
		discoverer.OptSDPrefix(role),
		discoverer.OptSDVersionRange("<2.6.0-dev"), // only select the 2.5.x proxy.
		discoverer.OptSDForcePort(port))            // because the port in session is the internal port, not the public port, so force the port to use when resolving.
	dialTimeout := paramtable.Get().ProxyGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	opts := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.ProxyRole,
			opts...,
		)
	})
	fs.legacyProxy = lazygrpc.WithServiceCreator(conn, milvuspb.NewMilvusServiceClient)
	fs.rb = rb
	fs.Logger().Info("streaming service is not ready, legacy proxy is initiated to forward request", zap.Int("proxyPort", port))
}

// getDialOptions returns the dial options for the legacy proxy.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	opts := paramtable.Get().ProxyGrpcClientCfg.GetDialOptionsFromConfig()
	opts = append(opts, grpc.WithResolvers(rb))

	if paramtable.Get().ProxyGrpcServerCfg.TLSMode.GetAsInt() == 1 || paramtable.Get().ProxyGrpcServerCfg.TLSMode.GetAsInt() == 2 {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{"round_robin": map[string]interface{}{}},
		},
	}
	defaultServiceConfigJSON, err := json.Marshal(defaultServiceConfig)
	if err != nil {
		panic(err)
	}
	opts = append(opts, grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)))

	// Add a unary interceptor to carry incoming metadata to outgoing calls.
	opts = append(opts, grpc.WithUnaryInterceptor(
		func(ctx context.Context, method string, req interface{}, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			// carry incoming metadata into outgoing
			newCtx := ctx
			incomingMD, ok := metadata.FromIncomingContext(ctx)
			if ok {
				newCtx = metadata.NewOutgoingContext(ctx, incomingMD)
			}
			return invoker(newCtx, method, req, reply, cc, opts...)
		},
	))
	return opts
}

// markForwardDisabled marks the forward disabled.
func (fs *forwardServiceImpl) markForwardDisabled() {
	fs.isForwardDisabled = true
	fs.Logger().Info("streaming service is ready, forward is disabled")
	if fs.legacyProxy != nil {
		legacyProxy := fs.legacyProxy
		fs.legacyProxy = nil
		rb := fs.rb
		fs.rb = nil
		go func() {
			legacyProxy.Close()
			fs.Logger().Info("legacy proxy closed")
			rb.Close()
			fs.Logger().Info("resolver builder closed")
		}()
	}
}

// ForwardLegacyProxyUnaryServerInterceptor forwards the request to the legacy proxy.
// When upgrading from 2.5.x to 2.6.x, the streaming service is not ready yet,
// the dml cannot be executed at new 2.6.x proxy until all 2.5.x proxies are down.
//
// so we need to forward the request to the 2.5.x proxy.
func ForwardLegacyProxyUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if info.FullMethod != milvuspb.MilvusService_Insert_FullMethodName &&
			info.FullMethod != milvuspb.MilvusService_Delete_FullMethodName &&
			info.FullMethod != milvuspb.MilvusService_Upsert_FullMethodName &&
			info.FullMethod != milvuspb.MilvusService_Search_FullMethodName &&
			info.FullMethod != milvuspb.MilvusService_HybridSearch_FullMethodName &&
			info.FullMethod != milvuspb.MilvusService_Query_FullMethodName {
			return handler(ctx, req)
		}

		// try to forward the request to the legacy proxy.
		resp, err := WAL().ForwardService().ForwardLegacyProxy(ctx, req)
		if err == nil {
			return resp, nil
		}
		if !errors.Is(err, ErrForwardDisabled) {
			return nil, err
		}
		// if the forward is disabled, do the operation at current proxy.
		return handler(ctx, req)
	}
}
