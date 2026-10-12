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

package client

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/tracer"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ Client = (*client)(nil)

// Client calls PKIndexCoordService. Every method returns a merr error.
//
// A merr.ErrChannelNotAvailable error means the term of the caller is fenced
// by a newer owner of the vchannel; the caller must stop writing that vchannel.
// A merr.ErrServiceUnavailable error is retryable.
type Client interface {
	// ClaimManifest claims the manifest of a vchannel for the term in the request.
	ClaimManifest(ctx context.Context, req *pkindexpb.ClaimManifestRequest) (*pkindexpb.Manifest, error)

	// CommitFlush commits a flush result to the manifest of a vchannel.
	CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error)

	// GetManifest returns the manifest of a vchannel.
	// The bool result is false if the manifest does not exist.
	GetManifest(ctx context.Context, req *pkindexpb.GetManifestRequest) (*pkindexpb.Manifest, bool, error)

	// Close releases the connection of the client.
	Close()
}

// NewClient creates a client that reaches PKIndexCoordService on the mixcoord.
func NewClient(etcdCli *clientv3.Client) Client {
	role := sessionutil.GetSessionPrefixByRole(typeutil.MixCoordRole)
	rb := resolver.NewSessionBuilder(etcdCli, discoverer.OptSDPrefix(role), discoverer.OptSDExclusive(), discoverer.OptSDVersionRange(">=2.6.0-dev"))
	dialTimeout := paramtable.Get().StreamingCoordGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.MixCoordRole,
			dialOptions...,
		)
	})
	return &client{
		rb:      rb,
		conn:    conn,
		service: lazygrpc.WithServiceCreator(conn, pkindexpb.NewPKIndexCoordServiceClient),
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingCoordGrpcClientCfg
	tlsCfg := &paramtable.Get().InternalTLSCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": "milvus.proto.pkindex.PKIndexCoordService"},
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
	creds, err := tlsCfg.GetClientCreds(context.Background())
	if err != nil {
		panic(err)
	}
	dialOptions := cfg.GetDialOptionsFromConfig()
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(
			mlog.UnaryClientInterceptor(),
			otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionUnaryClientInterceptor(),
		),
		grpc.WithReturnConnectionError(),
		grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)),
	)
	return dialOptions
}

type client struct {
	rb      resolver.Builder
	conn    lazygrpc.Conn
	service lazygrpc.Service[pkindexpb.PKIndexCoordServiceClient]
}

func (c *client) ClaimManifest(ctx context.Context, req *pkindexpb.ClaimManifestRequest) (*pkindexpb.Manifest, error) {
	svc, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := svc.ClaimManifest(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	return resp.GetManifest(), nil
}

func (c *client) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error) {
	svc, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := svc.CommitFlush(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	return resp.GetManifest(), nil
}

func (c *client) GetManifest(ctx context.Context, req *pkindexpb.GetManifestRequest) (*pkindexpb.Manifest, bool, error) {
	svc, err := c.service.GetService(ctx)
	if err != nil {
		return nil, false, err
	}
	resp, err := svc.GetManifest(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, false, err
	}
	return resp.GetManifest(), resp.GetFound(), nil
}

func (c *client) Close() {
	c.service.Close()
	c.conn.Close()
	c.rb.Close()
}
