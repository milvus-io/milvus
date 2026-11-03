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

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
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

var (
	_ Client            = (*client)(nil)
	_ manifest.Committer = Client(nil)
)

// Client calls PKIndexCoordService and implements manifest.Committer: it
// translates the index types to the manifest proto and the merr codes of the
// service to the errors of the Committer contract. It panics when a Manager
// is built over a nil Client: a Manager without a working Committer must not
// start.
//
// Every error that is not fenced or rejected is retryable: a context cancel
// or deadline error, a gRPC status error with code DeadlineExceeded or
// Canceled, and merr.ErrServiceUnavailable, which also covers a connection
// failure and a gRPC transport error that carries no merr code.
type Client interface {
	// CommitFlush applies a flush and returns the complete manifest after it.
	// A retry with the same arguments after a lost response returns the
	// manifest that the first call committed.
	CommitFlush(ctx context.Context, args manifest.FlushArgs) (manifest.View, error)

	// GetRecoveryManifests returns one answer for each ref, in the order of
	// refs. An item that fails has its own Err. The caller sends at most
	// manifest.MaxRecoveryManifests refs in one call.
	GetRecoveryManifests(ctx context.Context, refs []manifest.VChannelRef) ([]manifest.RecoveryManifest, error)

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
		// the receive limit of the mix coordinator client; a later call option wins
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(paramtable.Get().RootCoordGrpcClientCfg.ClientMaxRecvSize.GetAsInt())),
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

func (c *client) CommitFlush(ctx context.Context, args manifest.FlushArgs) (manifest.View, error) {
	req := &pkindexpb.CommitFlushRequest{
		CollectionId:       args.CollectionID,
		Vchannel:           args.VChannel,
		PchannelTerm:       args.PChannelTerm,
		AddSsts:            manifest.SSTRefsOf(args.AddTables),
		CheckpointTimeTick: args.CheckpointTimeTick,
		IndexId:            args.IndexID,
		ResetTimeTick:      args.ResetTimeTick,
	}
	svc, err := c.service.GetService(ctx)
	if err != nil {
		return manifest.View{}, fromServiceErr(toClientErr(err))
	}
	resp, err := svc.CommitFlush(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return manifest.View{}, fromServiceErr(toClientErr(err))
	}
	m := resp.GetManifest()
	if m == nil {
		// A retry gets the same answer: the service broke its contract.
		return manifest.View{}, errors.Wrapf(manifest.ErrRejected, "vchannel %s: the commit returned no manifest", args.VChannel)
	}
	return manifest.ViewOf(m), nil
}

func (c *client) GetRecoveryManifests(ctx context.Context, refs []manifest.VChannelRef) ([]manifest.RecoveryManifest, error) {
	targets := make([]*pkindexpb.ManifestTarget, len(refs))
	for i, ref := range refs {
		targets[i] = &pkindexpb.ManifestTarget{CollectionId: ref.CollectionID, Vchannel: ref.VChannel, IndexId: ref.IndexID}
	}
	svc, err := c.service.GetService(ctx)
	if err != nil {
		return nil, fromServiceErr(toClientErr(err))
	}
	resp, err := svc.GetRecoveryManifests(ctx, &pkindexpb.GetRecoveryManifestsRequest{Targets: targets})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, fromServiceErr(toClientErr(err))
	}
	entries := resp.GetEntries()
	if len(entries) != len(refs) {
		return nil, errors.Wrapf(manifest.ErrRejected, "the manifest service returned %d entries for %d refs", len(entries), len(refs))
	}
	out := make([]manifest.RecoveryManifest, len(entries))
	for i, e := range entries {
		if err := merr.Error(e.GetStatus()); err != nil {
			// A lookup carries no term, so the service never fences it: the
			// fence branch of fromServiceErr matters only for CommitFlush.
			out[i].Err = fromServiceErr(err)
			continue
		}
		if !e.GetFound() {
			continue
		}
		p := e.GetPointer()
		if p.GetVersion() <= 0 || p.GetBodyKey() == "" {
			out[i].Err = errors.Wrapf(manifest.ErrRejected, "vchannel %s: a found manifest with version %d and body key %q",
				refs[i].VChannel, p.GetVersion(), p.GetBodyKey())
			continue
		}
		out[i] = manifest.RecoveryManifest{Found: true, Version: p.GetVersion(), BodyKey: p.GetBodyKey()}
	}
	return out, nil
}

// fromServiceErr translates an error of the manifest service into the errors
// of the Committer contract. The code of a fenced term becomes ErrFenced. Any
// other merr code that is not retryable becomes ErrRejected: the service
// rejected the request for good (an invalid request, a stale term, a
// corrupted manifest, a failed disk of the coordinator). A retryable merr
// code, a context cancel or deadline, and any other error stay as they are:
// the caller retries them. The cause stays in the chain. The rule follows the
// retriable bit of merr, so a code that the coordinator adds later is
// classified without a change here; a code that merr marks permanent by
// mistake ends the WAL instance, which is the safer failure.
func fromServiceErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, merr.ErrChannelNotAvailable):
		return errors.Mark(err, manifest.ErrFenced)
	case merr.IsCanceledOrTimeout(err):
		// A non-retryable merr error that wraps a context error is still a
		// cancel or timeout of this call, not a rejection.
		return err
	case merr.IsMilvusError(err) && !merr.IsRetryableErr(err):
		return errors.Mark(err, manifest.ErrRejected)
	default:
		return err
	}
}

// toClientErr returns err unchanged when it is nil, a merr error, a context
// cancel or deadline error, or a gRPC status error with code DeadlineExceeded
// or Canceled. Any other error is a connection or transport failure; it
// becomes a retryable merr.ErrServiceUnavailable that keeps the cause.
func toClientErr(err error) error {
	if err == nil || merr.IsMilvusError(err) || merr.IsCanceledOrTimeout(err) {
		return err
	}
	if code := status.Code(err); code == codes.DeadlineExceeded || code == codes.Canceled {
		return err
	}
	return merr.WrapErrServiceUnavailableErr(err, "call primary key index coordinator")
}

func (c *client) Close() {
	c.service.Close()
	c.conn.Close()
	c.rb.Close()
}
