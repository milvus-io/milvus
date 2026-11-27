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

package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/replicate"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeReplicateStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeReplicateStream) Context() context.Context                  { return f.ctx }
func (f fakeReplicateStream) Send(*milvuspb.ReplicateResponse) error    { return nil }
func (f fakeReplicateStream) Recv() (*milvuspb.ReplicateRequest, error) { return nil, nil }

// inspectingStreamHook is a hook that reads its request, as a plug-in built
// against hook.Hook is entitled to: it does not embed DefaultHook, so every
// method here is the one the stream actually calls, and a nil req would be
// caught by the type assertion rather than ignored.
type inspectingStreamHook struct {
	t *testing.T

	refuse error

	mocks   []string
	befores []string
	afters  []string
	results []any
	errs    []error
}

func (h *inspectingStreamHook) Init(map[string]string) error        { return nil }
func (h *inspectingStreamHook) VerifyAPIKey(string) (string, error) { return "", nil }
func (h *inspectingStreamHook) Release()                            {}

func (h *inspectingStreamHook) request(req interface{}) *milvuspb.ReplicateRequest {
	h.t.Helper()
	require.NotNil(h.t, req, "a stream consults the hook with a request, never nil")
	typed, ok := req.(*milvuspb.ReplicateRequest)
	require.True(h.t, ok, "the request is the message type the stream carries, got %T", req)
	return typed
}

func (h *inspectingStreamHook) Mock(_ context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	h.request(req)
	h.mocks = append(h.mocks, fullMethod)
	return false, nil, nil
}

func (h *inspectingStreamHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	h.request(req)
	h.befores = append(h.befores, fullMethod)
	if h.refuse != nil {
		return ctx, h.refuse
	}
	// What a hook puts on the context must reach the stream: the cluster id
	// is what the replicate server reads first - off the incoming metadata,
	// as a client sends it - and the fake stream below deliberately carries
	// none of its own.
	return metadata.NewIncomingContext(ctx, metadata.Pairs("cluster-id", "cluster-from-the-hook")), nil
}

func (h *inspectingStreamHook) After(_ context.Context, result interface{}, err error, fullMethod string) error {
	h.afters = append(h.afters, fullMethod)
	h.results = append(h.results, result)
	h.errs = append(h.errs, err)
	return nil
}

var _ hook.Hook = (*inspectingStreamHook)(nil)

func installStreamHook(t *testing.T, h hook.Hook) {
	t.Helper()
	hookutil.InitOnceHook()
	hookutil.SetTestHook(h)
	t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
}

// CreateReplicateStream is one of the two streams that consult the hook by hand
// - DumpMessages is the other - because the interceptor that consults it for
// every other RPC is a unary one and an interceptor chain binds to one of
// gRPC's two call kinds. It must consult it
// the same way even so: Mock, Before and After in order, a typed non-nil
// request, and the handler under the context Before returned. The stream
// server here is stopped at its first read, so the test is about the seam and
// not about replication.
func TestCreateReplicateStreamConsultsTheHookLikeAUnaryRPC(t *testing.T) {
	h := &inspectingStreamHook{t: t}
	installStreamHook(t, h)
	execute := mockey.Mock((*replicate.ReplicateStreamServer).Execute).Return(nil).Build()
	defer execute.UnPatch()

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.CreateReplicateStream(fakeReplicateStream{ctx: context.Background()})
	require.NoError(t, err,
		"the replicate server reads the cluster id off the stream's context, and only the hook's context carries one")

	want := []string{milvuspb.MilvusService_CreateReplicateStream_FullMethodName}
	assert.Equal(t, want, h.mocks, "Mock is consulted, as for every other RPC")
	assert.Equal(t, want, h.befores)
	assert.Equal(t, want, h.afters, "After sees the stream end, as it sees every other RPC's result")
	assert.Equal(t, []error{nil}, h.errs)
}

// A refusal from Before travels as a gRPC status a client does not retry, and
// After is not consulted for an RPC that never ran - the same shape the unary
// interceptor gives a refusal.
func TestCreateReplicateStreamRefusedByTheHook(t *testing.T) {
	h := &inspectingStreamHook{t: t, refuse: merr.ErrServiceUnimplemented}
	installStreamHook(t, h)

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.CreateReplicateStream(fakeReplicateStream{ctx: context.Background()})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"a stream refusal travels as a gRPC status, and must not be the codes.Unknown a client retries")
	assert.Len(t, h.befores, 1)
	assert.Empty(t, h.afters, "a refused RPC has no result for After to see")
}

// A hook that Mocks the stream ends it with the hook's verdict: there is no
// stream to send a mocked answer down, so what the hook returns as its error
// is what the caller gets, and the stream server never starts.
func TestCreateReplicateStreamMockedByTheHook(t *testing.T) {
	installStreamHook(t, mockingStreamHook{})
	started := false
	execute := mockey.Mock((*replicate.ReplicateStreamServer).Execute).
		To(func(*replicate.ReplicateStreamServer) error { started = true; return nil }).Build()
	defer execute.UnPatch()

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.CreateReplicateStream(fakeReplicateStream{ctx: context.Background()})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.False(t, started, "a mocked stream never reaches the replicate server")
}

// mockingStreamHook answers the replicate stream from Mock, refusing it.
type mockingStreamHook struct {
	hookutil.DefaultHook
}

func (mockingStreamHook) Mock(_ context.Context, _ interface{}, fullMethod string) (bool, interface{}, error) {
	if fullMethod == milvuspb.MilvusService_CreateReplicateStream_FullMethodName {
		return true, nil, merr.ErrServiceUnimplemented
	}
	return false, nil, nil
}

// dumpRequestHook records how DumpMessages consults it, and checks that the
// request it is shown is the dump request itself: this stream, unlike the
// replicate stream, carries a request message of its own.
type dumpRequestHook struct {
	t      *testing.T
	want   *milvuspb.DumpMessagesRequest
	refuse error

	calls []string
	errs  []error
}

type dumpHookContextKey struct{}

func (h *dumpRequestHook) Init(map[string]string) error        { return nil }
func (h *dumpRequestHook) VerifyAPIKey(string) (string, error) { return "", nil }
func (h *dumpRequestHook) Release()                            {}

func (h *dumpRequestHook) Mock(_ context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	assert.Same(h.t, h.want, req, "a dump consults the hook with its own request")
	h.calls = append(h.calls, "Mock "+fullMethod)
	return false, nil, nil
}

func (h *dumpRequestHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	assert.Same(h.t, h.want, req, "a dump consults the hook with its own request")
	h.calls = append(h.calls, "Before "+fullMethod)
	if h.refuse != nil {
		return ctx, h.refuse
	}
	return context.WithValue(ctx, dumpHookContextKey{}, "from-the-hook"), nil
}

func (h *dumpRequestHook) After(_ context.Context, _ interface{}, err error, fullMethod string) error {
	h.calls = append(h.calls, "After "+fullMethod)
	h.errs = append(h.errs, err)
	return nil
}

var _ hook.Hook = (*dumpRequestHook)(nil)

// DumpMessages is the service's other stream, and it consults the hook the way
// CreateReplicateStream does: Mock, Before and After in order, the dump running
// under the context Before returned, and After seeing how the dump ended. The
// dump itself is stopped at once, so the test is about the seam and not about
// reading a WAL.
func TestDumpMessagesConsultsTheHookLikeAUnaryRPC(t *testing.T) {
	req := &milvuspb.DumpMessagesRequest{}
	h := &dumpRequestHook{t: t, want: req}
	installStreamHook(t, h)
	var seen any
	dump := mockey.Mock((*Proxy).dumpMessages).
		To(func(_ *Proxy, _ *milvuspb.DumpMessagesRequest, stream milvuspb.MilvusService_DumpMessagesServer) error {
			seen = stream.Context().Value(dumpHookContextKey{})
			return merr.WrapErrParameterMissing("pchannel")
		}).Build()
	defer dump.UnPatch()

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(req, &mockDumpMessagesServer{ctx: context.Background()})
	assert.ErrorIs(t, err, merr.ErrParameterMissing, "the dump's own error reaches the caller unchanged")
	method := milvuspb.MilvusService_DumpMessages_FullMethodName
	assert.Equal(t, []string{"Mock " + method, "Before " + method, "After " + method}, h.calls)
	assert.Equal(t, "from-the-hook", seen, "the dump runs under the context Before returned")
	require.Len(t, h.errs, 1)
	assert.ErrorIs(t, h.errs[0], merr.ErrParameterMissing, "After sees how the dump ended")
}

// A hook that refuses the dump from Before stops it before any WAL is read: the
// refusal travels as the non-retried InvalidArgument every other RPC's refusal
// does, and After is not consulted for a dump that never ran.
func TestDumpMessagesRefusedByTheHook(t *testing.T) {
	req := &milvuspb.DumpMessagesRequest{}
	h := &dumpRequestHook{t: t, want: req, refuse: merr.ErrServiceUnimplemented}
	installStreamHook(t, h)
	ran := false
	dump := mockey.Mock((*Proxy).dumpMessages).
		To(func(*Proxy, *milvuspb.DumpMessagesRequest, milvuspb.MilvusService_DumpMessagesServer) error {
			ran = true
			return nil
		}).Build()
	defer dump.UnPatch()

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(req, &mockDumpMessagesServer{ctx: context.Background()})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"a stream refusal travels as a gRPC status, and must not be the codes.Unknown a client retries")
	assert.False(t, ran, "a refused dump never reads the WAL")
	method := milvuspb.MilvusService_DumpMessages_FullMethodName
	assert.Equal(t, []string{"Mock " + method, "Before " + method}, h.calls, "a refused RPC has no result for After to see")
}

// refusingHook refuses every RPC from Before, and records which it was asked
// about.
type refusingHook struct {
	hookutil.DefaultHook
	befores []string
}

func (h *refusingHook) Before(ctx context.Context, _ interface{}, fullMethod string) (context.Context, error) {
	h.befores = append(h.befores, fullMethod)
	return ctx, merr.ErrServiceUnimplemented
}

// A unary RPC milvus-proto adds is covered by the interceptor the moment it
// exists; a stream is consulted by hand, so a stream it adds is one a hook
// would silently never see. This drives every stream the proxy consults the
// hook for against a hook that refuses everything, and fails as soon as the
// service declares a stream the list does not know.
func TestEveryStreamTheServiceDeclaresConsultsTheHook(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	consulted := map[string]func() error{
		"CreateReplicateStream": func() error {
			return node.CreateReplicateStream(fakeReplicateStream{ctx: context.Background()})
		},
		"DumpMessages": func() error {
			return node.DumpMessages(&milvuspb.DumpMessagesRequest{}, &mockDumpMessagesServer{ctx: context.Background()})
		},
	}

	declared := make([]string, 0, len(milvuspb.MilvusService_ServiceDesc.Streams))
	for _, stream := range milvuspb.MilvusService_ServiceDesc.Streams {
		declared = append(declared, stream.StreamName)
	}
	known := make([]string, 0, len(consulted))
	for name := range consulted {
		known = append(known, name)
	}
	assert.ElementsMatch(t, known, declared,
		"every stream the service declares must consult the hook by hand, as the ones listed here do")

	for name, call := range consulted {
		h := &refusingHook{}
		installStreamHook(t, h)
		err := call()
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "%s: the hook's refusal ends the stream", name)
		assert.Equal(t, []string{"/" + milvuspb.MilvusService_ServiceDesc.ServiceName + "/" + name}, h.befores,
			"%s consults Before", name)
	}
}
