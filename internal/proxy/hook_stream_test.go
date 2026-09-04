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

// CreateReplicateStream is the one RPC that consults the hook by hand, because
// the interceptor that consults it for every other RPC is a unary one and an
// interceptor chain binds to one of gRPC's two call kinds. It must consult it
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
