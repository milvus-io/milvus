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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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

// CreateReplicateStream is the one RPC that consults the hook by hand, because
// the interceptor that consults it for every other RPC is a unary one and an
// interceptor chain binds to one of gRPC's two call kinds. That hand-written
// call is exactly the kind a refactor can drop without anything failing, so it
// is pinned here.
func TestCreateReplicateStreamConsultsTheHook(t *testing.T) {
	hookutil.InitOnceHook()
	hookutil.SetTestHook(refusingStreamHook{})
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.CreateReplicateStream(fakeReplicateStream{ctx: context.Background()})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"a stream refusal travels as a gRPC status, and must not be the codes.Unknown a client retries")
}

// refusingStreamHook withholds only the replicate stream, so the test cannot
// pass by the RPC failing for some unrelated reason.
type refusingStreamHook struct {
	hookutil.DefaultHook
}

func (refusingStreamHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	if fullMethod == milvuspb.MilvusService_CreateReplicateStream_FullMethodName {
		return ctx, merr.ErrServiceUnimplemented
	}
	return ctx, nil
}
