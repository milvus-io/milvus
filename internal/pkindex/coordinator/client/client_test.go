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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// fakeService is a lazygrpc.Service that returns a fixed service.
type fakeService struct {
	lazygrpc.Conn
	svc pkindexpb.PKIndexCoordServiceClient
	err error
}

func (s *fakeService) GetService(context.Context) (pkindexpb.PKIndexCoordServiceClient, error) {
	return s.svc, s.err
}

// fakeCoordClient returns the configured status with a fixed manifest.
type fakeCoordClient struct {
	pkindexpb.PKIndexCoordServiceClient
	status *commonpb.Status
	err    error
}

func (f *fakeCoordClient) ClaimManifest(context.Context, *pkindexpb.ClaimManifestRequest, ...grpc.CallOption) (*pkindexpb.ClaimManifestResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &pkindexpb.ClaimManifestResponse{Status: f.status, Manifest: &pkindexpb.Manifest{}}, nil
}

func (f *fakeCoordClient) CommitFlush(context.Context, *pkindexpb.CommitFlushRequest, ...grpc.CallOption) (*pkindexpb.CommitFlushResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &pkindexpb.CommitFlushResponse{Status: f.status, Manifest: &pkindexpb.Manifest{}}, nil
}

func (f *fakeCoordClient) GetManifest(context.Context, *pkindexpb.GetManifestRequest, ...grpc.CallOption) (*pkindexpb.GetManifestResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &pkindexpb.GetManifestResponse{Status: f.status, Manifest: &pkindexpb.Manifest{}, Found: true}, nil
}

func newTestClient(f *fakeCoordClient) *client {
	return &client{service: &fakeService{svc: f}}
}

func TestClientSuccess(t *testing.T) {
	c := newTestClient(&fakeCoordClient{status: merr.Success()})
	ctx := context.Background()

	m, err := c.ClaimManifest(ctx, &pkindexpb.ClaimManifestRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, m)

	m, err = c.CommitFlush(ctx, &pkindexpb.CommitFlushRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, m)

	m, found, err := c.GetManifest(ctx, &pkindexpb.GetManifestRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, m)
	assert.True(t, found)
}

func TestClientErrorStatus(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		err  error
	}{
		{"fenced", merr.ErrChannelNotAvailable},
		{"unavailable", merr.ErrServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(&fakeCoordClient{status: merr.Status(tc.err)})

			m, err := c.ClaimManifest(ctx, &pkindexpb.ClaimManifestRequest{})
			assert.ErrorIs(t, err, tc.err)
			assert.Nil(t, m)

			m, err = c.CommitFlush(ctx, &pkindexpb.CommitFlushRequest{})
			assert.ErrorIs(t, err, tc.err)
			assert.Nil(t, m)

			m, found, err := c.GetManifest(ctx, &pkindexpb.GetManifestRequest{})
			assert.ErrorIs(t, err, tc.err)
			assert.Nil(t, m)
			assert.False(t, found)
		})
	}
}

func TestClientTransportError(t *testing.T) {
	c := newTestClient(&fakeCoordClient{err: merr.ErrServiceUnavailable})
	_, err := c.ClaimManifest(context.Background(), &pkindexpb.ClaimManifestRequest{})
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

// callAll calls the three methods and returns their errors.
func callAll(c *client) []error {
	ctx := context.Background()
	_, e1 := c.ClaimManifest(ctx, &pkindexpb.ClaimManifestRequest{})
	_, e2 := c.CommitFlush(ctx, &pkindexpb.CommitFlushRequest{})
	_, _, e3 := c.GetManifest(ctx, &pkindexpb.GetManifestRequest{})
	return []error{e1, e2, e3}
}

func TestClientRawErrorsBecomeUnavailable(t *testing.T) {
	cases := []struct {
		name string
		c    *client
	}{
		{"grpc unavailable", newTestClient(&fakeCoordClient{err: grpcstatus.Error(codes.Unavailable, "down")})},
		{"grpc unimplemented", newTestClient(&fakeCoordClient{err: grpcstatus.Error(codes.Unimplemented, "old server")})},
		{"get service", &client{service: &fakeService{err: errors.New("dial failed")}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range callAll(tc.c) {
				assert.True(t, errors.Is(err, merr.ErrServiceUnavailable))
				assert.True(t, merr.IsRetryableErr(err))
			}
		})
	}
}

func TestClientContextErrorsUnchanged(t *testing.T) {
	for _, ctxErr := range []error{context.DeadlineExceeded, context.Canceled} {
		for _, c := range []*client{
			newTestClient(&fakeCoordClient{err: ctxErr}),
			{service: &fakeService{err: ctxErr}},
		} {
			for _, err := range callAll(c) {
				assert.Equal(t, ctxErr, err)
			}
		}
	}
}

func TestClientGRPCDeadlineAndCancelUnchanged(t *testing.T) {
	for _, code := range []codes.Code{codes.DeadlineExceeded, codes.Canceled} {
		grpcErr := grpcstatus.Error(code, "rpc ended")
		for _, err := range callAll(newTestClient(&fakeCoordClient{err: grpcErr})) {
			assert.Equal(t, grpcErr, err)
			assert.Equal(t, code, grpcstatus.Code(err))
			assert.False(t, errors.Is(err, merr.ErrServiceUnavailable))
		}
	}
}
