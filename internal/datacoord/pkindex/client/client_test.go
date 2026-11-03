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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
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

// fakeCoordClient records the last request and answers what the test sets.
type fakeCoordClient struct {
	pkindexpb.PKIndexCoordServiceClient
	// status answers both calls when set, with the configured manifest and
	// entries when set.
	status *commonpb.Status
	// flushErr and lookupErr fail the corresponding call before the status.
	flushErr  error
	lookupErr error
	flushReq  *pkindexpb.CommitFlushRequest
	lookupReq *pkindexpb.GetRecoveryManifestsRequest
	flushResp *pkindexpb.Manifest
	// lookupResp is the entry list of the response when it is set; the call
	// otherwise answers with no entries.
	lookupResp []*pkindexpb.ManifestPointerEntry
}

func (f *fakeCoordClient) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest, _ ...grpc.CallOption) (*pkindexpb.CommitFlushResponse, error) {
	f.flushReq = req
	if f.flushErr != nil {
		return nil, f.flushErr
	}
	return &pkindexpb.CommitFlushResponse{Status: f.status, Manifest: f.flushResp}, nil
}

func (f *fakeCoordClient) GetRecoveryManifests(ctx context.Context, req *pkindexpb.GetRecoveryManifestsRequest, _ ...grpc.CallOption) (*pkindexpb.GetRecoveryManifestsResponse, error) {
	f.lookupReq = req
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return &pkindexpb.GetRecoveryManifestsResponse{Status: f.status, Entries: f.lookupResp}, nil
}

func newTestClient(f *fakeCoordClient) *client {
	return &client{service: &fakeService{svc: f}}
}

func TestClientSuccess(t *testing.T) {
	c := newTestClient(&fakeCoordClient{flushResp: sampleManifest(), lookupResp: []*pkindexpb.ManifestPointerEntry{
		{Status: merr.Success(), Found: true, Pointer: &pkindexpb.ManifestPointer{Version: 3, BodyKey: "b"}},
	}})
	ctx := context.Background()

	m, err := c.CommitFlush(ctx, manifest.FlushArgs{CollectionID: 7, IndexID: 11, VChannel: "ch_0_7v0"})
	assert.NoError(t, err)
	assert.Equal(t, sampleView(), m)

	got, err := c.GetRecoveryManifests(ctx, []manifest.VChannelRef{{CollectionID: 7, IndexID: 11, VChannel: "ch_0_7v0"}})
	assert.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, got[0].Found)
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

			_, err := c.CommitFlush(ctx, manifest.FlushArgs{})
			assert.ErrorIs(t, err, tc.err)
			assert.Equal(t, errors.Is(tc.err, merr.ErrChannelNotAvailable), errors.Is(err, manifest.ErrFenced))

			_, err = c.GetRecoveryManifests(ctx, []manifest.VChannelRef{{}})
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

func TestClientTransportError(t *testing.T) {
	c := newTestClient(&fakeCoordClient{flushErr: merr.ErrServiceUnavailable})
	_, err := c.CommitFlush(context.Background(), manifest.FlushArgs{})
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

// callAll calls the two methods and returns their errors.
func callAll(c *client) []error {
	ctx := context.Background()
	_, e1 := c.CommitFlush(ctx, manifest.FlushArgs{})
	_, e2 := c.GetRecoveryManifests(ctx, nil)
	return []error{e1, e2}
}

func TestClientRawErrorsBecomeUnavailable(t *testing.T) {
	cases := []struct {
		name string
		c    *client
	}{
		{"grpc unavailable", newTestClient(&fakeCoordClient{flushErr: grpcstatus.Error(codes.Unavailable, "down"), lookupErr: grpcstatus.Error(codes.Unavailable, "down")})},
		{"grpc unimplemented", newTestClient(&fakeCoordClient{flushErr: grpcstatus.Error(codes.Unimplemented, "old server"), lookupErr: grpcstatus.Error(codes.Unimplemented, "old server")})},
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
			newTestClient(&fakeCoordClient{flushErr: ctxErr, lookupErr: ctxErr}),
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
		for _, err := range callAll(newTestClient(&fakeCoordClient{flushErr: grpcErr, lookupErr: grpcErr})) {
			assert.Equal(t, grpcErr, err)
			assert.Equal(t, code, grpcstatus.Code(err))
			assert.False(t, errors.Is(err, merr.ErrServiceUnavailable))
		}
	}
}
