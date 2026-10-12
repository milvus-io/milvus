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

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// fakeService is a lazygrpc.Service that returns a fixed service.
type fakeService struct {
	lazygrpc.Conn
	svc pkindexpb.PKIndexCoordServiceClient
}

func (s *fakeService) GetService(context.Context) (pkindexpb.PKIndexCoordServiceClient, error) {
	return s.svc, nil
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
