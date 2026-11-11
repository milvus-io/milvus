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

package grpcquerynode

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// countingCompressor is gzip under its own name, counting the messages it
// compresses. Client and server share the process's registry, so one call
// compresses its request, and its reply unless the server sends it raw.
type countingCompressor struct {
	encoding.Compressor
	compressed atomic.Int64
}

func (c *countingCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	c.compressed.Add(1)
	return c.Compressor.Compress(w)
}

func (c *countingCompressor) Name() string { return "test-counting-gzip" }

var testCompressor = func() *countingCompressor {
	c := &countingCompressor{Compressor: encoding.GetCompressor(gzip.Name)}
	encoding.RegisterCompressor(c)
	return c
}()

// A compressed request gets an uncompressed Materialize reply (its body
// carries its own compression), while other RPCs answer with the request's
// compressor.
func TestMaterializeReplyUncompressed(t *testing.T) {
	mockQN := mocks.NewMockQueryNode(t)
	// Non-empty messages: gRPC does not compress an empty one.
	materialized := &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success(), BodyRawSize: 1}}
	mockQN.EXPECT().Materialize(mock.Anything, mock.Anything).Return(materialized, nil).Maybe()
	mockQN.EXPECT().MaterializeSegments(mock.Anything, mock.Anything).Return(materialized, nil).Maybe()
	mockQN.EXPECT().QuerySegments(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{Status: merr.Success(), AllRetrieveCount: 1}, nil).Maybe()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	querypb.RegisterQueryNodeServer(srv, &Server{querynode: mockQN})
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := querypb.NewQueryNodeClient(conn)
	req := &querypb.QueryRequest{DmlChannels: []string{"ch"}}

	cases := []struct {
		name string
		call func(ctx context.Context, opts ...grpc.CallOption) error
		// compressed is the messages compressed: the request, and the reply
		// unless it is sent raw.
		compressed int64
	}{
		{"Materialize", func(ctx context.Context, opts ...grpc.CallOption) error {
			_, err := client.Materialize(ctx, req, opts...)
			return err
		}, 1},
		{"MaterializeSegments", func(ctx context.Context, opts ...grpc.CallOption) error {
			_, err := client.MaterializeSegments(ctx, req, opts...)
			return err
		}, 1},
		{"QuerySegments", func(ctx context.Context, opts ...grpc.CallOption) error {
			_, err := client.QuerySegments(ctx, req, opts...)
			return err
		}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := testCompressor.compressed.Load()
			require.NoError(t, c.call(context.Background(), grpc.UseCompressor(testCompressor.Name())))
			assert.Equal(t, c.compressed, testCompressor.compressed.Load()-before)
		})
	}
}
