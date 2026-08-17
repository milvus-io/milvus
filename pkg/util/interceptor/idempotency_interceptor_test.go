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

package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util"
)

func TestIdempotencyKeyFromContext(t *testing.T) {
	assert.Equal(t, "", IdempotencyKeyFromContext(context.Background()))

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
	assert.Equal(t, "", IdempotencyKeyFromContext(ctx))

	ctx = metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(util.HeaderIdempotencyKey, "run-1-batch-1"))
	assert.Equal(t, "run-1-batch-1", IdempotencyKeyFromContext(ctx))

	// gRPC normalizes metadata keys to lower case, so a differently-cased header
	// from the client still resolves.
	ctx = metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("Idempotency-Key", "run-1-batch-2"))
	assert.Equal(t, "run-1-batch-2", IdempotencyKeyFromContext(ctx))
}

func TestIdempotencyKeyPropagationUnaryClientInterceptor(t *testing.T) {
	method := "MockMethod"
	req := &milvuspb.InsertRequest{}
	interceptor := IdempotencyKeyPropagationUnaryClientInterceptor()

	invokeWith := func(ctx context.Context) metadata.MD {
		var forwarded metadata.MD
		invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			forwarded, _ = metadata.FromOutgoingContext(ctx)
			return nil
		}
		assert.NoError(t, interceptor(ctx, method, req, nil, nil, invoker))
		return forwarded
	}

	t.Run("key present is forwarded exactly once", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(util.HeaderIdempotencyKey, "run-1-batch-1"))
		md := invokeWith(ctx)
		assert.Equal(t, []string{"run-1-batch-1"}, md.Get(util.HeaderIdempotencyKey))
	})

	t.Run("no key leaves the outgoing metadata without the header", func(t *testing.T) {
		// An empty value is not the same as an absent one downstream, so the
		// header must genuinely not be there.
		md := invokeWith(metadata.NewOutgoingContext(context.Background(),
			metadata.Pairs("existing", "value")))
		assert.Empty(t, md.Get(util.HeaderIdempotencyKey))
		_, ok := md[util.HeaderIdempotencyKey]
		assert.False(t, ok)
		assert.Equal(t, []string{"value"}, md.Get("existing"))
	})

	t.Run("existing outgoing metadata is preserved", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(util.HeaderIdempotencyKey, "run-1-batch-1"))
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("existing", "value"))
		md := invokeWith(ctx)
		assert.Equal(t, []string{"run-1-batch-1"}, md.Get(util.HeaderIdempotencyKey))
		assert.Equal(t, []string{"value"}, md.Get("existing"))
	})
}
