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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// TestValidateIdempotencyKey covers the door the adversarial review on milvus#52544
// found open: Go accepts header bytes >= 0x80, gRPC rejects everything outside
// printable ASCII as codes.Internal, and ClientBase answers that code by resetting
// the connection every caller on the proxy shares.
func TestValidateIdempotencyKey(t *testing.T) {
	paramtable.Init()
	limit := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.GetAsInt()

	t.Run("absent key is valid", func(t *testing.T) {
		assert.NoError(t, ValidateIdempotencyKey(""))
	})

	t.Run("printable ASCII is valid", func(t *testing.T) {
		assert.NoError(t, ValidateIdempotencyKey("run-1/batch_2.a~b c"))
		// The range is inclusive at both ends.
		assert.NoError(t, ValidateIdempotencyKey("\x20\x7e"))
	})

	t.Run("non-ASCII is rejected", func(t *testing.T) {
		// The exact value grpc-go would fail the stream on.
		err := ValidateIdempotencyKey("批次-1")
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "printable ASCII")
	})

	t.Run("control bytes are rejected", func(t *testing.T) {
		// HTAB passes net/textproto but not grpc-go.
		assert.ErrorIs(t, ValidateIdempotencyKey("run\t1"), merr.ErrParameterInvalid)
		assert.ErrorIs(t, ValidateIdempotencyKey("run\n1"), merr.ErrParameterInvalid)
	})

	t.Run("oversized key is rejected at the door", func(t *testing.T) {
		assert.NoError(t, ValidateIdempotencyKey(strings.Repeat("a", limit)))
		err := ValidateIdempotencyKey(strings.Repeat("a", limit+1))
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "exceeds limit")
	})
}

// TestIdempotencyKeyPropagationRejectsInvalidKey pins the gRPC ingress door: a key
// that did not come through the REST middleware must fail the call here rather than
// be forwarded, since forwarding is what produces the transport-level failure.
func TestIdempotencyKeyPropagationRejectsInvalidKey(t *testing.T) {
	paramtable.Init()

	invoked := false
	invoker := func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, opts ...grpc.CallOption,
	) error {
		invoked = true
		return nil
	}

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(util.HeaderIdempotencyKey, "批次-1"))
	err := IdempotencyKeyPropagationUnaryClientInterceptor()(
		ctx, "/milvus.proto.data.DataCoord/ImportV2", nil, nil, nil, invoker)

	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.False(t, invoked, "the call must not reach the transport")
}
