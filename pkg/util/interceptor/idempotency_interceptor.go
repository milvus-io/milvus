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

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ValidateIdempotencyKey rejects a client-supplied key that cannot survive the
// transport, or that exceeds the broadcaster's admission bound.
//
// gRPC refuses a metadata value holding any byte outside printable ASCII, and it
// does so before the stream exists, as codes.Internal. ClientBase reads that code
// as a broken connection and answers it by retrying and periodically resetting the
// shared client, so a key the HTTP layer happily accepts -- Go allows every byte
// >= 0x80 in a header value -- turns one request into repeated teardowns of a
// connection every caller on that proxy shares. Refusing the key at the door keeps
// that out of the transport and names it as the parameter error it is.
//
// The length bound is the same one the broadcaster applies. Checking it here too
// is not redundant: the broadcaster sits after the key has already been copied
// onto every coordinator RPC of the request, so only a check at the door can stop
// an oversized key from being carried across process boundaries first.
//
// An absent key is valid: a request without one is simply not idempotent.
func ValidateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	limit := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.GetAsInt()
	if len(key) > limit {
		return merr.WrapErrParameterInvalidMsg(
			"idempotency key length %d exceeds limit %d", len(key), limit)
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] > 0x7E {
			return merr.WrapErrParameterInvalidMsg(
				"idempotency key must be printable ASCII, found byte %#x at offset %d", key[i], i)
		}
	}
	return nil
}

// IdempotencyKeyFromContext returns the client-supplied idempotency key carried
// in the gRPC incoming metadata, or "" when the request carries none.
//
// Read at the RPC entrypoint rather than in a dedicated server interceptor,
// matching how the db name is handled: the entrypoint already owns the incoming
// metadata, so an extra interceptor would only add a hop.
func IdempotencyKeyFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	// util.HeaderIdempotencyKey is already lowercase, which is the form gRPC
	// normalizes metadata keys to; lowering it again here would only hide a future
	// edit to the constant that the write side would not survive either.
	values := md[util.HeaderIdempotencyKey]
	if len(values) < 1 {
		return ""
	}
	return values[0]
}

// IdempotencyKeyPropagationUnaryClientInterceptor returns a new unary client
// interceptor that copies the idempotency key of the incoming request onto the
// outgoing context, so an idempotent API needs no request field of its own to
// carry the key across a component hop.
//
// A request without a key is left untouched: every RPC in the cluster goes
// through this chain, and an empty metadata value is not the same as an absent
// one for the components that read it.
//
// This covers the clients built on grpcclient.ClientBase — the coordinator and
// node clients. The streaming clients maintain their own interceptor chains and
// are deliberately not covered: a coordinator reaches the broadcaster in-process,
// and from there the key travels as the `_ik` message property rather than as
// metadata.
func IdempotencyKeyPropagationUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if key := IdempotencyKeyFromContext(ctx); key != "" {
			// The gRPC ingress has no middleware of its own, so this is where a key
			// that did not come through the REST door is first seen. Fail the call
			// rather than forward it: forwarding is what turns a bad key into a
			// transport-level codes.Internal and a connection reset.
			if err := ValidateIdempotencyKey(key); err != nil {
				return err
			}
			ctx = metadata.AppendToOutgoingContext(ctx, util.HeaderIdempotencyKey, key)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
