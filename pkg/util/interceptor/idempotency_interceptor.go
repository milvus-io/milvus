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
// transport, or that exceeds streaming.idempotency.maxKeyLength.
//
// gRPC refuses a metadata value holding any byte outside printable ASCII, and it
// does so before the stream exists, as codes.Internal. ClientBase reads that code
// as a broken connection and answers it by retrying and periodically resetting the
// shared client, so a key the HTTP layer happily accepts -- Go allows every byte
// >= 0x80 in a header value -- turns one request into repeated teardowns of a
// connection every caller on that proxy shares. Refusing the key at the door keeps
// that out of the transport and names it as the parameter error it is.
//
// The length bound is enforced here and at the REST middleware, and nowhere else:
// these are the only two doors a client key enters through, and both sit in front
// of the copy that puts the key on every coordinator RPC of the request, so a check
// any later would be measuring a key that has already crossed process boundaries
// and been retained. The broadcaster deliberately re-checks nothing -- a second
// bound behind these doors could only ever reject a key they had already admitted,
// which for a retry inside its idempotency window means answering it with an error
// instead of the original result.
//
// The bound is refreshable, so lowering it does reject in-window retries at these
// doors. That is a property of the doors, not something a later check could undo.
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
	// ValueFromIncomingContext, not FromIncomingContext: the latter copies the
	// whole incoming metadata -- one map plus one []string per entry -- and this
	// runs on every outgoing unary RPC through
	// IdempotencyKeyPropagationUnaryClientInterceptor, i.e. on the cluster's
	// hottest path, almost always to learn the key is absent. The lookup below
	// allocates only when it matches.
	//
	// util.HeaderIdempotencyKey is already lowercase, which is the form gRPC
	// normalizes metadata keys to, so the exact-key hit carries it; the
	// EqualFold fallback inside covers metadata that was not built through the
	// helpers, which is what the copy-and-lower above used to cover.
	values := metadata.ValueFromIncomingContext(ctx, util.HeaderIdempotencyKey)
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
