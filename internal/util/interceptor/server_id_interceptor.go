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
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const ServerIDKey = "ServerID"

type GetServerIDFunc func() int64

var (
	ErrServerIDMismatch = fmt.Errorf("server ID mismatch")
)

func WrapErrServerIDMismatch(expectedID, actualID int64, msg ...string) error {
	err := errors.Wrapf(ErrServerIDMismatch, "expected=%s, actual=%s", expectedID, actualID)
	if len(msg) > 0 {
		err = errors.Wrap(err, strings.Join(msg, "; "))
	}
	return err
}

// ServerIDValidationUnaryServerInterceptor returns a new unary server interceptor that
// verifies whether the target server ID of request matches with the server's ID and rejects it accordingly.
func ServerIDValidationUnaryServerInterceptor(fn GetServerIDFunc) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}
		values := md.Get(ServerIDKey)
		if len(values) == 0 {
			return handler(ctx, req)
		}
		targetServerID, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return handler(ctx, req)
		}
		actualServerID := fn()
		if targetServerID != actualServerID {
			return nil, WrapErrServerIDMismatch(targetServerID, actualServerID)
		}
		return handler(ctx, req)
	}
}

// ServerIDValidationStreamServerInterceptor returns a new streaming server interceptor that
// verifies whether the target server ID of request matches with the server's ID and rejects it accordingly.
func ServerIDValidationStreamServerInterceptor(fn GetServerIDFunc) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return handler(srv, ss)
		}
		values := md.Get(ServerIDKey)
		if len(values) == 0 {
			return handler(srv, ss)
		}
		targetServerID, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return handler(srv, ss)
		}
		actualServerID := fn()
		if targetServerID != actualServerID {
			return WrapErrServerIDMismatch(targetServerID, actualServerID)
		}
		return handler(srv, ss)
	}
}

// ServerIDInjectionUnaryClientInterceptor returns a new unary client interceptor that
// injects target server ID into the request.
func ServerIDInjectionUnaryClientInterceptor(targetServerID int64) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, ServerIDKey, fmt.Sprint(targetServerID))
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ServerIDInjectionStreamClientInterceptor returns a new streaming client interceptor that
// injects target server ID into the request.
func ServerIDInjectionStreamClientInterceptor(targetServerID int64) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, ServerIDKey, fmt.Sprint(targetServerID))
		return streamer(ctx, desc, cc, method, opts...)
	}
}
