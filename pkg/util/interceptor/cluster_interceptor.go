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

	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const ClusterKey = "Cluster"

// ClusterValidationUnaryServerInterceptor returns a new unary server interceptor that
// rejects the request if the client's cluster differs from that of the server.
// It is chiefly employed to tackle the `Cross-Cluster Routing` issue.
func ClusterValidationUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}
		clusters := md.Get(ClusterKey)
		if len(clusters) == 0 {
			return handler(ctx, req)
		}
		cluster := clusters[0]
		if cluster != "" && cluster != paramtable.Get().CommonCfg.ClusterPrefix.GetValue() {
			return nil, merr.WrapErrCrossClusterRouting(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), cluster)
		}
		return handler(ctx, req)
	}
}

// ClusterValidationStreamServerInterceptor returns a new streaming server interceptor that
// rejects the request if the client's cluster differs from that of the server.
// It is chiefly employed to tackle the `Cross-Cluster Routing` issue.
func ClusterValidationStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return handler(srv, ss)
		}
		clusters := md.Get(ClusterKey)
		if len(clusters) == 0 {
			return handler(srv, ss)
		}
		cluster := clusters[0]
		if cluster != "" && cluster != paramtable.Get().CommonCfg.ClusterPrefix.GetValue() {
			return merr.WrapErrCrossClusterRouting(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), cluster)
		}
		return handler(srv, ss)
	}
}

// ClusterInjectionUnaryClientInterceptor returns a new unary client interceptor that injects `cluster` into outgoing context.
func ClusterInjectionUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, ClusterKey, paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ClusterInjectionStreamClientInterceptor returns a new streaming client interceptor that injects `cluster` into outgoing context.
func ClusterInjectionStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, ClusterKey, paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
		return streamer(ctx, desc, cc, method, opts...)
	}
}
