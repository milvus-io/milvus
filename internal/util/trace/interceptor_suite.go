// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package trace

import (
	"context"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// InterceptorSuite contains client option and server option
type InterceptorSuite struct {
	ClientOpts []grpc.DialOption
	ServerOpts []grpc.ServerOption
}

var (
	filterFunc = func(ctx context.Context, fullMethodName string) bool {
		if fullMethodName == `/milvus.proto.rootcoord.RootCoord/UpdateChannelTimeTick` ||
			fullMethodName == `/milvus.proto.rootcoord.RootCoord/AllocTimestamp` {
			return false
		}
		return true
	}
)

// GetInterceptorOpts returns the Option of gRPC open-tracing
func GetInterceptorOpts() []grpc_opentracing.Option {
	tracer := opentracing.GlobalTracer()
	opts := []grpc_opentracing.Option{
		grpc_opentracing.WithTracer(tracer),
		grpc_opentracing.WithFilterFunc(filterFunc),
	}
	return opts
}
