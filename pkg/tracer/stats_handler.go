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

package tracer

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/stats"
)

var (
	dynamicServerHandler *dynamicOtelGrpcStatsHandler
	initServerOnce       sync.Once
	dynamicClientHandler *dynamicOtelGrpcStatsHandler
	initClientOnce       sync.Once
)

// dynamicOtelGrpcStatsHandler wraps otelgprc.StatsHandler
// to implement runtime configuration update.
type dynamicOtelGrpcStatsHandler struct {
	handler atomic.Pointer[stats.Handler]
}

func getDynamicServerHandler() *dynamicOtelGrpcStatsHandler {
	initServerOnce.Do(func() {
		statsHandler := otelgrpc.NewServerHandler(
			otelgrpc.WithInterceptorFilter(filterFunc),
			otelgrpc.WithTracerProvider(otel.GetTracerProvider()),
		)

		dynamicServerHandler = &dynamicOtelGrpcStatsHandler{}
		dynamicServerHandler.handler.Store(&statsHandler)
	})

	return dynamicServerHandler
}

func getDynamicClientHandler() *dynamicOtelGrpcStatsHandler {
	initClientOnce.Do(func() {
		statsHandler := otelgrpc.NewClientHandler(
			otelgrpc.WithInterceptorFilter(filterFunc),
			otelgrpc.WithTracerProvider(otel.GetTracerProvider()),
		)

		dynamicClientHandler = &dynamicOtelGrpcStatsHandler{}
		dynamicClientHandler.handler.Store(&statsHandler)
	})

	return dynamicClientHandler
}

// GetDynamicOtelGrpcServerStatsHandler returns the singleton instance of grpc server stats.Handler
func GetDynamicOtelGrpcServerStatsHandler() stats.Handler {
	return getDynamicServerHandler()
}

// GetDynamicOtelGrpcClientStatsHandler returns the singleton instance of grpc client stats.Handler
func GetDynamicOtelGrpcClientStatsHandler() stats.Handler {
	return getDynamicClientHandler()
}

func NotifyTracerProviderUpdated() {
	serverhandler := getDynamicServerHandler()
	statsHandler := otelgrpc.NewServerHandler(
		otelgrpc.WithInterceptorFilter(filterFunc),
		otelgrpc.WithTracerProvider(otel.GetTracerProvider()),
	)

	serverhandler.setHandler(statsHandler)

	clientHandler := getDynamicClientHandler()
	statsHandler = otelgrpc.NewClientHandler(
		otelgrpc.WithInterceptorFilter(filterFunc),
		otelgrpc.WithTracerProvider(otel.GetTracerProvider()),
	)
	clientHandler.setHandler(statsHandler)
}

func (h *dynamicOtelGrpcStatsHandler) getHandler() stats.Handler {
	return *h.handler.Load()
}

func (h *dynamicOtelGrpcStatsHandler) setHandler(handler stats.Handler) {
	h.handler.Store(&handler)
}

// TagRPC can attach some information to the given context.
// The context used for the rest lifetime of the RPC will be derived from
// the returned context.
func (h *dynamicOtelGrpcStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	handler := h.getHandler()
	if handler == nil {
		return ctx
	}

	return handler.TagRPC(ctx, info)
}

// HandleRPC processes the RPC stats.
func (h *dynamicOtelGrpcStatsHandler) HandleRPC(ctx context.Context, stats stats.RPCStats) {
	handler := h.getHandler()
	if handler == nil {
		return
	}

	handler.HandleRPC(ctx, stats)
}

// TagConn can attach some information to the given context.
// The returned context will be used for stats handling.
// For conn stats handling, the context used in HandleConn for this
// connection will be derived from the context returned.
// For RPC stats handling,
//   - On server side, the context used in HandleRPC for all RPCs on this
//
// connection will be derived from the context returned.
//   - On client side, the context is not derived from the context returned.
func (h *dynamicOtelGrpcStatsHandler) TagConn(ctx context.Context, tagInfo *stats.ConnTagInfo) context.Context {
	handler := h.getHandler()
	if handler == nil {
		return ctx
	}

	return handler.TagConn(ctx, tagInfo)
}

// HandleConn processes the Conn stats.
func (h *dynamicOtelGrpcStatsHandler) HandleConn(ctx context.Context, stats stats.ConnStats) {
	handler := h.getHandler()
	if handler == nil {
		return
	}

	handler.HandleConn(ctx, stats)
}
