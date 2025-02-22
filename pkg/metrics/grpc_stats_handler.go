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

package metrics

import (
	"context"
	"strconv"

	"google.golang.org/grpc/stats"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// milvusStatsKey is context key type.
type milvusStatsKey struct{}

// RPCStats stores the meta and payload size info
// it should be attached to context so that request sizing could be avoided
type RPCStats struct {
	fullMethodName     string
	collectionName     string
	inboundPayloadSize int
	inboundLabel       string
	nodeID             int64
}

func (s *RPCStats) SetCollectionName(collName string) *RPCStats {
	if s == nil {
		return s
	}
	s.collectionName = collName
	return s
}

func (s *RPCStats) SetInboundLabel(label string) *RPCStats {
	if s == nil {
		return s
	}
	s.inboundLabel = label
	return s
}

func (s *RPCStats) SetNodeID(nodeID int64) *RPCStats {
	if s == nil {
		return s
	}
	s.nodeID = nodeID
	return s
}

func attachStats(ctx context.Context, stats *RPCStats) context.Context {
	return context.WithValue(ctx, milvusStatsKey{}, stats)
}

func GetStats(ctx context.Context) *RPCStats {
	stats, ok := ctx.Value(milvusStatsKey{}).(*RPCStats)
	if !ok {
		return nil
	}

	return stats
}

// grpcSizeStatsHandler implementing stats.Handler
// this handler process grpc request & response related metrics logic
type grpcSizeStatsHandler struct {
	outboundMethods typeutil.Set[string]
	targetMethods   typeutil.Set[string]
}

func NewGRPCSizeStatsHandler() *grpcSizeStatsHandler {
	return &grpcSizeStatsHandler{
		targetMethods:   make(typeutil.Set[string]),
		outboundMethods: make(typeutil.Set[string]),
	}
}

func (h *grpcSizeStatsHandler) isTarget(method string) bool {
	return h.targetMethods.Contain(method)
}

func (h *grpcSizeStatsHandler) shouldRecordOutbound(method string) bool {
	return h.outboundMethods.Contain(method)
}

func (h *grpcSizeStatsHandler) WithTargetMethods(methods ...string) *grpcSizeStatsHandler {
	h.targetMethods.Insert(methods...)
	h.outboundMethods.Insert(methods...)
	return h
}

func (h *grpcSizeStatsHandler) WithInboundRecord(methods ...string) *grpcSizeStatsHandler {
	h.targetMethods.Insert(methods...)
	return h
}

// TagConn exists to satisfy gRPC stats.Handler interface.
func (h *grpcSizeStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn exists to satisfy gRPC stats.Handler interface.
func (h *grpcSizeStatsHandler) HandleConn(_ context.Context, _ stats.ConnStats) {}

func (h *grpcSizeStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	// if method is not target, just return origin ctx
	if !h.isTarget(info.FullMethodName) {
		return ctx
	}
	// attach stats
	return attachStats(ctx, &RPCStats{fullMethodName: info.FullMethodName})
}

// HandleRPC implements per-RPC stats instrumentation.
func (h *grpcSizeStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	mstats := GetStats(ctx)
	// if no stats found, do nothing
	if mstats == nil {
		return
	}

	switch rs := rs.(type) {
	case *stats.InPayload:
		// store inbound payload size in stats, collection name could be fetch in service after
		mstats.inboundPayloadSize = rs.Length
	case *stats.OutPayload:
		// all info set
		// set metrics with inbound size and related meta
		nodeIDValue := strconv.FormatInt(mstats.nodeID, 10)
		ProxyReceiveBytes.WithLabelValues(
			nodeIDValue,
			mstats.inboundLabel, mstats.collectionName).Add(float64(mstats.inboundPayloadSize))
		// set outbound payload size metrics for marked methods
		if h.shouldRecordOutbound(mstats.fullMethodName) {
			ProxyReadReqSendBytes.WithLabelValues(nodeIDValue).Add(float64(rs.Length))
		}
	default:
	}
}
