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

package httpserver

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type compactionTargetRESTRequest struct {
	proto.Message
}

func (req *compactionTargetRESTRequest) UnmarshalJSON(body []byte) error {
	return protojson.Unmarshal(body, req.Message)
}

func (req *compactionTargetRESTRequest) GetDbName() string {
	if getter, ok := req.Message.(interface{ GetDbName() string }); ok {
		return getter.GetDbName()
	}
	return ""
}

func (req *compactionTargetRESTRequest) GetCollectionName() string {
	if getter, ok := req.Message.(interface{ GetCollectionName() string }); ok {
		return getter.GetCollectionName()
	}
	return ""
}

func (h *HandlersV2) registerCompactionTargetRoutes(router gin.IRouter) {
	router.POST(CompactionTargetCategory+CreateAction, timeoutMiddleware(wrapperPost(
		func() any { return &compactionTargetRESTRequest{&milvuspb.CreateCompactionTargetRequest{}} },
		wrapperTraceLog(h.createCompactionTarget),
	)))
	router.POST(CompactionTargetCategory+DescribeAction, timeoutMiddleware(wrapperPost(
		func() any { return &compactionTargetRESTRequest{&milvuspb.DescribeCompactionTargetRequest{}} },
		wrapperTraceLog(h.describeCompactionTarget),
	)))
	router.POST(CompactionTargetCategory+ListAction, timeoutMiddleware(wrapperPost(
		func() any { return &compactionTargetRESTRequest{&milvuspb.ListCompactionTargetsRequest{}} },
		wrapperTraceLog(h.listCompactionTargets),
	)))
	router.POST(CompactionTargetCategory+DropAction, timeoutMiddleware(wrapperPost(
		func() any { return &compactionTargetRESTRequest{&milvuspb.DropCompactionTargetRequest{}} },
		wrapperTraceLog(h.dropCompactionTarget),
	)))
}

func (h *HandlersV2) createCompactionTarget(ctx context.Context, c *gin.Context, request any, _ string) (any, error) {
	req := request.(*compactionTargetRESTRequest).Message.(*milvuspb.CreateCompactionTargetRequest)
	c.Set(ContextRequest, req)
	resp, err := h.wrapperProxy(ctx, c, req, false, false, milvuspb.MilvusService_CreateCompactionTarget_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return h.proxy.CreateCompactionTarget(ctx, req.(*milvuspb.CreateCompactionTargetRequest))
		})
	if err != nil {
		return resp, err
	}
	result := resp.(*milvuspb.CreateCompactionTargetResponse)
	data := gin.H{
		"targetId":  formatRESTInt64(result.GetTargetId(), false),
		"totalRows": formatRESTInt64(result.GetTotalRows(), false),
	}
	if optimize := result.GetOptimize(); optimize != nil {
		data["optimize"] = gin.H{
			"inputTargetSizeBytes":  formatRESTInt64(optimize.GetInputTargetSizeBytes(), false),
			"actualTargetSizeBytes": formatRESTInt64(optimize.GetActualTargetSizeBytes(), false),
		}
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: data})
	return resp, nil
}

func (h *HandlersV2) describeCompactionTarget(ctx context.Context, c *gin.Context, request any, _ string) (any, error) {
	req := request.(*compactionTargetRESTRequest).Message.(*milvuspb.DescribeCompactionTargetRequest)
	c.Set(ContextRequest, req)
	resp, err := h.wrapperProxy(ctx, c, req, false, false, milvuspb.MilvusService_DescribeCompactionTarget_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return h.proxy.DescribeCompactionTarget(ctx, req.(*milvuspb.DescribeCompactionTargetRequest))
		})
	if err != nil {
		return resp, err
	}
	result := resp.(*milvuspb.DescribeCompactionTargetResponse)
	return resp, writeCompactionTargets(c, result.GetTarget())
}

func (h *HandlersV2) listCompactionTargets(ctx context.Context, c *gin.Context, request any, _ string) (any, error) {
	req := request.(*compactionTargetRESTRequest).Message.(*milvuspb.ListCompactionTargetsRequest)
	c.Set(ContextRequest, req)
	resp, err := h.wrapperProxy(ctx, c, req, false, false, milvuspb.MilvusService_ListCompactionTargets_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return h.proxy.ListCompactionTargets(ctx, req.(*milvuspb.ListCompactionTargetsRequest))
		})
	if err != nil {
		return resp, err
	}
	result := resp.(*milvuspb.ListCompactionTargetsResponse)
	targets := make([]json.RawMessage, 0, len(result.GetTargets()))
	for _, target := range result.GetTargets() {
		data, err := marshalCompactionTarget(target)
		if err != nil {
			writeCompactionTargetEncodingError(c, err)
			return resp, err
		}
		targets = append(targets, data)
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: targets})
	return resp, nil
}

func (h *HandlersV2) dropCompactionTarget(ctx context.Context, c *gin.Context, request any, _ string) (any, error) {
	req := request.(*compactionTargetRESTRequest).Message.(*milvuspb.DropCompactionTargetRequest)
	c.Set(ContextRequest, req)
	resp, err := h.wrapperProxy(ctx, c, req, false, false, milvuspb.MilvusService_DropCompactionTarget_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return h.proxy.DropCompactionTarget(ctx, req.(*milvuspb.DropCompactionTargetRequest))
		})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func writeCompactionTargets(c *gin.Context, target *milvuspb.CompactionTargetInfo) error {
	data, err := marshalCompactionTarget(target)
	if err != nil {
		writeCompactionTargetEncodingError(c, err)
		return err
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: data})
	return nil
}

func marshalCompactionTarget(target *milvuspb.CompactionTargetInfo) (json.RawMessage, error) {
	// Protobuf JSON preserves 64-bit IDs and distinguishes absent observations from zero rows.
	return protojson.MarshalOptions{EmitDefaultValues: true}.Marshal(target)
}

func writeCompactionTargetEncodingError(c *gin.Context, err error) {
	failure := merr.Wrap(err, "could not encode compaction target")
	HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(failure), HTTPReturnMessage: failure.Error()})
}
