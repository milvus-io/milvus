// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (h *HandlersV2) addFileResource(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*FileResourceReq)
	req := &milvuspb.AddFileResourceRequest{
		Name: httpReq.Name,
		Path: httpReq.Path,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AddFileResource", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AddFileResource(reqCtx, req.(*milvuspb.AddFileResourceRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) removeFileResource(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*FileResourceNameReq)
	req := &milvuspb.RemoveFileResourceRequest{
		Name: httpReq.Name,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RemoveFileResource", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RemoveFileResource(reqCtx, req.(*milvuspb.RemoveFileResourceRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listFileResources(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListFileResourcesRequest{}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListFileResources", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListFileResources(reqCtx, req.(*milvuspb.ListFileResourcesRequest))
	})
	if err == nil {
		resources := make([]gin.H, 0, len(resp.(*milvuspb.ListFileResourcesResponse).GetResources()))
		for _, resource := range resp.(*milvuspb.ListFileResourcesResponse).GetResources() {
			resources = append(resources, gin.H{
				"id":   resource.GetId(),
				"name": resource.GetName(),
				"path": resource.GetPath(),
			})
		}
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: resources,
		})
	}
	return resp, err
}
