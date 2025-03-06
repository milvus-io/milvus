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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
)

func toKVPair(data map[string]string) []*commonpb.KeyValuePair {
	if len(data) == 0 {
		return nil
	}

	var pairs []*commonpb.KeyValuePair
	for k, v := range data {
		pairs = append(pairs, &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		})
	}
	return pairs
}

func toPbRGNodeFilter(nodeFilter *ResourceGroupNodeFilter) *rgpb.ResourceGroupNodeFilter {
	if nodeFilter == nil {
		return nil
	}

	return &rgpb.ResourceGroupNodeFilter{
		NodeLabels: toKVPair(nodeFilter.GetNodeLabels()),
	}
}

func toPbResourceGroupConfig(config *ResourceGroupConfig) *rgpb.ResourceGroupConfig {
	if config == nil {
		return nil
	}

	return &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: config.GetRequests().GetNodeNum(),
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: config.GetLimits().GetNodeNum(),
		},
		TransferFrom: lo.Map(config.GetTransferFrom(), func(t *ResourceGroupTransfer, _ int) *rgpb.ResourceGroupTransfer {
			return &rgpb.ResourceGroupTransfer{
				ResourceGroup: t.GetResourceGroup(),
			}
		}),
		TransferTo: lo.Map(config.GetTransferTo(), func(t *ResourceGroupTransfer, _ int) *rgpb.ResourceGroupTransfer {
			return &rgpb.ResourceGroupTransfer{
				ResourceGroup: t.GetResourceGroup(),
			}
		}),
		NodeFilter: toPbRGNodeFilter(config.GetNodeFilter()),
	}
}

func (h *HandlersV2) createResourceGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*ResourceGroupReq)
	req := &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: httpReq.GetName(),
		Config:        toPbResourceGroupConfig(httpReq.GetConfig()),
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateResourceGroup", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateResourceGroup(reqCtx, req.(*milvuspb.CreateResourceGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropResourceGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*ResourceGroupReq)
	req := &milvuspb.DropResourceGroupRequest{
		ResourceGroup: httpReq.GetName(),
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropResourceGroup", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropResourceGroup(reqCtx, req.(*milvuspb.DropResourceGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listResourceGroups(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListResourceGroupsRequest{}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListResourceGroups", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListResourceGroups(reqCtx, req.(*milvuspb.ListResourceGroupsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListResourceGroupsResponse).GetResourceGroups()))
	}
	return resp, err
}

func (h *HandlersV2) describeResourceGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*ResourceGroupReq)
	req := &milvuspb.DescribeResourceGroupRequest{
		ResourceGroup: httpReq.GetName(),
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeResourceGroup", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeResourceGroup(reqCtx, req.(*milvuspb.DescribeResourceGroupRequest))
	})
	if err == nil {
		response := resp.(*milvuspb.DescribeResourceGroupResponse)
		HTTPReturn(c, http.StatusOK, gin.H{
			"resource_group": response.GetResourceGroup(),
		})
	}
	return resp, err
}

func (h *HandlersV2) updateResourceGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*UpdateResourceGroupReq)
	req := &milvuspb.UpdateResourceGroupsRequest{
		ResourceGroups: lo.MapValues(httpReq.GetResourceGroups(), func(config *ResourceGroupConfig, name string) *rgpb.ResourceGroupConfig {
			return toPbResourceGroupConfig(config)
		}),
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/UpdateResourceGroups", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.UpdateResourceGroups(reqCtx, req.(*milvuspb.UpdateResourceGroupsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) transferReplica(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*TransferReplicaReq)
	req := &milvuspb.TransferReplicaRequest{
		SourceResourceGroup: httpReq.GetSourceRgName(),
		TargetResourceGroup: httpReq.GetTargetRgName(),
		CollectionName:      httpReq.GetCollectionName(),
		NumReplica:          httpReq.GetReplicaNum(),
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/TransferMaster", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.TransferReplica(reqCtx, req.(*milvuspb.TransferReplicaRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}
