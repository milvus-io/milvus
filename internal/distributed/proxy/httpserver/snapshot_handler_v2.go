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
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (h *HandlersV2) createSnapshot(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CreateSnapshotReq)
	req := &milvuspb.CreateSnapshotRequest{
		DbName:                      dbName,
		CollectionName:              httpReq.CollectionName,
		Name:                        httpReq.SnapshotName,
		Description:                 httpReq.Description,
		CompactionProtectionSeconds: httpReq.CompactionProtectionSeconds,
		SkipIndex:                   httpReq.SkipIndex,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateSnapshot", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateSnapshot(reqCtx, req.(*milvuspb.CreateSnapshotRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropSnapshot(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*SnapshotReq)
	req := &milvuspb.DropSnapshotRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Name:           httpReq.SnapshotName,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropSnapshot", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropSnapshot(reqCtx, req.(*milvuspb.DropSnapshotRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listSnapshots(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionNameReq)
	req := &milvuspb.ListSnapshotsRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListSnapshots", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListSnapshots(reqCtx, req.(*milvuspb.ListSnapshotsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListSnapshotsResponse).GetSnapshots()))
	}
	return resp, err
}

func (h *HandlersV2) describeSnapshot(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*SnapshotReq)
	req := &milvuspb.DescribeSnapshotRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Name:           httpReq.SnapshotName,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeSnapshot", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeSnapshot(reqCtx, req.(*milvuspb.DescribeSnapshotRequest))
	})
	if err == nil {
		snapshot := resp.(*milvuspb.DescribeSnapshotResponse)
		allowInt64, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{
				"snapshotName":   snapshot.GetName(),
				"description":    snapshot.GetDescription(),
				"collectionName": snapshot.GetCollectionName(),
				"partitionNames": snapshot.GetPartitionNames(),
				"createTs":       formatRESTInt64(snapshot.GetCreateTs(), allowInt64),
				"s3Location":     snapshot.GetS3Location(),
				"skipIndex":      snapshot.GetSkipIndex(),
			},
		})
	}
	return resp, err
}

func (h *HandlersV2) restoreSnapshot(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RestoreSnapshotReq)
	targetDBName := httpReq.TargetDbName
	if targetDBName == "" {
		targetDBName = c.Request.Header.Get(HTTPHeaderDBName)
		if targetDBName == "" {
			targetDBName = DefaultDbName
		}
	}
	req := &milvuspb.RestoreSnapshotRequest{
		Name:                 httpReq.SnapshotName,
		DbName:               dbName,
		CollectionName:       httpReq.SourceCollectionName,
		TargetDbName:         targetDBName,
		TargetCollectionName: httpReq.TargetCollectionName,
		SkipIndex:            httpReq.SkipIndex,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RestoreSnapshot", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RestoreSnapshot(reqCtx, req.(*milvuspb.RestoreSnapshotRequest))
	})
	if err == nil {
		allowInt64, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{
				"jobId": formatRESTInt64(resp.(*milvuspb.RestoreSnapshotResponse).GetJobId(), allowInt64),
			},
		})
	}
	return resp, err
}

func (h *HandlersV2) pinSnapshotData(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PinSnapshotDataReq)
	req := &milvuspb.PinSnapshotDataRequest{
		Name:           httpReq.SnapshotName,
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		TtlSeconds:     httpReq.TTLSeconds,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/PinSnapshotData", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.PinSnapshotData(reqCtx, req.(*milvuspb.PinSnapshotDataRequest))
	})
	if err == nil {
		allowInt64, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{
				"pinId": formatRESTInt64(resp.(*milvuspb.PinSnapshotDataResponse).GetPinId(), allowInt64),
			},
		})
	}
	return resp, err
}

func (h *HandlersV2) unpinSnapshotData(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*UnpinSnapshotDataReq)
	pinID, err := strconv.ParseInt(httpReq.PinID, 10, 64)
	if err != nil || pinID <= 0 {
		paramErr := merr.WrapErrParameterInvalidMsg("pinId must be a positive decimal int64, got %q", httpReq.PinID)
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(paramErr), HTTPReturnMessage: paramErr.Error()})
		return nil, paramErr
	}
	req := &milvuspb.UnpinSnapshotDataRequest{
		PinId: pinID,
	}
	c.Set(ContextRequest, req)

	resp, err := h.wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/UnpinSnapshotData", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.UnpinSnapshotData(reqCtx, req.(*milvuspb.UnpinSnapshotDataRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}
