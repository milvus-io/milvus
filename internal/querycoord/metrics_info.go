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

package querycoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// TODO(dragondriver): add more detail metrics
func getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
	qc *QueryCoord,
) (*milvuspb.GetMetricsResponse, error) {

	coordTopology := metricsinfo.CoordTopology{
		Self: metricsinfo.ComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, Params.QueryCoordID),
		},
		ConnectedNodes: make([]metricsinfo.ComponentInfos, 0),
	}

	nodesMetrics := qc.cluster.getMetrics(ctx, req)
	for _, nodeMetrics := range nodesMetrics {
		if nodeMetrics.err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(nodeMetrics.err))
			coordTopology.ConnectedNodes = append(coordTopology.ConnectedNodes, metricsinfo.ComponentInfos{
				HasError:    true,
				ErrorReason: nodeMetrics.err.Error(),
				// Name doesn't matter here cause we can't get it when error occurs, using address as the Name?
				Name: "",
			})
			continue
		}

		if nodeMetrics.resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("invalid metrics of query node was found",
				zap.Any("error_code", nodeMetrics.resp.Status.ErrorCode),
				zap.Any("error_reason", nodeMetrics.resp.Status.Reason))
			coordTopology.ConnectedNodes = append(coordTopology.ConnectedNodes, metricsinfo.ComponentInfos{
				HasError:    true,
				ErrorReason: nodeMetrics.resp.Status.Reason,
				Name:        nodeMetrics.resp.ComponentName,
			})
			continue
		}

		infos := metricsinfo.ComponentInfos{}
		err := infos.Unmarshal(nodeMetrics.resp.Response)
		if err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(err))
			coordTopology.ConnectedNodes = append(coordTopology.ConnectedNodes, metricsinfo.ComponentInfos{
				HasError:    true,
				ErrorReason: err.Error(),
				Name:        nodeMetrics.resp.ComponentName,
			})
			continue
		}
		coordTopology.ConnectedNodes = append(coordTopology.ConnectedNodes, infos)
	}

	resp, err := coordTopology.Marshal()
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response: resp,
	}, nil
}
