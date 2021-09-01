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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

// TODO(dragondriver): add more detail metrics
func (s *Server) getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (*milvuspb.GetMetricsResponse, error) {

	clusterTopology := metricsinfo.DataClusterTopology{
		Self: metricsinfo.DataCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
			},
		},
		ConnectedNodes: make([]metricsinfo.DataNodeInfos, 0),
	}

	nodes := s.cluster.GetNodes()
	for _, node := range nodes {
		metrics, err := node.GetClient().GetMetrics(ctx, req)
		if err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(err))
			clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, metricsinfo.DataNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: err.Error(),
					// Name doesn't matter here cause we can't get it when error occurs, using address as the Name?
					Name: "",
				},
			})
			continue
		}

		if metrics.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("invalid metrics of query node was found",
				zap.Any("error_code", metrics.Status.ErrorCode),
				zap.Any("error_reason", metrics.Status.Reason))
			clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, metricsinfo.DataNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: metrics.Status.Reason,
					Name:        metrics.ComponentName,
				},
			})
			continue
		}

		infos := metricsinfo.DataNodeInfos{}
		err = metricsinfo.UnmarshalComponentInfos(metrics.Response, &infos)
		if err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(err))
			clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, metricsinfo.DataNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: err.Error(),
					Name:        metrics.ComponentName,
				},
			})
			continue
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)
	}

	coordTopology := metricsinfo.DataCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
			// TODO(dragondriver): connection info
		},
	}

	resp, err := metricsinfo.MarshalTopology(coordTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
	}, nil
}
