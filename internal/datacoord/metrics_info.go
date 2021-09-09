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
	"errors"
	"os"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

// getSystemInfoMetrics compose data cluster metrics
func (s *Server) getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more detail metrics

	// get datacoord info
	nodes := s.cluster.GetNodes()
	clusterTopology := metricsinfo.DataClusterTopology{
		Self:           s.getDataCoordMetrics(),
		ConnectedNodes: make([]metricsinfo.DataNodeInfos, 0, len(nodes)),
	}

	// for each data node, fetch metrics info
	log.Debug("datacoord.getSystemInfoMetrics",
		zap.Int("data nodes num", len(nodes)))
	for _, node := range nodes {
		infos, err := s.getDataNodeMetrics(ctx, req, node)
		if err != nil {
			log.Warn("fails to get datanode metrics", zap.Error(err))
			continue
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)
	}

	// compose topolgoy struct
	coordTopology := metricsinfo.DataCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp := &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		Response:      "",
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
	}
	var err error
	resp.Response, err = metricsinfo.MarshalTopology(coordTopology)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// getDataCoordMetrics composes datacoord infos
func (s *Server) getDataCoordMetrics() metricsinfo.DataCoordInfos {
	return metricsinfo.DataCoordInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.NodeID),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:           s.session.Address,
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       metricsinfo.GetMemoryCount(),
				MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo: metricsinfo.DeployMetrics{
				SystemVersion: os.Getenv(metricsinfo.GitCommitEnvKey),
				DeployMode:    os.Getenv(metricsinfo.DeployModeEnvKey),
			},
			// TODO(dragondriver): CreatedTime & UpdatedTime, easy but time-costing
			Type: typeutil.DataCoordRole,
		},
		SystemConfigurations: metricsinfo.DataCoordConfiguration{
			SegmentMaxSize: Params.SegmentMaxSize,
		},
	}
}

// getDataNodeMetrics composes data node infos
// this function will invoke GetMetrics with data node specified in NodeInfo
func (s *Server) getDataNodeMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *NodeInfo) (metricsinfo.DataNodeInfos, error) {
	infos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HasError: true,
		},
	}
	if node == nil {
		return infos, errors.New("datanode is nil")
	}

	if node.GetClient() == nil {
		return infos, errors.New("datanode client is nil")
	}

	metrics, err := node.GetClient().GetMetrics(ctx, req)
	if err != nil {
		log.Warn("invalid metrics of data node was found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		// err handled, returns nil
		return infos, nil
	}
	infos.BaseComponentInfos.Name = metrics.GetComponentName()

	if metrics.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("invalid metrics of data node was found",
			zap.Any("error_code", metrics.Status.ErrorCode),
			zap.Any("error_reason", metrics.Status.Reason))
		infos.BaseComponentInfos.ErrorReason = metrics.GetStatus().GetReason()
		return infos, nil
	}

	err = metricsinfo.UnmarshalComponentInfos(metrics.GetResponse(), &infos)
	if err != nil {
		log.Warn("invalid metrics of data node was found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		return infos, nil
	}
	infos.BaseComponentInfos.HasError = false
	return infos, nil
}
