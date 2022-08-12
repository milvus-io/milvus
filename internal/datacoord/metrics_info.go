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

package datacoord

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
	"go.uber.org/zap"
)

//getComponentConfigurations returns the configurations of dataNode matching req.Pattern
func getComponentConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) *internalpb.ShowConfigurationsResponse {
	prefix := "datacoord."
	matchedConfig := Params.DataCoordCfg.Base.GetByPattern(prefix + req.Pattern)
	configList := make([]*commonpb.KeyValuePair, 0, len(matchedConfig))
	for key, value := range matchedConfig {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}
}

// getSystemInfoMetrics composes data cluster metrics
func (s *Server) getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more detail metrics

	// get datacoord info
	nodes := s.cluster.GetSessions()
	clusterTopology := metricsinfo.DataClusterTopology{
		Self:           s.getDataCoordMetrics(),
		ConnectedNodes: make([]metricsinfo.DataNodeInfos, 0, len(nodes)),
	}

	// for each data node, fetch metrics info
	log.Debug("datacoord.getSystemInfoMetrics",
		zap.Int("DataNodes number", len(nodes)))
	for _, node := range nodes {
		infos, err := s.getDataNodeMetrics(ctx, req, node)
		if err != nil {
			log.Warn("fails to get DataNode metrics", zap.Error(err))
			continue
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)
	}

	// compose topolgoy struct
	coordTopology := metricsinfo.DataCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.DataCoordCfg.GetNodeID()),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp := &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		Response:      "",
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.DataCoordCfg.GetNodeID()),
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
	ret := metricsinfo.DataCoordInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, Params.DataCoordCfg.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:           s.session.Address,
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       metricsinfo.GetMemoryCount(),
				MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: Params.DataCoordCfg.CreatedTime.String(),
			UpdatedTime: Params.DataCoordCfg.UpdatedTime.String(),
			Type:        typeutil.DataCoordRole,
			ID:          s.session.ServerID,
		},
		SystemConfigurations: metricsinfo.DataCoordConfiguration{
			SegmentMaxSize: Params.DataCoordCfg.SegmentMaxSize,
		},
	}

	metricsinfo.FillDeployMetricsWithEnv(&ret.BaseComponentInfos.SystemInfo)

	return ret
}

// getDataNodeMetrics composes DataNode infos
// this function will invoke GetMetrics with DataNode specified in NodeInfo
func (s *Server) getDataNodeMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *Session) (metricsinfo.DataNodeInfos, error) {
	infos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HasError: true,
			ID:       int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		},
	}
	if node == nil {
		return infos, errors.New("DataNode is nil")
	}

	cli, err := node.GetOrCreateClient(ctx)
	if err != nil {
		return infos, err
	}

	metrics, err := cli.GetMetrics(ctx, req)
	if err != nil {
		log.Warn("invalid metrics of DataNode was found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		// err handled, returns nil
		return infos, nil
	}
	infos.BaseComponentInfos.Name = metrics.GetComponentName()

	if metrics.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("invalid metrics of DataNode was found",
			zap.Any("error_code", metrics.Status.ErrorCode),
			zap.Any("error_reason", metrics.Status.Reason))
		infos.BaseComponentInfos.ErrorReason = metrics.GetStatus().GetReason()
		return infos, nil
	}

	err = metricsinfo.UnmarshalComponentInfos(metrics.GetResponse(), &infos)
	if err != nil {
		log.Warn("invalid metrics of DataNode found",
			zap.Error(err))
		infos.BaseComponentInfos.ErrorReason = err.Error()
		return infos, nil
	}
	infos.BaseComponentInfos.HasError = false
	return infos, nil
}
