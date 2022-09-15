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

package querycoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

//getComponentConfigurations returns the configurations of queryCoord matching req.Pattern
func getComponentConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) *internalpb.ShowConfigurationsResponse {
	prefix := "querycoord."
	matchedConfig := Params.QueryCoordCfg.Base.GetByPattern(prefix + req.Pattern)
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

// TODO(dragondriver): add more detail metrics
func getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
	qc *QueryCoord) (string, error) {

	clusterTopology := metricsinfo.QueryClusterTopology{
		Self: metricsinfo.QueryCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, Params.QueryCoordCfg.GetNodeID()),
				HardwareInfos: metricsinfo.HardwareMetrics{
					IP:           qc.session.Address,
					CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
					CPUCoreUsage: metricsinfo.GetCPUUsage(),
					Memory:       metricsinfo.GetMemoryCount(),
					MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
					Disk:         metricsinfo.GetDiskCount(),
					DiskUsage:    metricsinfo.GetDiskUsage(),
				},
				SystemInfo:  metricsinfo.DeployMetrics{},
				CreatedTime: Params.QueryCoordCfg.CreatedTime.String(),
				UpdatedTime: Params.QueryCoordCfg.UpdatedTime.String(),
				Type:        typeutil.QueryCoordRole,
				ID:          qc.session.ServerID,
			},
			SystemConfigurations: metricsinfo.QueryCoordConfiguration{
				SearchChannelPrefix:       Params.CommonCfg.QueryCoordSearch,
				SearchResultChannelPrefix: Params.CommonCfg.QueryCoordSearchResult,
			},
		},
		ConnectedNodes: make([]metricsinfo.QueryNodeInfos, 0),
	}
	metricsinfo.FillDeployMetricsWithEnv(&clusterTopology.Self.SystemInfo)

	nodesMetrics := qc.cluster.GetMetrics(ctx, req)
	for _, nodeMetrics := range nodesMetrics {
		if nodeMetrics.err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(nodeMetrics.err))
			clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, metricsinfo.QueryNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: nodeMetrics.err.Error(),
					Name:        metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, nodeMetrics.nodeID),
					ID:          int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				},
			})
			continue
		}

		if nodeMetrics.resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("invalid metrics of query node was found",
				zap.Any("error_code", nodeMetrics.resp.Status.ErrorCode),
				zap.Any("error_reason", nodeMetrics.resp.Status.Reason))
			clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, metricsinfo.QueryNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: nodeMetrics.resp.Status.Reason,
					Name:        nodeMetrics.resp.ComponentName,
					ID:          int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				},
			})
			continue
		}

		infos := metricsinfo.QueryNodeInfos{}
		err := metricsinfo.UnmarshalComponentInfos(nodeMetrics.resp.Response, &infos)
		if err != nil {
			log.Warn("invalid metrics of query node was found",
				zap.Error(err))
			clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, metricsinfo.QueryNodeInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					HasError:    true,
					ErrorReason: err.Error(),
					Name:        nodeMetrics.resp.ComponentName,
					ID:          int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				},
			})
			continue
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)
	}

	coordTopology := metricsinfo.QueryCoordTopology{
		Cluster: clusterTopology,
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, Params.QueryCoordCfg.GetNodeID()),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp, err := metricsinfo.MarshalTopology(coordTopology)
	if err != nil {
		return "", err
	}

	return resp, nil
}
