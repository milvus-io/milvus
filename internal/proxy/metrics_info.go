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

package proxy

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type (
	getMetricsFuncType         func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	showConfigurationsFuncType func(ctx context.Context, request *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
)

// getQuotaMetrics returns ProxyQuotaMetrics.
func getQuotaMetrics() (*metricsinfo.ProxyQuotaMetrics, error) {
	var err error
	rms := make([]metricsinfo.RateMetric, 0)
	getRateMetric := func(label string) {
		rate, err2 := rateCol.Rate(label, ratelimitutil.DefaultAvgDuration)
		if err2 != nil {
			err = err2
			return
		}
		rms = append(rms, metricsinfo.RateMetric{
			Label: label,
			Rate:  rate,
		})
	}

	getSubLabelRateMetric := func(label string) {
		rates, err2 := rateCol.RateSubLabel(label, ratelimitutil.DefaultAvgDuration)
		if err2 != nil {
			err = err2
			return
		}
		for s, f := range rates {
			rms = append(rms, metricsinfo.RateMetric{
				Label: s,
				Rate:  f,
			})
		}
	}
	getRateMetric(internalpb.RateType_DMLInsert.String())
	getRateMetric(internalpb.RateType_DMLUpsert.String())
	getRateMetric(internalpb.RateType_DMLDelete.String())
	getRateMetric(internalpb.RateType_DQLSearch.String())
	getSubLabelRateMetric(internalpb.RateType_DQLSearch.String())
	getRateMetric(internalpb.RateType_DQLQuery.String())
	getSubLabelRateMetric(internalpb.RateType_DQLQuery.String())
	if err != nil {
		return nil, err
	}
	return &metricsinfo.ProxyQuotaMetrics{
		Hms: metricsinfo.HardwareMetrics{},
		Rms: rms,
	}, nil
}

// getProxyMetrics get metrics of Proxy, not including the topological metrics of Query cluster and Data cluster.
func getProxyMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest, node *Proxy) (*milvuspb.GetMetricsResponse, error) {
	quotaMetrics, err := getQuotaMetrics()
	if err != nil {
		return nil, err
	}
	proxyMetricInfo := getProxyMetricInfo(ctx, node, quotaMetrics)
	proxyRoleName := metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID())

	resp, err := metricsinfo.MarshalComponentInfos(proxyMetricInfo)
	if err != nil {
		return nil, err
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: proxyRoleName,
	}, nil
}

func getProxyMetricInfo(ctx context.Context, node *Proxy, quotaMetrics *metricsinfo.ProxyQuotaMetrics) *metricsinfo.ProxyInfos {
	totalMem := hardware.GetMemoryCount()
	usedMem := hardware.GetUsedMemoryCount()
	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
	}
	hardwareMetrics := metricsinfo.HardwareMetrics{
		IP:               node.session.Address,
		CPUCoreCount:     hardware.GetCPUNum(),
		CPUCoreUsage:     hardware.GetCPUUsage(),
		Memory:           totalMem,
		MemoryUsage:      usedMem,
		Disk:             total,
		DiskUsage:        used,
		IOWaitPercentage: ioWait,
	}

	if quotaMetrics != nil {
		quotaMetrics.Hms = hardwareMetrics
	}

	proxyRoleName := metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID())
	return &metricsinfo.ProxyInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HasError:      false,
			Name:          proxyRoleName,
			HardwareInfos: hardwareMetrics,
			SystemInfo:    metricsinfo.DeployMetrics{},
			CreatedTime:   paramtable.GetCreateTime().String(),
			UpdatedTime:   paramtable.GetUpdateTime().String(),
			Type:          typeutil.ProxyRole,
			ID:            node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.ProxyConfiguration{
			DefaultPartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(),
			DefaultIndexName:     Params.CommonCfg.DefaultIndexName.GetValue(),
		},
		QuotaMetrics: quotaMetrics,
	}
}

// getSystemInfoMetrics returns the system information metrics.
func getSystemInfoMetrics(
	ctx context.Context,
	request *milvuspb.GetMetricsRequest,
	node *Proxy,
) (*milvuspb.GetMetricsResponse, error) {
	var err error
	systemTopology := metricsinfo.SystemTopology{
		NodesInfo: make([]metricsinfo.SystemTopologyNode, 0),
	}

	identifierMap := make(map[string]int)

	proxyRoleName := metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID())
	identifierMap[proxyRoleName] = int(node.session.ServerID)

	proxyInfo := getProxyMetricInfo(ctx, node, nil)
	proxyTopologyNode := metricsinfo.SystemTopologyNode{
		Identifier: int(node.session.ServerID),
		Connected:  make([]metricsinfo.ConnectionEdge, 0),
		Infos:      proxyInfo,
	}
	metricsinfo.FillDeployMetricsWithEnv(&(proxyTopologyNode.Infos.(*metricsinfo.ProxyInfos).SystemInfo))

	mixCoordResp, err := node.mixCoord.GetMetrics(ctx, request)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: proxyRoleName,
		}, nil
	}

	var mixCoordTopology metricsinfo.SystemTopology
	err = metricsinfo.UnmarshalTopology(mixCoordResp.GetResponse(), &mixCoordTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: proxyRoleName,
		}, nil
	}

	for _, node := range mixCoordTopology.NodesInfo {
		if node.Infos != nil {
			switch node.Infos.(type) {
			case *metricsinfo.RootCoordInfos:
				proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: node.Identifier,
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.RootCoordRole,
				})
			case *metricsinfo.DataCoordInfos:
				proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: node.Identifier,
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.DataCoordRole,
				})
			case *metricsinfo.QueryCoordInfos:
				proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: node.Identifier,
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.QueryCoordRole,
				})
			case *metricsinfo.QueryNodeInfos:
				proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: node.Identifier,
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.QueryNodeRole,
				})
			case *metricsinfo.DataNodeInfos:
				proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: node.Identifier,
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.DataNodeRole,
				})
			}
		}
	}

	systemTopology.NodesInfo = append(systemTopology.NodesInfo, proxyTopologyNode)
	systemTopology.NodesInfo = append(systemTopology.NodesInfo, mixCoordTopology.NodesInfo...)

	resp, err := metricsinfo.MarshalTopology(systemTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: proxyRoleName,
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: proxyRoleName,
	}, nil
}
