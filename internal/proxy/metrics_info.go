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
	"sync"

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

	// FIXME:All proxy metrics are retrieved from RootCoord, while single proxy metrics are obtained from the proxy itself.
	proxyInfo := getProxyMetricInfo(ctx, node, nil)
	proxyTopologyNode := metricsinfo.SystemTopologyNode{
		Identifier: int(node.session.ServerID),
		Connected:  make([]metricsinfo.ConnectionEdge, 0),
		Infos:      proxyInfo,
	}
	metricsinfo.FillDeployMetricsWithEnv(&(proxyTopologyNode.Infos.(*metricsinfo.ProxyInfos).SystemInfo))

	var wg sync.WaitGroup

	var queryCoordResp *milvuspb.GetMetricsResponse
	var queryCoordErr error
	var queryCoordTopology metricsinfo.QueryCoordTopology
	queryCoordRoleName := ""

	var dataCoordResp *milvuspb.GetMetricsResponse
	var dataCoordErr error
	var dataCoordTopology metricsinfo.DataCoordTopology
	dataCoordRoleName := ""

	var rootCoordResp *milvuspb.GetMetricsResponse
	var rootCoordErr error
	var rootCoordTopology metricsinfo.RootCoordTopology
	rootCoordRoleName := ""

	wg.Add(1)
	go func() {
		defer wg.Done()

		queryCoordResp, queryCoordErr = node.mixCoord.GetMetrics(ctx, request)
		if queryCoordErr != nil {
			return
		}
		queryCoordRoleName = queryCoordResp.GetComponentName()
		queryCoordErr = metricsinfo.UnmarshalTopology(queryCoordResp.GetResponse(), &queryCoordTopology)
	}()

	wg.Wait()

	identifierMap[queryCoordRoleName] = int(queryCoordTopology.Cluster.Self.ID)
	identifierMap[dataCoordRoleName] = int(dataCoordTopology.Cluster.Self.ID)
	identifierMap[rootCoordRoleName] = int(rootCoordTopology.Self.ID)

	if queryCoordErr == nil && queryCoordResp != nil {
		proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
			ConnectedIdentifier: identifierMap[queryCoordRoleName],
			Type:                metricsinfo.Forward,
			TargetType:          typeutil.QueryCoordRole,
		})

		// QueryCoord in system topology graph
		queryCoordTopologyNode := metricsinfo.SystemTopologyNode{
			Identifier: identifierMap[queryCoordRoleName],
			Connected:  make([]metricsinfo.ConnectionEdge, 0),
			Infos:      &queryCoordTopology.Cluster.Self,
		}

		// fill connection edge, a little trick here
		for _, edge := range queryCoordTopology.Connections.ConnectedComponents {
			switch edge.TargetType {
			case typeutil.RootCoordRole:
				if rootCoordErr == nil && rootCoordResp != nil {
					queryCoordTopologyNode.Connected = append(queryCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
						ConnectedIdentifier: identifierMap[rootCoordRoleName],
						Type:                metricsinfo.Forward,
						TargetType:          typeutil.RootCoordRole,
					})
				}
			case typeutil.DataCoordRole:
				if dataCoordErr == nil && dataCoordResp != nil {
					queryCoordTopologyNode.Connected = append(queryCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
						ConnectedIdentifier: identifierMap[dataCoordRoleName],
						Type:                metricsinfo.Forward,
						TargetType:          typeutil.DataCoordRole,
					})
				}
			case typeutil.QueryCoordRole:
				queryCoordTopologyNode.Connected = append(queryCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: identifierMap[queryCoordRoleName],
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.QueryCoordRole,
				})
			}
		}

		// add query nodes to system topology graph
		for _, queryNode := range queryCoordTopology.Cluster.ConnectedNodes {
			node := queryNode
			identifier := int(node.ID)
			identifierMap[queryNode.Name] = identifier
			queryNodeTopologyNode := metricsinfo.SystemTopologyNode{
				Identifier: identifier,
				Connected:  nil,
				Infos:      &node,
			}
			systemTopology.NodesInfo = append(systemTopology.NodesInfo, queryNodeTopologyNode)
			queryCoordTopologyNode.Connected = append(queryCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
				ConnectedIdentifier: identifier,
				Type:                metricsinfo.CoordConnectToNode,
				TargetType:          typeutil.QueryNodeRole,
			})
		}

		// add QueryCoord to system topology graph
		systemTopology.NodesInfo = append(systemTopology.NodesInfo, queryCoordTopologyNode)
	}

	if dataCoordErr == nil && dataCoordResp != nil {
		proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
			ConnectedIdentifier: identifierMap[dataCoordRoleName],
			Type:                metricsinfo.Forward,
			TargetType:          typeutil.DataCoordRole,
		})

		// DataCoord in system topology graph
		dataCoordTopologyNode := metricsinfo.SystemTopologyNode{
			Identifier: identifierMap[dataCoordRoleName],
			Connected:  make([]metricsinfo.ConnectionEdge, 0),
			Infos:      &dataCoordTopology.Cluster.Self,
		}

		// fill connection edge, a little trick here
		for _, edge := range dataCoordTopology.Connections.ConnectedComponents {
			switch edge.TargetType {
			case typeutil.RootCoordRole:
				if rootCoordErr == nil && rootCoordResp != nil {
					dataCoordTopologyNode.Connected = append(dataCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
						ConnectedIdentifier: identifierMap[rootCoordRoleName],
						Type:                metricsinfo.Forward,
						TargetType:          typeutil.RootCoordRole,
					})
				}
			case typeutil.DataCoordRole:
				dataCoordTopologyNode.Connected = append(dataCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: identifierMap[dataCoordRoleName],
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.DataCoordRole,
				})
			case typeutil.QueryCoordRole:
				if queryCoordErr == nil && queryCoordResp != nil {
					dataCoordTopologyNode.Connected = append(dataCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
						ConnectedIdentifier: identifierMap[queryCoordRoleName],
						Type:                metricsinfo.Forward,
						TargetType:          typeutil.QueryCoordRole,
					})
				}
			}
		}

		// add data nodes to system topology graph
		for _, dataNode := range dataCoordTopology.Cluster.ConnectedDataNodes {
			node := dataNode
			identifier := int(node.ID)
			identifierMap[dataNode.Name] = identifier
			dataNodeTopologyNode := metricsinfo.SystemTopologyNode{
				Identifier: identifier,
				Connected:  nil,
				Infos:      &node,
			}
			systemTopology.NodesInfo = append(systemTopology.NodesInfo, dataNodeTopologyNode)
			dataCoordTopologyNode.Connected = append(dataCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
				ConnectedIdentifier: identifier,
				Type:                metricsinfo.CoordConnectToNode,
				TargetType:          typeutil.DataNodeRole,
			})
		}

		// add DataCoord to system topology graph
		systemTopology.NodesInfo = append(systemTopology.NodesInfo, dataCoordTopologyNode)
	}

	if rootCoordErr == nil && rootCoordResp != nil {
		proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
			ConnectedIdentifier: identifierMap[rootCoordRoleName],
			Type:                metricsinfo.Forward,
			TargetType:          typeutil.RootCoordRole,
		})

		// root coord in system topology graph
		rootCoordTopologyNode := metricsinfo.SystemTopologyNode{
			Identifier: identifierMap[rootCoordRoleName],
			Connected:  make([]metricsinfo.ConnectionEdge, 0),
			Infos:      &rootCoordTopology.Self,
		}

		// fill connection edge, a little trick here
		for _, edge := range rootCoordTopology.Connections.ConnectedComponents {
			switch edge.TargetType {
			case typeutil.RootCoordRole:
				rootCoordTopologyNode.Connected = append(rootCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: identifierMap[rootCoordRoleName],
					Type:                metricsinfo.Forward,
					TargetType:          typeutil.RootCoordRole,
				})
			case typeutil.DataCoordRole:
				if dataCoordErr == nil && dataCoordResp != nil {
					rootCoordTopologyNode.Connected = append(rootCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
						ConnectedIdentifier: identifierMap[dataCoordRoleName],
						Type:                metricsinfo.Forward,
						TargetType:          typeutil.DataCoordRole,
					})
				}
			case typeutil.QueryCoordRole:
				if queryCoordErr == nil && queryCoordResp != nil {
					rootCoordTopologyNode.Connected = append(rootCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
						ConnectedIdentifier: identifierMap[queryCoordRoleName],
						Type:                metricsinfo.Forward,
						TargetType:          typeutil.QueryCoordRole,
					})
				}
			}
		}

		// add root coord to system topology graph
		systemTopology.NodesInfo = append(systemTopology.NodesInfo, rootCoordTopologyNode)
	}

	// add proxy to system topology graph
	systemTopology.NodesInfo = append(systemTopology.NodesInfo, proxyTopologyNode)

	resp, err := metricsinfo.MarshalTopology(systemTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID()),
	}, nil
}
