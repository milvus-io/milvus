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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type getMetricsFuncType func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
type showConfigurationsFuncType func(ctx context.Context, request *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)

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
	getRateMetric(internalpb.RateType_DMLInsert.String())
	getRateMetric(internalpb.RateType_DMLUpsert.String())
	getRateMetric(internalpb.RateType_DMLDelete.String())
	getRateMetric(internalpb.RateType_DQLSearch.String())
	getRateMetric(internalpb.RateType_DQLQuery.String())
	getRateMetric(metricsinfo.ReadResultThroughput)
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
	totalMem := hardware.GetMemoryCount()
	usedMem := hardware.GetUsedMemoryCount()
	quotaMetrics, err := getQuotaMetrics()
	if err != nil {
		return nil, err
	}
	hardwareMetrics := metricsinfo.HardwareMetrics{
		IP:           node.session.Address,
		CPUCoreCount: hardware.GetCPUNum(),
		CPUCoreUsage: hardware.GetCPUUsage(),
		Memory:       totalMem,
		MemoryUsage:  usedMem,
		Disk:         hardware.GetDiskCount(),
		DiskUsage:    hardware.GetDiskUsage(),
	}
	quotaMetrics.Hms = hardwareMetrics

	proxyRoleName := metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID())
	proxyMetricInfo := metricsinfo.ProxyInfos{
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

	resp, err := metricsinfo.MarshalComponentInfos(proxyMetricInfo)
	if err != nil {
		return nil, err
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID()),
	}, nil
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

	proxyTopologyNode := metricsinfo.SystemTopologyNode{
		Identifier: int(node.session.ServerID),
		Connected:  make([]metricsinfo.ConnectionEdge, 0),
		Infos: &metricsinfo.ProxyInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				HasError:    false,
				ErrorReason: "",
				Name:        proxyRoleName,
				HardwareInfos: metricsinfo.HardwareMetrics{
					IP:           node.session.Address,
					CPUCoreCount: hardware.GetCPUNum(),
					CPUCoreUsage: hardware.GetCPUUsage(),
					Memory:       hardware.GetMemoryCount(),
					MemoryUsage:  hardware.GetUsedMemoryCount(),
					Disk:         hardware.GetDiskCount(),
					DiskUsage:    hardware.GetDiskUsage(),
				},
				SystemInfo:  metricsinfo.DeployMetrics{},
				CreatedTime: paramtable.GetCreateTime().String(),
				UpdatedTime: paramtable.GetUpdateTime().String(),
				Type:        typeutil.ProxyRole,
				ID:          node.session.ServerID,
			},
			SystemConfigurations: metricsinfo.ProxyConfiguration{
				DefaultPartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(),
				DefaultIndexName:     Params.CommonCfg.DefaultIndexName.GetValue(),
			},
		},
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

		queryCoordResp, queryCoordErr = node.queryCoord.GetMetrics(ctx, request)
		if queryCoordErr != nil {
			return
		}
		queryCoordRoleName = queryCoordResp.GetComponentName()
		queryCoordErr = metricsinfo.UnmarshalTopology(queryCoordResp.GetResponse(), &queryCoordTopology)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		dataCoordResp, dataCoordErr = node.dataCoord.GetMetrics(ctx, request)
		if dataCoordErr != nil {
			return
		}
		dataCoordRoleName = dataCoordResp.GetComponentName()
		dataCoordErr = metricsinfo.UnmarshalTopology(dataCoordResp.GetResponse(), &dataCoordTopology)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		rootCoordResp, rootCoordErr = node.rootCoord.GetMetrics(ctx, request)
		if rootCoordErr != nil {
			return
		}
		rootCoordRoleName = rootCoordResp.GetComponentName()
		rootCoordErr = metricsinfo.UnmarshalTopology(rootCoordResp.GetResponse(), &rootCoordTopology)
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

		// add data nodes to system topology graph
		for _, indexNode := range dataCoordTopology.Cluster.ConnectedIndexNodes {
			node := indexNode
			identifier := int(node.ID)
			identifierMap[indexNode.Name] = identifier
			indexNodeTopologyNode := metricsinfo.SystemTopologyNode{
				Identifier: identifier,
				Connected:  nil,
				Infos:      &node,
			}
			systemTopology.NodesInfo = append(systemTopology.NodesInfo, indexNodeTopologyNode)
			dataCoordTopologyNode.Connected = append(dataCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
				ConnectedIdentifier: identifier,
				Type:                metricsinfo.CoordConnectToNode,
				TargetType:          typeutil.IndexNodeRole,
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
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, paramtable.GetNodeID()),
	}, nil
}
