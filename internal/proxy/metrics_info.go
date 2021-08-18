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

package proxy

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

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

	proxyRoleName := metricsinfo.ConstructComponentName(typeutil.ProxyRole, Params.ProxyID)
	identifierMap[proxyRoleName] = getUniqueIntGeneratorIns().get()
	proxyTopologyNode := metricsinfo.SystemTopologyNode{
		Identifier: identifierMap[proxyRoleName],
		Connected:  make([]metricsinfo.ConnectionEdge, 0),
		Infos: &metricsinfo.ProxyInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				HasError:    false,
				ErrorReason: "",
				Name:        proxyRoleName,
			},
		},
	}

	queryCoordResp, queryCoordErr := node.queryCoord.GetMetrics(ctx, request)
	skipQueryCoord := false
	queryCoordRoleName := ""
	if queryCoordErr != nil || queryCoordResp == nil {
		skipQueryCoord = true
	} else {
		queryCoordRoleName = queryCoordResp.ComponentName
		identifierMap[queryCoordRoleName] = getUniqueIntGeneratorIns().get()
	}

	if !skipQueryCoord {
		proxyTopologyNode.Connected = append(proxyTopologyNode.Connected, metricsinfo.ConnectionEdge{
			ConnectedIdentifier: identifierMap[queryCoordRoleName],
			Type:                metricsinfo.Forward,
			TargetType:          typeutil.QueryCoordRole,
		})

		queryCoordTopology := metricsinfo.QueryCoordTopology{}
		err = metricsinfo.UnmarshalTopology(queryCoordResp.Response, &queryCoordTopology)
		if err == nil {
			// query coord in system topology graph
			queryCoordTopologyNode := metricsinfo.SystemTopologyNode{
				Identifier: identifierMap[queryCoordRoleName],
				Connected:  make([]metricsinfo.ConnectionEdge, 0),
				Infos:      &queryCoordTopology.Cluster.Self,
			}

			// add query nodes to system topology graph
			for _, queryNode := range queryCoordTopology.Cluster.ConnectedNodes {
				identifier := getUniqueIntGeneratorIns().get()
				identifierMap[queryNode.Name] = identifier
				queryNodeTopologyNode := metricsinfo.SystemTopologyNode{
					Identifier: identifier,
					Connected:  nil,
					Infos:      &queryNode,
				}
				systemTopology.NodesInfo = append(systemTopology.NodesInfo, queryNodeTopologyNode)
				queryCoordTopologyNode.Connected = append(queryCoordTopologyNode.Connected, metricsinfo.ConnectionEdge{
					ConnectedIdentifier: identifier,
					Type:                metricsinfo.CoordConnectToNode,
					TargetType:          typeutil.QueryNodeRole,
				})
			}

			// add query coord to system topology graph
			systemTopology.NodesInfo = append(systemTopology.NodesInfo, queryCoordTopologyNode)
		}
	}

	// TODO(dragondriver): integrate other coordinator

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
			ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, Params.ProxyID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.ProxyRole, Params.ProxyID),
	}, nil
}
