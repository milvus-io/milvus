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
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"

	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

func TestProxy_metrics(t *testing.T) {
	var err error

	ctx := context.Background()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()

	dc := NewDataCoordMock()
	dc.Start()
	defer dc.Stop()

	ic := NewIndexCoordMock()
	ic.Start()
	defer ic.Stop()

	proxy := &Proxy{
		rootCoord:  rc,
		queryCoord: qc,
		dataCoord:  dc,
		indexCoord: ic,
		session:    &sessionutil.Session{Address: funcutil.GenRandomStr()},
	}

	rc.getMetricsFunc = func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
		id := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

		rootCoordTopology := metricsinfo.RootCoordTopology{
			Self: metricsinfo.RootCoordInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					Name:          metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
					HardwareInfos: metricsinfo.HardwareMetrics{},
					SystemInfo:    metricsinfo.DeployMetrics{},
					Type:          typeutil.RootCoordRole,
					ID:            id,
				},
				SystemConfigurations: metricsinfo.RootCoordConfiguration{},
			},
			Connections: metricsinfo.ConnTopology{
				Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
				ConnectedComponents: []metricsinfo.ConnectionInfo{
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.IndexCoordRole, id),
						TargetType: typeutil.IndexCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
						TargetType: typeutil.QueryCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
						TargetType: typeutil.DataCoordRole,
					},
				},
			},
		}

		resp, _ := metricsinfo.MarshalTopology(rootCoordTopology)

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
		}, nil
	}

	qc.getMetricsFunc = func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
		id := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

		clusterTopology := metricsinfo.QueryClusterTopology{
			Self: metricsinfo.QueryCoordInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					Name:          metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
					HardwareInfos: metricsinfo.HardwareMetrics{},
					SystemInfo:    metricsinfo.DeployMetrics{},
					Type:          typeutil.QueryCoordRole,
					ID:            id,
				},
				SystemConfigurations: metricsinfo.QueryCoordConfiguration{},
			},
			ConnectedNodes: make([]metricsinfo.QueryNodeInfos, 0),
		}

		infos := metricsinfo.QueryNodeInfos{
			BaseComponentInfos:   metricsinfo.BaseComponentInfos{},
			SystemConfigurations: metricsinfo.QueryNodeConfiguration{},
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)

		coordTopology := metricsinfo.QueryCoordTopology{
			Cluster: clusterTopology,
			Connections: metricsinfo.ConnTopology{
				Name: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
				ConnectedComponents: []metricsinfo.ConnectionInfo{
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
						TargetType: typeutil.RootCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
						TargetType: typeutil.DataCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.IndexCoordRole, id),
						TargetType: typeutil.IndexCoordRole,
					},
				},
			},
		}

		resp, _ := metricsinfo.MarshalTopology(coordTopology)

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
		}, nil
	}

	dc.getMetricsFunc = func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
		id := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

		clusterTopology := metricsinfo.DataClusterTopology{
			Self: metricsinfo.DataCoordInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					Name:          metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
					HardwareInfos: metricsinfo.HardwareMetrics{},
					SystemInfo:    metricsinfo.DeployMetrics{},
					Type:          typeutil.DataCoordRole,
					ID:            id,
				},
				SystemConfigurations: metricsinfo.DataCoordConfiguration{},
			},
			ConnectedNodes: make([]metricsinfo.DataNodeInfos, 0),
		}

		infos := metricsinfo.DataNodeInfos{
			BaseComponentInfos:   metricsinfo.BaseComponentInfos{},
			SystemConfigurations: metricsinfo.DataNodeConfiguration{},
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)

		coordTopology := metricsinfo.DataCoordTopology{
			Cluster: clusterTopology,
			Connections: metricsinfo.ConnTopology{
				Name: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
				ConnectedComponents: []metricsinfo.ConnectionInfo{
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
						TargetType: typeutil.RootCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
						TargetType: typeutil.QueryCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.IndexCoordRole, id),
						TargetType: typeutil.IndexCoordRole,
					},
				},
			},
		}

		resp, _ := metricsinfo.MarshalTopology(coordTopology)

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
		}, nil

	}

	ic.getMetricsFunc = func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
		id := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

		clusterTopology := metricsinfo.IndexClusterTopology{
			Self: metricsinfo.IndexCoordInfos{
				BaseComponentInfos: metricsinfo.BaseComponentInfos{
					Name:          metricsinfo.ConstructComponentName(typeutil.IndexCoordRole, id),
					HardwareInfos: metricsinfo.HardwareMetrics{},
					SystemInfo:    metricsinfo.DeployMetrics{},
					Type:          typeutil.IndexCoordRole,
					ID:            id,
				},
				SystemConfigurations: metricsinfo.IndexCoordConfiguration{},
			},
			ConnectedNodes: make([]metricsinfo.IndexNodeInfos, 0),
		}

		infos := metricsinfo.IndexNodeInfos{
			BaseComponentInfos:   metricsinfo.BaseComponentInfos{},
			SystemConfigurations: metricsinfo.IndexNodeConfiguration{},
		}
		clusterTopology.ConnectedNodes = append(clusterTopology.ConnectedNodes, infos)

		coordTopology := metricsinfo.IndexCoordTopology{
			Cluster: clusterTopology,
			Connections: metricsinfo.ConnTopology{
				Name: metricsinfo.ConstructComponentName(typeutil.IndexCoordRole, id),
				ConnectedComponents: []metricsinfo.ConnectionInfo{
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
						TargetType: typeutil.RootCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
						TargetType: typeutil.QueryCoordRole,
					},
					{
						TargetName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
						TargetType: typeutil.DataCoordRole,
					},
				},
			},
		}

		resp, _ := metricsinfo.MarshalTopology(coordTopology)

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexCoordRole, id),
		}, nil

	}

	req, _ := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	resp, err := getSystemInfoMetrics(ctx, req, proxy)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	rc.getMetricsFunc = nil
	qc.getMetricsFunc = nil
	dc.getMetricsFunc = nil
	ic.getMetricsFunc = nil
}
