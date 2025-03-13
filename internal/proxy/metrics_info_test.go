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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

func TestProxy_metrics(t *testing.T) {
	var err error

	ctx := context.Background()
	req, _ := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	resp, err := getSystemInfoMetrics(ctx, req, getMockProxyRequestMetrics())
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func getMockProxyRequestMetrics() *Proxy {
	rc := NewRootCoordMock()
	defer rc.Close()

	qc := getQueryCoordClient()
	dc := NewDataCoordMock()
	defer dc.Close()

	proxy := &Proxy{
		rootCoord:  rc,
		queryCoord: qc,
		dataCoord:  dc,
		session:    &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{Address: funcutil.GenRandomStr()}},
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
			Status:        merr.Success(),
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, id),
		}, nil
	}

	getMetricsFunc := func(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
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
				},
			},
		}

		resp, _ := metricsinfo.MarshalTopology(coordTopology)

		return &milvuspb.GetMetricsResponse{
			Status:        merr.Success(),
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, id),
		}, nil
	}
	qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(getMetricsFunc(nil, nil))

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
			ConnectedDataNodes: make([]metricsinfo.DataNodeInfos, 0),
		}

		infos := metricsinfo.DataNodeInfos{
			BaseComponentInfos:   metricsinfo.BaseComponentInfos{},
			SystemConfigurations: metricsinfo.DataNodeConfiguration{},
		}
		clusterTopology.ConnectedDataNodes = append(clusterTopology.ConnectedDataNodes, infos)

		indexNodeInfos := metricsinfo.DataNodeInfos{
			BaseComponentInfos:   metricsinfo.BaseComponentInfos{},
			SystemConfigurations: metricsinfo.DataNodeConfiguration{},
		}
		clusterTopology.ConnectedDataNodes = append(clusterTopology.ConnectedDataNodes, indexNodeInfos)

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
				},
			},
		}

		resp, _ := metricsinfo.MarshalTopology(coordTopology)

		return &milvuspb.GetMetricsResponse{
			Status:        merr.Success(),
			Response:      resp,
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, id),
		}, nil
	}
	return proxy
}
