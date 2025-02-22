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

package metricsinfo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Test_ConstructComponentName(t *testing.T) {
	roleList := []string{
		typeutil.QueryNodeRole,
	}
	idList := []typeutil.UniqueID{
		1,
	}

	for _, role := range roleList {
		for _, id := range idList {
			log.Info("TestConstructComponentName",
				zap.String("ComponentName", ConstructComponentName(role, id)))
		}
	}
}

func TestQueryClusterTopology_Codec(t *testing.T) {
	topology1 := QueryClusterTopology{
		Self: QueryCoordInfos{
			BaseComponentInfos: BaseComponentInfos{
				Name: ConstructComponentName(typeutil.QueryCoordRole, 1),
				ID:   1,
			},
		},
		ConnectedNodes: []QueryNodeInfos{
			{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.QueryNodeRole, 2),
					ID:   2,
				},
			},
			{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.QueryNodeRole, 3),
					ID:   3,
				},
			},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestQueryClusterTopology_Codec",
		zap.String("marshaled_result", s))
	var topology2 QueryClusterTopology
	err = UnmarshalTopology(s, &topology2)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Self, topology2.Self)
	assert.Equal(t, len(topology1.ConnectedNodes), len(topology2.ConnectedNodes))
	for i := range topology1.ConnectedNodes {
		assert.Equal(t, topology1.ConnectedNodes[i], topology2.ConnectedNodes[i])
	}
}

func TestQueryCoordTopology_Codec(t *testing.T) {
	topology1 := QueryCoordTopology{
		Cluster: QueryClusterTopology{
			Self: QueryCoordInfos{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.QueryCoordRole, 1),
					ID:   1,
				},
			},
			ConnectedNodes: []QueryNodeInfos{
				{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.QueryNodeRole, 2),
						ID:   2,
					},
				},
				{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.QueryNodeRole, 3),
						ID:   3,
					},
				},
			},
		},
		Connections: ConnTopology{
			Name: ConstructComponentName(typeutil.QueryCoordRole, 1),
			ConnectedComponents: []ConnectionInfo{
				{
					TargetType: typeutil.RootCoordRole,
				},
			},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestQueryCoordTopology_Codec",
		zap.String("marshaled_result", s))
	var topology2 QueryCoordTopology
	err = UnmarshalTopology(s, &topology2)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Cluster.Self, topology2.Cluster.Self)
	assert.Equal(t, len(topology1.Cluster.ConnectedNodes), len(topology2.Cluster.ConnectedNodes))
	for i := range topology1.Cluster.ConnectedNodes {
		assert.Equal(t, topology1.Cluster.ConnectedNodes[i], topology2.Cluster.ConnectedNodes[i])
	}
	assert.Equal(t, topology1.Connections.Name, topology2.Connections.Name)
	assert.Equal(t, len(topology1.Connections.ConnectedComponents), len(topology1.Connections.ConnectedComponents))
	for i := range topology1.Connections.ConnectedComponents {
		assert.Equal(t, topology1.Connections.ConnectedComponents[i], topology2.Connections.ConnectedComponents[i])
	}
}

func TestDataClusterTopology_Codec(t *testing.T) {
	topology1 := DataClusterTopology{
		Self: DataCoordInfos{
			BaseComponentInfos: BaseComponentInfos{
				Name: ConstructComponentName(typeutil.DataCoordRole, 1),
				ID:   1,
			},
		},
		ConnectedDataNodes: []DataNodeInfos{
			{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.DataNodeRole, 2),
					ID:   2,
				},
			},
			{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.DataNodeRole, 3),
					ID:   3,
				},
			},
		},
		ConnectedIndexNodes: []IndexNodeInfos{
			{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.IndexNodeRole, 4),
					ID:   4,
				},
			},
			{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.IndexNodeRole, 5),
					ID:   5,
				},
			},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestDataClusterTopology_Codec",
		zap.String("marshaled_result", s))
	var topology2 DataClusterTopology
	err = UnmarshalTopology(s, &topology2)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Self, topology2.Self)
	assert.Equal(t, len(topology1.ConnectedDataNodes), len(topology2.ConnectedDataNodes))
	assert.Equal(t, len(topology1.ConnectedIndexNodes), len(topology2.ConnectedIndexNodes))
	for i := range topology1.ConnectedDataNodes {
		assert.Equal(t, topology1.ConnectedDataNodes[i], topology2.ConnectedDataNodes[i])
	}
	for i := range topology1.ConnectedIndexNodes {
		assert.Equal(t, topology1.ConnectedIndexNodes[i], topology2.ConnectedIndexNodes[i])
	}
}

func TestDataCoordTopology_Codec(t *testing.T) {
	topology1 := DataCoordTopology{
		Cluster: DataClusterTopology{
			Self: DataCoordInfos{
				BaseComponentInfos: BaseComponentInfos{
					Name: ConstructComponentName(typeutil.DataCoordRole, 1),
					ID:   1,
				},
			},
			ConnectedDataNodes: []DataNodeInfos{
				{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.DataNodeRole, 2),
						ID:   2,
					},
				},
				{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.DataNodeRole, 3),
						ID:   3,
					},
				},
			},
			ConnectedIndexNodes: []IndexNodeInfos{
				{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.IndexNodeRole, 4),
						ID:   4,
					},
				},
				{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.IndexNodeRole, 5),
						ID:   5,
					},
				},
			},
		},
		Connections: ConnTopology{
			Name: ConstructComponentName(typeutil.DataCoordRole, 1),
			ConnectedComponents: []ConnectionInfo{
				{
					TargetType: typeutil.RootCoordRole,
				},
			},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestDataCoordTopology_Codec",
		zap.String("marshaled_result", s))
	var topology2 DataCoordTopology
	err = UnmarshalTopology(s, &topology2)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Cluster.Self, topology2.Cluster.Self)
	assert.Equal(t, len(topology1.Cluster.ConnectedDataNodes), len(topology2.Cluster.ConnectedDataNodes))
	assert.Equal(t, len(topology1.Cluster.ConnectedIndexNodes), len(topology2.Cluster.ConnectedIndexNodes))
	for i := range topology1.Cluster.ConnectedDataNodes {
		assert.Equal(t, topology1.Cluster.ConnectedDataNodes[i], topology2.Cluster.ConnectedDataNodes[i])
	}
	for i := range topology1.Cluster.ConnectedIndexNodes {
		assert.Equal(t, topology1.Cluster.ConnectedIndexNodes[i], topology2.Cluster.ConnectedIndexNodes[i])
	}
	assert.Equal(t, topology1.Connections.Name, topology2.Connections.Name)
	assert.Equal(t, len(topology1.Connections.ConnectedComponents), len(topology1.Connections.ConnectedComponents))
	for i := range topology1.Connections.ConnectedComponents {
		assert.Equal(t, topology1.Connections.ConnectedComponents[i], topology2.Connections.ConnectedComponents[i])
	}
}

func TestRootCoordTopology_Codec(t *testing.T) {
	topology1 := RootCoordTopology{
		Self: RootCoordInfos{
			BaseComponentInfos: BaseComponentInfos{
				Name: ConstructComponentName(typeutil.RootCoordRole, 1),
				ID:   1,
			},
		},
		Connections: ConnTopology{
			Name:                ConstructComponentName(typeutil.RootCoordRole, 1),
			ConnectedComponents: []ConnectionInfo{},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestRootCoordTopology_Codec",
		zap.String("marshaled_result", s))
	var topology2 RootCoordTopology
	err = UnmarshalTopology(s, &topology2)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Self, topology2.Self)
	assert.Equal(t, topology1.Connections.Name, topology2.Connections.Name)
	assert.Equal(t, len(topology1.Connections.ConnectedComponents), len(topology1.Connections.ConnectedComponents))
	for i := range topology1.Connections.ConnectedComponents {
		assert.Equal(t, topology1.Connections.ConnectedComponents[i], topology2.Connections.ConnectedComponents[i])
	}
}

func TestConnTopology_Codec(t *testing.T) {
	topology1 := ConnTopology{
		Name: ConstructComponentName(typeutil.ProxyRole, 1),
		ConnectedComponents: []ConnectionInfo{
			{
				TargetType: typeutil.DataCoordRole,
			},
			{
				TargetType: typeutil.QueryCoordRole,
			},
			{
				TargetType: typeutil.RootCoordRole,
			},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestConnTopology_Codec",
		zap.String("marshaled_result", s))
	var topology2 ConnTopology
	err = UnmarshalTopology(s, &topology2)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Name, topology2.Name)
	assert.Equal(t, len(topology1.ConnectedComponents), len(topology2.ConnectedComponents))
	for i := range topology1.ConnectedComponents {
		assert.Equal(t, topology1.ConnectedComponents[i], topology2.ConnectedComponents[i])
	}
}

func TestSystemTopology_Codec(t *testing.T) {
	topology1 := SystemTopology{
		NodesInfo: []SystemTopologyNode{
			{
				Identifier: 1,
				Infos: &QueryCoordInfos{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.QueryCoordRole, 1),
						ID:   1,
					},
				},
				Connected: []ConnectionEdge{
					{
						ConnectedIdentifier: 2,
						Type:                CoordConnectToNode,
						TargetType:          typeutil.QueryNodeRole,
					},
				},
			},
			{
				Identifier: 2,
				Infos: &QueryNodeInfos{
					BaseComponentInfos: BaseComponentInfos{
						Name: ConstructComponentName(typeutil.QueryNodeRole, 2),
						ID:   2,
					},
				},
				Connected: []ConnectionEdge{},
			},
		},
	}
	s, err := MarshalTopology(topology1)
	assert.Equal(t, nil, err)
	log.Info("TestSystemTopology_Codec",
		zap.String("marshaled_result", s))
	// no need to test unmarshal
}
