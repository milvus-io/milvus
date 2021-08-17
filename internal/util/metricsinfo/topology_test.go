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

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/stretchr/testify/assert"
)

func TestConstructComponentName(t *testing.T) {
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

func TestComponentInfos_Codec(t *testing.T) {
	infos1 := ComponentInfos{
		Name: ConstructComponentName(typeutil.ProxyRole, 1),
	}
	s, err := infos1.Marshal()
	assert.Equal(t, nil, err)
	var infos2 ComponentInfos
	err = infos2.Unmarshal(s)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1.Name, infos2.Name)
}

func TestCoordTopology_Codec(t *testing.T) {
	topology1 := CoordTopology{
		Self: ComponentInfos{
			Name: ConstructComponentName(typeutil.QueryCoordRole, 1),
		},
		ConnectedNodes: []ComponentInfos{
			{
				Name: ConstructComponentName(typeutil.QueryNodeRole, 1),
			},
			{
				Name: ConstructComponentName(typeutil.QueryNodeRole, 2),
			},
		},
	}
	s, err := topology1.Marshal()
	assert.Equal(t, nil, err)
	var topology2 CoordTopology
	err = topology2.Unmarshal(s)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Self.Name, topology2.Self.Name)
	assert.Equal(t, len(topology1.ConnectedNodes), len(topology2.ConnectedNodes))
	for i := range topology1.ConnectedNodes {
		assert.Equal(t, topology1.ConnectedNodes[i], topology2.ConnectedNodes[i])
	}
}

func TestConnTopology_Codec(t *testing.T) {
	topology1 := ConnTopology{
		Name: ConstructComponentName(typeutil.ProxyRole, 1),
		ConnectedComponents: []string{
			ConstructComponentName(typeutil.RootCoordRole, 1),
			ConstructComponentName(typeutil.QueryCoordRole, 1),
			ConstructComponentName(typeutil.DataCoordRole, 1),
			ConstructComponentName(typeutil.IndexCoordRole, 1),
		},
	}
	s, err := topology1.Marshal()
	assert.Equal(t, nil, err)
	var topology2 ConnTopology
	err = topology2.Unmarshal(s)
	assert.Equal(t, nil, err)
	assert.Equal(t, topology1.Name, topology2.Name)
	assert.Equal(t, len(topology1.ConnectedComponents), len(topology2.ConnectedComponents))
	for i := range topology1.ConnectedComponents {
		assert.Equal(t, topology1.ConnectedComponents[i], topology2.ConnectedComponents[i])
	}
}

func TestSystemTopology_Codec(t *testing.T) {
	topology1 := SystemTopology{}
	s, err := topology1.Marshal()
	assert.Equal(t, nil, err)
	var topology2 SystemTopology
	err = topology2.Unmarshal(s)
	assert.Equal(t, nil, err)
}
