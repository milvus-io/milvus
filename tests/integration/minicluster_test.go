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

package integration

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/querynode"
	"github.com/stretchr/testify/assert"
)

func TestMiniClusterStartAndStop(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	err = c.Stop()
	assert.NoError(t, err)
}

func TestAddRemoveDataNode(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	datanode := datanode.NewDataNode(ctx, c.factory)
	datanode.SetEtcdClient(c.etcdCli)
	//datanode := c.CreateDefaultDataNode()

	err = c.AddDataNode(datanode)
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.DataNodeNum)
	assert.Equal(t, 2, len(c.dataNodes))

	err = c.RemoveDataNode(datanode)
	assert.NoError(t, err)

	assert.Equal(t, 1, c.clusterConfig.DataNodeNum)
	assert.Equal(t, 1, len(c.dataNodes))

	// add default node and remove randomly
	err = c.AddDataNode(nil)
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.DataNodeNum)
	assert.Equal(t, 2, len(c.dataNodes))

	err = c.RemoveDataNode(nil)
	assert.NoError(t, err)

	assert.Equal(t, 1, c.clusterConfig.DataNodeNum)
	assert.Equal(t, 1, len(c.dataNodes))
}

func TestAddRemoveQueryNode(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	queryNode := querynode.NewQueryNode(ctx, c.factory)
	queryNode.SetEtcdClient(c.etcdCli)
	//queryNode := c.CreateDefaultQueryNode()

	err = c.AddQueryNode(queryNode)
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.QueryNodeNum)
	assert.Equal(t, 2, len(c.queryNodes))

	err = c.RemoveQueryNode(queryNode)
	assert.NoError(t, err)

	assert.Equal(t, 1, c.clusterConfig.QueryNodeNum)
	assert.Equal(t, 1, len(c.queryNodes))

	// add default node and remove randomly
	err = c.AddQueryNode(nil)
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.QueryNodeNum)
	assert.Equal(t, 2, len(c.queryNodes))

	err = c.RemoveQueryNode(nil)
	assert.NoError(t, err)

	assert.Equal(t, 1, c.clusterConfig.QueryNodeNum)
	assert.Equal(t, 1, len(c.queryNodes))
}

func TestAddRemoveIndexNode(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	indexNode := indexnode.NewIndexNode(ctx, c.factory)
	indexNode.SetEtcdClient(c.etcdCli)
	//indexNode := c.CreateDefaultIndexNode()

	err = c.AddIndexNode(indexNode)
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.IndexNodeNum)
	assert.Equal(t, 2, len(c.indexNodes))

	err = c.RemoveIndexNode(indexNode)
	assert.NoError(t, err)

	assert.Equal(t, 1, c.clusterConfig.IndexNodeNum)
	assert.Equal(t, 1, len(c.indexNodes))

	// add default node and remove randomly
	err = c.AddIndexNode(nil)
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.IndexNodeNum)
	assert.Equal(t, 2, len(c.indexNodes))

	err = c.RemoveIndexNode(nil)
	assert.NoError(t, err)

	assert.Equal(t, 1, c.clusterConfig.IndexNodeNum)
	assert.Equal(t, 1, len(c.indexNodes))
}

func TestUpdateClusterSize(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	err = c.UpdateClusterSize(ClusterConfig{
		QueryNodeNum: -1,
		DataNodeNum:  -1,
		IndexNodeNum: -1,
	})
	assert.Error(t, err)

	err = c.UpdateClusterSize(ClusterConfig{
		QueryNodeNum: 2,
		DataNodeNum:  2,
		IndexNodeNum: 2,
	})
	assert.NoError(t, err)

	assert.Equal(t, 2, c.clusterConfig.DataNodeNum)
	assert.Equal(t, 2, c.clusterConfig.QueryNodeNum)
	assert.Equal(t, 2, c.clusterConfig.IndexNodeNum)

	assert.Equal(t, 2, len(c.dataNodes))
	assert.Equal(t, 2, len(c.queryNodes))
	assert.Equal(t, 2, len(c.indexNodes))

	err = c.UpdateClusterSize(ClusterConfig{
		DataNodeNum:  3,
		QueryNodeNum: 2,
		IndexNodeNum: 1,
	})
	assert.NoError(t, err)

	assert.Equal(t, 3, c.clusterConfig.DataNodeNum)
	assert.Equal(t, 2, c.clusterConfig.QueryNodeNum)
	assert.Equal(t, 1, c.clusterConfig.IndexNodeNum)

	assert.Equal(t, 3, len(c.dataNodes))
	assert.Equal(t, 2, len(c.queryNodes))
	assert.Equal(t, 1, len(c.indexNodes))
}
