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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/querynodev2"
)

type MiniClusterMethodsSuite struct {
	MiniClusterSuite
}

func (s *MiniClusterMethodsSuite) TestStartAndStop() {
	// Do nothing
}

func (s *MiniClusterMethodsSuite) TestRemoveDataNode() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	datanode := datanode.NewDataNode(ctx, c.factory)
	datanode.SetEtcdClient(c.EtcdCli)
	// datanode := c.CreateDefaultDataNode()

	err := c.AddDataNode(datanode)
	s.NoError(err)

	s.Equal(2, c.clusterConfig.DataNodeNum)
	s.Equal(2, len(c.DataNodes))

	err = c.RemoveDataNode(datanode)
	s.NoError(err)

	s.Equal(1, c.clusterConfig.DataNodeNum)
	s.Equal(1, len(c.DataNodes))

	// add default node and remove randomly
	err = c.AddDataNode(nil)
	s.NoError(err)

	s.Equal(2, c.clusterConfig.DataNodeNum)
	s.Equal(2, len(c.DataNodes))

	err = c.RemoveDataNode(nil)
	s.NoError(err)

	s.Equal(1, c.clusterConfig.DataNodeNum)
	s.Equal(1, len(c.DataNodes))
}

func (s *MiniClusterMethodsSuite) TestRemoveQueryNode() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	queryNode := querynodev2.NewQueryNode(ctx, c.factory)
	queryNode.SetEtcdClient(c.EtcdCli)
	// queryNode := c.CreateDefaultQueryNode()

	err := c.AddQueryNode(queryNode)
	s.NoError(err)

	s.Equal(2, c.clusterConfig.QueryNodeNum)
	s.Equal(2, len(c.QueryNodes))

	err = c.RemoveQueryNode(queryNode)
	s.NoError(err)

	s.Equal(1, c.clusterConfig.QueryNodeNum)
	s.Equal(1, len(c.QueryNodes))

	// add default node and remove randomly
	err = c.AddQueryNode(nil)
	s.NoError(err)

	s.Equal(2, c.clusterConfig.QueryNodeNum)
	s.Equal(2, len(c.QueryNodes))

	err = c.RemoveQueryNode(nil)
	s.NoError(err)

	s.Equal(1, c.clusterConfig.QueryNodeNum)
	s.Equal(1, len(c.QueryNodes))
}

func (s *MiniClusterMethodsSuite) TestRemoveIndexNode() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	indexNode := indexnode.NewIndexNode(ctx, c.factory)
	indexNode.SetEtcdClient(c.EtcdCli)
	// indexNode := c.CreateDefaultIndexNode()

	err := c.AddIndexNode(indexNode)
	s.NoError(err)

	s.Equal(2, c.clusterConfig.IndexNodeNum)
	s.Equal(2, len(c.IndexNodes))

	err = c.RemoveIndexNode(indexNode)
	s.NoError(err)

	s.Equal(1, c.clusterConfig.IndexNodeNum)
	s.Equal(1, len(c.IndexNodes))

	// add default node and remove randomly
	err = c.AddIndexNode(nil)
	s.NoError(err)

	s.Equal(2, c.clusterConfig.IndexNodeNum)
	s.Equal(2, len(c.IndexNodes))

	err = c.RemoveIndexNode(nil)
	s.NoError(err)

	s.Equal(1, c.clusterConfig.IndexNodeNum)
	s.Equal(1, len(c.IndexNodes))
}

func (s *MiniClusterMethodsSuite) TestUpdateClusterSize() {
	c := s.Cluster

	err := c.UpdateClusterSize(ClusterConfig{
		QueryNodeNum: -1,
		DataNodeNum:  -1,
		IndexNodeNum: -1,
	})
	s.Error(err)

	err = c.UpdateClusterSize(ClusterConfig{
		QueryNodeNum: 2,
		DataNodeNum:  2,
		IndexNodeNum: 2,
	})
	s.NoError(err)

	s.Equal(2, c.clusterConfig.DataNodeNum)
	s.Equal(2, c.clusterConfig.QueryNodeNum)
	s.Equal(2, c.clusterConfig.IndexNodeNum)

	s.Equal(2, len(c.DataNodes))
	s.Equal(2, len(c.QueryNodes))
	s.Equal(2, len(c.IndexNodes))

	err = c.UpdateClusterSize(ClusterConfig{
		DataNodeNum:  3,
		QueryNodeNum: 2,
		IndexNodeNum: 1,
	})
	s.NoError(err)

	s.Equal(3, c.clusterConfig.DataNodeNum)
	s.Equal(2, c.clusterConfig.QueryNodeNum)
	s.Equal(1, c.clusterConfig.IndexNodeNum)

	s.Equal(3, len(c.DataNodes))
	s.Equal(2, len(c.QueryNodes))
	s.Equal(1, len(c.IndexNodes))
}

func TestMiniCluster(t *testing.T) {
	t.Skip("Skip integration test, need to refactor integration test framework")
	suite.Run(t, new(MiniClusterMethodsSuite))
}
