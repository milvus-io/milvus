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

package crossclusterrouting

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
)

type CrossClusterRoutingSuite struct {
	integration.MiniClusterSuite
}

func (s *CrossClusterRoutingSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())

	s.WithMilvusConfig("grpc.client.maxMaxAttempts", "1")
	s.MiniClusterSuite.SetupSuite()
}

func (s *CrossClusterRoutingSuite) TestCrossClusterRouting() {
	randomClusterKey := uuid.New().String()
	// test rootCoord
	address, _ := s.Cluster.DefaultMixCoord().GetAddress(s.Cluster.GetContext())
	conn, err := process.DailGRPClient(s.Cluster.GetContext(), address, randomClusterKey, s.Cluster.DefaultMixCoord().GetNodeID())
	s.NoError(err)
	defer conn.Close()
	mixcoordClient := rootcoordpb.NewRootCoordClient(conn)
	_, err = mixcoordClient.ShowCollections(s.Cluster.GetContext(), &milvuspb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		DbName: "fake_db_name",
	})
	assert.Contains(s.T(), err.Error(), merr.ErrServiceCrossClusterRouting.Error())

	// test proxy
	address, _ = s.Cluster.DefaultProxy().GetAddress(s.Cluster.GetContext())
	conn, err = process.DailGRPClient(s.Cluster.GetContext(), address, randomClusterKey, s.Cluster.DefaultProxy().GetNodeID())
	s.NoError(err)
	defer conn.Close()
	proxyClient := proxypb.NewProxyClient(conn)
	_, err = proxyClient.InvalidateCollectionMetaCache(s.Cluster.GetContext(), &proxypb.InvalidateCollMetaCacheRequest{})
	assert.Contains(s.T(), err.Error(), merr.ErrServiceCrossClusterRouting.Error())

	// test dataNode
	address, _ = s.Cluster.DefaultDataNode().GetAddress(s.Cluster.GetContext())
	conn, err = process.DailGRPClient(s.Cluster.GetContext(), address, randomClusterKey, s.Cluster.DefaultProxy().GetNodeID())
	s.NoError(err)
	defer conn.Close()
	dataClient := datapb.NewDataNodeClient(conn)
	_, err = dataClient.FlushSegments(s.Cluster.GetContext(), &datapb.FlushSegmentsRequest{})
	assert.Contains(s.T(), err.Error(), merr.ErrServiceCrossClusterRouting.Error())

	// test queryNode
	address, _ = s.Cluster.DefaultQueryNode().GetAddress(s.Cluster.GetContext())
	conn, err = process.DailGRPClient(s.Cluster.GetContext(), address, randomClusterKey, s.Cluster.DefaultQueryNode().GetNodeID())
	s.NoError(err)
	defer conn.Close()
	queryClient := querypb.NewQueryNodeClient(conn)
	_, err = queryClient.Search(s.Cluster.GetContext(), &querypb.SearchRequest{})
	assert.Contains(s.T(), err.Error(), merr.ErrServiceCrossClusterRouting.Error())

	// test streamingNode
	address, _ = s.Cluster.DefaultStreamingNode().GetAddress(s.Cluster.GetContext())
	conn, err = process.DailGRPClient(s.Cluster.GetContext(), address, randomClusterKey, s.Cluster.DefaultStreamingNode().GetNodeID())
	s.NoError(err)
	defer conn.Close()
	queryClient = querypb.NewQueryNodeClient(conn)
	_, err = queryClient.Search(s.Cluster.GetContext(), &querypb.SearchRequest{})
	assert.Contains(s.T(), err.Error(), merr.ErrServiceCrossClusterRouting.Error())
}

func TestCrossClusterRoutingSuite(t *testing.T) {
	suite.Run(t, new(CrossClusterRoutingSuite))
}
