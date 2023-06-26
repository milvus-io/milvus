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
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"

	grpcdatacoord "github.com/milvus-io/milvus/internal/distributed/datacoord"
	grpcdatacoordclient "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	grpcdatanode "github.com/milvus-io/milvus/internal/distributed/datanode"
	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	grpcproxy "github.com/milvus-io/milvus/internal/distributed/proxy"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	grpcquerycoord "github.com/milvus-io/milvus/internal/distributed/querycoord"
	grpcquerycoordclient "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	grpcquerynode "github.com/milvus-io/milvus/internal/distributed/querynode"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	grpcrootcoord "github.com/milvus-io/milvus/internal/distributed/rootcoord"
	grpcrootcoordclient "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
)

type CrossClusterRoutingSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	factory dependency.Factory
	client  *clientv3.Client

	// clients
	rootCoordClient  *grpcrootcoordclient.Client
	proxyClient      *grpcproxyclient.Client
	dataCoordClient  *grpcdatacoordclient.Client
	queryCoordClient *grpcquerycoordclient.Client
	dataNodeClient   *grpcdatanodeclient.Client
	queryNodeClient  *grpcquerynodeclient.Client
	indexNodeClient  *grpcindexnodeclient.Client

	// servers
	rootCoord  *grpcrootcoord.Server
	proxy      *grpcproxy.Server
	dataCoord  *grpcdatacoord.Server
	queryCoord *grpcquerycoord.Server
	dataNode   *grpcdatanode.Server
	queryNode  *grpcquerynode.Server
	indexNode  *grpcindexnode.Server
}

func (s *CrossClusterRoutingSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*180)
	rand.Seed(time.Now().UnixNano())

	paramtable.Get().Init()
	s.factory = dependency.NewDefaultFactory(true)
}

func (s *CrossClusterRoutingSuite) TearDownSuite() {
}

func (s *CrossClusterRoutingSuite) SetupTest() {
	s.T().Logf("Setup test...")
	var err error

	// setup etcd client
	etcdConfig := &paramtable.Get().EtcdCfg
	s.client, err = etcd.GetEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	s.NoError(err)
	metaRoot := paramtable.Get().EtcdCfg.MetaRootPath.GetValue()

	// setup clients
	s.rootCoordClient, err = grpcrootcoordclient.NewClient(s.ctx, metaRoot, s.client)
	s.NoError(err)
	s.dataCoordClient, err = grpcdatacoordclient.NewClient(s.ctx, metaRoot, s.client)
	s.NoError(err)
	s.queryCoordClient, err = grpcquerycoordclient.NewClient(s.ctx, metaRoot, s.client)
	s.NoError(err)
	s.proxyClient, err = grpcproxyclient.NewClient(s.ctx, paramtable.Get().ProxyGrpcClientCfg.GetInternalAddress())
	s.NoError(err)
	s.dataNodeClient, err = grpcdatanodeclient.NewClient(s.ctx, paramtable.Get().DataNodeGrpcClientCfg.GetAddress())
	s.NoError(err)
	s.queryNodeClient, err = grpcquerynodeclient.NewClient(s.ctx, paramtable.Get().QueryNodeGrpcClientCfg.GetAddress())
	s.NoError(err)
	s.indexNodeClient, err = grpcindexnodeclient.NewClient(s.ctx, paramtable.Get().IndexNodeGrpcClientCfg.GetAddress(), false)
	s.NoError(err)

	// setup servers
	s.rootCoord, err = grpcrootcoord.NewServer(s.ctx, s.factory)
	s.NoError(err)
	err = s.rootCoord.Run()
	s.NoError(err)
	s.T().Logf("rootCoord server successfully started")

	s.dataCoord = grpcdatacoord.NewServer(s.ctx, s.factory)
	s.NotNil(s.dataCoord)
	err = s.dataCoord.Run()
	s.NoError(err)
	s.T().Logf("dataCoord server successfully started")

	s.queryCoord, err = grpcquerycoord.NewServer(s.ctx, s.factory)
	s.NoError(err)
	err = s.queryCoord.Run()
	s.NoError(err)
	s.T().Logf("queryCoord server successfully started")

	s.proxy, err = grpcproxy.NewServer(s.ctx, s.factory)
	s.NoError(err)
	err = s.proxy.Run()
	s.NoError(err)
	s.T().Logf("proxy server successfully started")

	s.dataNode, err = grpcdatanode.NewServer(s.ctx, s.factory)
	s.NoError(err)
	err = s.dataNode.Run()
	s.NoError(err)
	s.T().Logf("dataNode server successfully started")

	s.queryNode, err = grpcquerynode.NewServer(s.ctx, s.factory)
	s.NoError(err)
	err = s.queryNode.Run()
	s.NoError(err)
	s.T().Logf("queryNode server successfully started")

	s.indexNode, err = grpcindexnode.NewServer(s.ctx, s.factory)
	s.NoError(err)
	err = s.indexNode.Run()
	s.NoError(err)
	s.T().Logf("indexNode server successfully started")
}

func (s *CrossClusterRoutingSuite) TearDownTest() {
	err := s.rootCoord.Stop()
	s.NoError(err)
	err = s.proxy.Stop()
	s.NoError(err)
	err = s.dataCoord.Stop()
	s.NoError(err)
	err = s.queryCoord.Stop()
	s.NoError(err)
	err = s.dataNode.Stop()
	s.NoError(err)
	err = s.queryNode.Stop()
	s.NoError(err)
	err = s.indexNode.Stop()
	s.NoError(err)
	s.cancel()
}

func (s *CrossClusterRoutingSuite) TestCrossClusterRoutingSuite() {
	const (
		waitFor  = time.Second * 10
		duration = time.Millisecond * 10
	)

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				err := paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, fmt.Sprintf("%d", rand.Int()))
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	// test rootCoord
	s.Eventually(func() bool {
		resp, err := s.rootCoordClient.ShowCollections(s.ctx, &milvuspb.ShowCollectionsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			),
			DbName: "fake_db_name",
		})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test dataCoord
	s.Eventually(func() bool {
		resp, err := s.dataCoordClient.GetRecoveryInfoV2(s.ctx, &datapb.GetRecoveryInfoRequestV2{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test queryCoord
	s.Eventually(func() bool {
		resp, err := s.queryCoordClient.LoadCollection(s.ctx, &querypb.LoadCollectionRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test proxy
	s.Eventually(func() bool {
		resp, err := s.proxyClient.InvalidateCollectionMetaCache(s.ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test dataNode
	s.Eventually(func() bool {
		resp, err := s.dataNodeClient.FlushSegments(s.ctx, &datapb.FlushSegmentsRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test queryNode
	s.Eventually(func() bool {
		resp, err := s.queryNodeClient.Search(s.ctx, &querypb.SearchRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test indexNode
	s.Eventually(func() bool {
		resp, err := s.indexNodeClient.CreateJob(s.ctx, &indexpb.CreateJobRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)
}

func TestCrossClusterRoutingSuite(t *testing.T) {
	suite.Run(t, new(CrossClusterRoutingSuite))
}
