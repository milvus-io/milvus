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
	"flag"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var caseTimeout time.Duration

func init() {
	flag.DurationVar(&caseTimeout, "caseTimeout", 10*time.Minute, "timeout duration for single case")
}

// EmbedEtcdSuite contains embed setup & teardown related logic
type EmbedEtcdSuite struct {
	EtcdServer *embed.Etcd
	EtcdDir    string
}

func (s *EmbedEtcdSuite) SetupEmbedEtcd() error {
	server, folder, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		return err
	}

	log.Info("wait for etcd server ready...")
	select {
	case <-server.Server.ReadyNotify():
		s.EtcdServer = server
		s.EtcdDir = folder
		log.Info("etcd server ready")
		return nil
	case <-time.After(30 * time.Second):
		server.Server.Stop() // trigger a shutdown
		log.Fatal("Etcd server took too long to start!")
	}
	return nil
}

func (s *EmbedEtcdSuite) TearDownEmbedEtcd() {
	defer os.RemoveAll(s.EtcdDir)
	if s.EtcdServer != nil {
		log.Info("start to stop etcd server")
		s.EtcdServer.Close()
		select {
		case <-s.EtcdServer.Server.StopNotify():
			log.Info("etcd server stopped")
			return
		case err := <-s.EtcdServer.Err():
			log.Warn("etcd server has crashed", zap.Error(err))
		}
	}
}

type MiniClusterSuite struct {
	suite.Suite
	EmbedEtcdSuite

	Cluster    *MiniClusterV2
	Opt        []OptionV2
	cancelFunc context.CancelFunc
}

func (s *MiniClusterSuite) SetupSuite() {
	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *MiniClusterSuite) TearDownSuite() {
	s.TearDownEmbedEtcd()
}

func (s *MiniClusterSuite) SetupTest() {
	log.SetLevel(zapcore.InfoLevel)
	s.T().Log("Setup test...")
	// setup mini cluster to use embed etcd
	endpoints := etcd.GetEmbedEtcdEndpoints(s.EtcdServer)
	val := strings.Join(endpoints, ",")
	// setup env value to init etcd source
	s.T().Setenv("etcd.endpoints", val)

	s.T().Log("Setup case timeout", caseTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	s.cancelFunc = cancel
	opts := append(s.Opt, func(c *MiniClusterV2) {
		// change config etcd endpoints
		c.params[params.EtcdCfg.Endpoints.Key] = val
	})
	c, err := StartMiniClusterV2(ctx, opts...)
	s.Require().NoError(err)
	s.Cluster = c

	checkWg := sync.WaitGroup{}
	checkWg.Add(1)
	// start mini cluster
	nodeIDCheckReport := func() {
		defer checkWg.Done()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("node id check timeout")
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				s.T().Log("node id report info: ", reportInfo)
				s.Equal(hookutil.OpTypeNodeID, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.NodeIDKey])
				return
			}
		}
	}
	go nodeIDCheckReport()
	s.Require().NoError(s.Cluster.Start())
	checkWg.Wait()
}

func (s *MiniClusterSuite) TearDownTest() {
	resp, err := s.Cluster.Proxy.ShowCollections(context.Background(), &milvuspb.ShowCollectionsRequest{
		Type: milvuspb.ShowType_InMemory,
	})
	if err == nil {
		for idx, collectionName := range resp.GetCollectionNames() {
			if resp.GetInMemoryPercentages()[idx] == 100 || resp.GetQueryServiceAvailable()[idx] {
				status, err := s.Cluster.Proxy.ReleaseCollection(context.Background(), &milvuspb.ReleaseCollectionRequest{
					CollectionName: collectionName,
				})
				err = merr.CheckRPCCall(status, err)
				s.NoError(err)
				collectionID := resp.GetCollectionIds()[idx]
				s.CheckCollectionCacheReleased(collectionID)
			}
		}
	}
	s.T().Log("Tear Down test...")
	defer s.cancelFunc()
	if s.Cluster != nil {
		s.Cluster.Stop()
	}
}
