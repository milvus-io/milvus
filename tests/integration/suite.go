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
	"time"

	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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

	s.EtcdServer = server
	s.EtcdDir = folder

	return nil
}

func (s *EmbedEtcdSuite) TearDownEmbedEtcd() {
	if s.EtcdServer != nil {
		s.EtcdServer.Server.Stop()
	}
	if s.EtcdDir != "" {
		os.RemoveAll(s.EtcdDir)
	}
}

type MiniClusterSuite struct {
	suite.Suite
	EmbedEtcdSuite

	Cluster    *MiniClusterV2
	cancelFunc context.CancelFunc
}

func (s *MiniClusterSuite) SetupSuite() {
	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *MiniClusterSuite) TearDownSuite() {
	s.TearDownEmbedEtcd()
}

func (s *MiniClusterSuite) SetupTest() {
	log.SetLevel(zapcore.DebugLevel)
	s.T().Log("Setup test...")
	// setup mini cluster to use embed etcd
	endpoints := etcd.GetEmbedEtcdEndpoints(s.EtcdServer)
	val := strings.Join(endpoints, ",")
	// setup env value to init etcd source
	s.T().Setenv("etcd.endpoints", val)

	params = paramtable.Get()

	s.T().Log("Setup case timeout", caseTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	s.cancelFunc = cancel
	c, err := StartMiniClusterV2(ctx, func(c *MiniClusterV2) {
		// change config etcd endpoints
		c.params[params.EtcdCfg.Endpoints.Key] = val
	})
	s.Require().NoError(err)
	s.Cluster = c

	// start mini cluster
	nodeIDCheckReport := func() {
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
}

func (s *MiniClusterSuite) TearDownTest() {
	resp, err := s.Cluster.Proxy.ShowCollections(context.Background(), &milvuspb.ShowCollectionsRequest{
		Type: milvuspb.ShowType_InMemory,
	})
	if err == nil {
		for idx, collectionName := range resp.GetCollectionNames() {
			if resp.GetInMemoryPercentages()[idx] == 100 || resp.GetQueryServiceAvailable()[idx] {
				s.Cluster.Proxy.ReleaseCollection(context.Background(), &milvuspb.ReleaseCollectionRequest{
					CollectionName: collectionName,
				})
			}
		}
	}
	s.T().Log("Tear Down test...")
	defer s.cancelFunc()
	if s.Cluster != nil {
		s.Cluster.Stop()
	}
}
