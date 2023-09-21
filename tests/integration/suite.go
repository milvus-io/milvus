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
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

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

	Cluster    *MiniCluster
	cancelFunc context.CancelFunc
}

func (s *MiniClusterSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *MiniClusterSuite) TearDownSuite() {
	s.TearDownEmbedEtcd()
}

func (s *MiniClusterSuite) SetupTest() {
	s.T().Log("Setup test...")
	// setup mini cluster to use embed etcd
	endpoints := etcd.GetEmbedEtcdEndpoints(s.EtcdServer)
	val := strings.Join(endpoints, ",")
	// setup env value to init etcd source
	s.T().Setenv("etcd.endpoints", val)

	params = paramtable.Get()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	s.cancelFunc = cancel
	c, err := StartMiniCluster(ctx, func(c *MiniCluster) {
		// change config etcd endpoints
		c.params[params.EtcdCfg.Endpoints.Key] = val
	})
	s.Require().NoError(err)
	s.Cluster = c

	// start mini cluster
	s.Require().NoError(s.Cluster.Start())
}

func (s *MiniClusterSuite) TearDownTest() {
	s.T().Log("Tear Down test...")
	defer s.cancelFunc()
	if s.Cluster != nil {
		s.Cluster.Stop()
	}
}
