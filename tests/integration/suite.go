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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

var caseTimeout time.Duration

func init() {
	flag.DurationVar(&caseTimeout, "caseTimeout", 10*time.Minute, "timeout duration for single case")
	streamingutil.SetStreamingServiceEnabled()
}

type MiniClusterSuite struct {
	suite.Suite

	envConfigs map[string]string
	Cluster    *cluster.MiniClusterV3
	cancelFunc context.CancelFunc
	opt        clusterSuiteOption
}

// WorkDir returns the work directory of the cluster.
func (s *MiniClusterSuite) WorkDir() string {
	return os.Getenv(cluster.MilvusWorkDirEnvKey)
}

// WithMilvusConfig sets the environment variable for the given key.
// The key can be got from the paramtable package, such as "common.QuotaConfigPath".
func (s *MiniClusterSuite) WithMilvusConfig(key string, value string) {
	if len(key) == 0 {
		panic("key is empty")
	}
	if s.envConfigs == nil {
		s.envConfigs = make(map[string]string)
	}
	envKey := strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	s.envConfigs[envKey] = value
}

// WithOptions set the options for the suite
// use `WithDropAllCollectionsWhenTestTearDown` to drop all collections when test tear down.
// use `WithoutResetDeploymentWhenTestTearDown` to not reset the default deployment when test tear down.
func (s *MiniClusterSuite) WithOptions(options ...ClusterSuiteOption) {
	for _, opts := range options {
		opts(&s.opt)
	}
}

// SetupSuite initializes the MiniClusterSuite by setting up the environment and starting the cluster.
// After it is called, the cluster is ready for tests.
func (s *MiniClusterSuite) SetupSuite() {
	paramtable.Init()
	s.T().Log("Setup test...")
	s.T().Log("Setup case timeout", caseTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	s.cancelFunc = cancel

	s.Cluster = cluster.NewMiniClusterV3(ctx, cluster.WithExtraEnv(s.envConfigs), cluster.WithWorkDir(s.WorkDir()))
	s.T().Log("Setup test success")
}

func (s *MiniClusterSuite) SetupTest() {
}

func (s *MiniClusterSuite) TearDownTest() {
	if !s.opt.notResetDeploymentWhenTestTearDown {
		s.Cluster.Reset()
	}
	if s.opt.dropAllCollectionsWhenTestTearDown {
		s.DropAllCollections()
	}
}

func (s *MiniClusterSuite) TearDownSuite() {
	resp, err := s.Cluster.MilvusClient.ShowCollections(context.Background(), &milvuspb.ShowCollectionsRequest{
		Type: milvuspb.ShowType_InMemory,
	})
	if err == nil {
		wg := sync.WaitGroup{}
		for idx, collectionName := range resp.GetCollectionNames() {
			wg.Add(1)
			idx := idx
			collectionName := collectionName
			func() {
				defer wg.Done()
				if resp.GetInMemoryPercentages()[idx] == 100 || resp.GetQueryServiceAvailable()[idx] {
					status, err := s.Cluster.MilvusClient.ReleaseCollection(context.Background(), &milvuspb.ReleaseCollectionRequest{
						CollectionName: collectionName,
					})
					err = merr.CheckRPCCall(status, err)
					s.NoError(err)
					collectionID := resp.GetCollectionIds()[idx]
					s.CheckCollectionCacheReleased(collectionID)
				}
			}()
		}
		wg.Wait()
	}
	s.T().Log("Tear Down test...")
	defer s.cancelFunc()
	if s.Cluster != nil {
		s.Cluster.Stop()
		s.Cluster = nil
	}
}
