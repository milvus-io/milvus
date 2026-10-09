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
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

var caseTimeout time.Duration

const integrationCaseTimeoutEnv = "MILVUS_INTEGRATION_CASE_TIMEOUT"

func init() {
	defaultTimeout, err := caseTimeoutFromEnvironment(os.Getenv)
	if err != nil {
		panic(err)
	}
	flag.DurationVar(&caseTimeout, "caseTimeout", defaultTimeout, "timeout duration for single case")
	streamingutil.SetStreamingServiceEnabled()
}

func caseTimeoutFromEnvironment(getenv func(string) string) (time.Duration, error) {
	value := getenv(integrationCaseTimeoutEnv)
	if value == "" {
		return 10 * time.Minute, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", integrationCaseTimeoutEnv, err)
	}
	return timeout, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout) //nolint:gosec // cancel is stored and called in TearDownSuite()
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
		for _, result := range releaseLoadedCollections(context.Background(), s.Cluster.MilvusClient, resp) {
			s.NoError(result.err, "release collection %s", result.name)
			s.CheckCollectionCacheReleased(result.id)
		}
	}
	s.T().Log("Tear Down test...")
	defer s.cancelFunc()
	if s.Cluster != nil {
		s.Cluster.Stop()
		s.Cluster = nil
	}
}

type collectionReleaseResult struct {
	name string
	id   int64
	err  error
}

// releaseLoadedCollections returns every selected collection in the original
// order, including failed releases. Cache checks and test assertions stay in
// the calling test goroutine.
func releaseLoadedCollections(ctx context.Context, client milvuspb.MilvusServiceClient, collections *milvuspb.ShowCollectionsResponse) []collectionReleaseResult {
	results := make([]collectionReleaseResult, 0, len(collections.GetCollectionNames()))
	for i, name := range collections.GetCollectionNames() {
		if collections.GetInMemoryPercentages()[i] == 100 || collections.GetQueryServiceAvailable()[i] {
			results = append(results, collectionReleaseResult{name: name, id: collections.GetCollectionIds()[i]})
		}
	}
	const concurrency = 4
	slots := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range results {
		slots <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			status, err := client.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
				CollectionName: results[i].name,
			})
			results[i].err = merr.CheckRPCCall(status, err)
		}(i)
	}
	wg.Wait()
	return results
}
