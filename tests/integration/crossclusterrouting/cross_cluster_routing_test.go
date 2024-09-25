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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type CrossClusterRoutingSuite struct {
	integration.MiniClusterSuite
}

func (s *CrossClusterRoutingSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	s.Require().NoError(s.SetupEmbedEtcd())

	paramtable.Init()
	paramtable.Get().Save("grpc.client.maxMaxAttempts", "1")
}

func (s *CrossClusterRoutingSuite) TearDownSuite() {
	s.TearDownEmbedEtcd()
	paramtable.Get().Save("grpc.client.maxMaxAttempts", strconv.FormatInt(paramtable.DefaultMaxAttempts, 10))
}

func (s *CrossClusterRoutingSuite) TestCrossClusterRouting() {
	const (
		waitFor  = time.Second * 10
		duration = time.Millisecond * 10
	)

	go func() {
		for {
			select {
			case <-time.After(15 * time.Second):
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
		resp, err := s.Cluster.RootCoordClient.ShowCollections(s.Cluster.GetContext(), &milvuspb.ShowCollectionsRequest{
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
		resp, err := s.Cluster.DataCoordClient.GetRecoveryInfoV2(s.Cluster.GetContext(), &datapb.GetRecoveryInfoRequestV2{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test queryCoord
	s.Eventually(func() bool {
		resp, err := s.Cluster.QueryCoordClient.LoadCollection(s.Cluster.GetContext(), &querypb.LoadCollectionRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test proxy
	s.Eventually(func() bool {
		resp, err := s.Cluster.ProxyClient.InvalidateCollectionMetaCache(s.Cluster.GetContext(), &proxypb.InvalidateCollMetaCacheRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test dataNode
	s.Eventually(func() bool {
		resp, err := s.Cluster.DataNodeClient.FlushSegments(s.Cluster.GetContext(), &datapb.FlushSegmentsRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test queryNode
	s.Eventually(func() bool {
		resp, err := s.Cluster.QueryNodeClient.Search(s.Cluster.GetContext(), &querypb.SearchRequest{})
		s.Suite.T().Logf("resp: %s, err: %s", resp, err)
		if err != nil {
			return strings.Contains(err.Error(), merr.ErrServiceUnavailable.Error())
		}
		return false
	}, waitFor, duration)

	// test indexNode
	s.Eventually(func() bool {
		resp, err := s.Cluster.IndexNodeClient.CreateJob(s.Cluster.GetContext(), &workerpb.CreateJobRequest{})
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
