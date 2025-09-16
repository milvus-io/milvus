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

package autoreplica

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim    = 128
	dbName = ""
)

type AutoReplicaTestSuite struct {
	integration.MiniClusterSuite
}

func (s *AutoReplicaTestSuite) SetupSuite() {
	// Set configuration for faster balance and node detection
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1")
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.CheckNodeInReplicaInterval.Key, "1")

	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
}

func (s *AutoReplicaTestSuite) loadCollection(collectionName string, db string, replica int, rgs []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load collection
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         db,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
		ResourceGroups: rgs,
	})
	if err = merr.CheckRPCCall(loadStatus, err); err != nil {
		panic(err)
	}
	s.WaitForLoadWithDB(ctx, db, collectionName)
}

func (s *AutoReplicaTestSuite) releaseCollection(db, collectionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// release collection
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName:         db,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(status))
}

func (s *AutoReplicaTestSuite) createCollectionWithAutoReplica(ctx context.Context, collectionName string) {
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionAutoReplicaEnableKey,
				Value: "true",
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	// Insert test data and create index
	for i := 0; i < 3; i++ {
		err = s.InsertAndFlush(ctx, dbName, collectionName, 2000, dim)
		s.NoError(err)
	}

	// create index
	createIndexStatus, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, "L2"),
	})
	s.NoError(err)
	s.True(merr.Ok(createIndexStatus))
	s.WaitForIndexBuiltWithDB(ctx, dbName, collectionName, integration.FloatVecField)
}

func (s *AutoReplicaTestSuite) TestAutoReplica() {
	ctx := context.Background()

	qn := s.Cluster.AddQueryNode()
	sn := s.Cluster.AddStreamingNode()
	collectionName := "test_auto_replica_collection" + rand.String(5)

	// Step 1: Create collection with auto replica enabled in properties
	s.createCollectionWithAutoReplica(ctx, collectionName)

	// Step 2: Load collection without specifying replica and resource_group
	s.loadCollection(collectionName, dbName, 0, nil)

	// Step 3: Verify replica count is 1 (initially only 1 query node)
	resp, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp.Status))
	s.Len(resp.GetReplicas(), 2)
	log.Info("Initial replica count verified", zap.Int("replicas", len(resp.GetReplicas())))

	// Step 4: Add two new query nodes
	s.Cluster.AddQueryNode()
	s.Cluster.AddQueryNode()
	s.Cluster.AddStreamingNode()
	s.Cluster.AddStreamingNode()

	// Step 5: Verify replica count becomes 4 (2 original + 2 new query nodes)
	s.Eventually(func() bool {
		resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			WithShardNodes: true,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		log.Info("Current replica count after adding nodes", zap.Int("replicas", len(resp2.GetReplicas())))
		return len(resp2.GetReplicas()) == 4 && lo.CountBy(resp2.GetReplicas(), func(replica *milvuspb.ReplicaInfo) bool {
			return len(replica.GetShardReplicas()) == 1
		}) == 4
	}, 30*time.Second, 2*time.Second)

	// Step 6: Stop one query node
	qn.Stop(10 * time.Second)
	sn.Stop(10 * time.Second)

	// Step 7: Verify replica count becomes 3 (4 - 1 stopped node)
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		log.Info("Current replica count after stopping node", zap.Int("replicas", len(resp3.GetReplicas())))
		return len(resp3.GetReplicas()) == 3 && lo.CountBy(resp3.GetReplicas(), func(replica *milvuspb.ReplicaInfo) bool {
			return len(replica.GetShardReplicas()) == 1
		}) == 3
	}, 30*time.Second, 2*time.Second)

	// Cleanup
	s.releaseCollection(dbName, collectionName)
}

func TestAutoReplica(t *testing.T) {
	suite.Run(t, new(AutoReplicaTestSuite))
}
