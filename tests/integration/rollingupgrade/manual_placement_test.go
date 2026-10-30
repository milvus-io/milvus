package rollingupgrade

import (
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type ManualPlacementSuite struct{ integration.MiniClusterSuite }

func (s *ManualPlacementSuite) SetupSuite() {
	paramtable.Init()
	// Manual placement remains available while optional optimization is disabled.
	s.WithMilvusConfig(paramtable.Get().QueryViewCfg.BalancerOptimizationEnabled.Key, "false")
	s.MiniClusterSuite.SetupSuite()
}

func (s *ManualPlacementSuite) TestMoveSegments() {
	c, ctx := s.Cluster, s.Cluster.GetContext()
	status, err := c.MixCoordClient.ResumeBalance(ctx, &querypb.ResumeBalanceRequest{})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	balance, err := c.MixCoordClient.CheckBalanceStatus(ctx, &querypb.CheckBalanceStatusRequest{})
	s.Require().NoError(merr.CheckRPCCall(balance, err))
	s.Require().True(balance.GetIsActive())
	status, err = c.MixCoordClient.SuspendBalance(ctx, &querypb.SuspendBalanceRequest{})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	balance, err = c.MixCoordClient.CheckBalanceStatus(ctx, &querypb.CheckBalanceStatusRequest{})
	s.Require().NoError(merr.CheckRPCCall(balance, err))
	s.Require().False(balance.GetIsActive())
	key := paramtable.Get().QueryViewCfg.BalancerOptimizationEnabled.Key
	stored, err := c.EtcdCli.Get(ctx, path.Join(c.RootPath(), "config", config.EtcdConfigKey(key)))
	s.Require().NoError(err)
	s.Require().Len(stored.Kvs, 1)
	s.Require().Equal("false", string(stored.Kvs[0].Value))
	// A new reader recovers the persisted setting, then changes it through
	// AlterConfig's storage entry point from outside the coordinator process.
	manager := config.NewManager()
	defer manager.Close()
	sourceConfig, err := config.NewEtcdSource(c.EtcdCli, &config.EtcdInfo{KeyPrefix: c.RootPath(), RefreshInterval: time.Hour})
	s.Require().NoError(err)
	s.Require().NoError(manager.AddSource(sourceConfig))
	_, recovered, err := manager.GetConfig(key)
	s.Require().NoError(err)
	s.Require().Equal("false", recovered)
	s.Require().NoError(manager.AlterConfigsInEtcd(sourceConfig, map[string]string{key: "true"}, nil))
	s.Require().Eventually(func() bool {
		resp, err := c.MixCoordClient.CheckBalanceStatus(ctx, &querypb.CheckBalanceStatusRequest{})
		return merr.CheckRPCCall(resp, err) == nil && resp.GetIsActive()
	}, 10*time.Second, 100*time.Millisecond, "a previous SuspendBalance must not mask subsequent AlterConfig")
	status, err = c.MixCoordClient.SuspendBalance(ctx, &querypb.SuspendBalanceRequest{})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	balance, err = c.MixCoordClient.CheckBalanceStatus(ctx, &querypb.CheckBalanceStatusRequest{})
	s.Require().NoError(merr.CheckRPCCall(balance, err))
	s.Require().False(balance.GetIsActive())
	second := c.AddQueryNode()
	name := "ManualPlacement" + funcutil.GenRandomStr()
	const dim, rows = 128, 3000
	schema, err := proto.Marshal(integration.ConstructSchema(name, dim, true))
	s.Require().NoError(err)
	status, err = c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: schema, ShardsNum: 2,
		Properties: []*commonpb.KeyValuePair{{Key: common.CollectionAutoCompactionKey, Value: "false"}},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	insert, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, NumRows: rows, HashKeys: integration.GenerateHashKeys(rows),
		FieldsData: []*schemapb.FieldData{integration.NewFloatVectorFieldData(integration.FloatVecField, rows, dim)},
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	flush, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	s.WaitForFlush(ctx, flush.GetFlushCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)
	status, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: integration.FloatVecField, IndexName: "vector_index",
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, integration.FloatVecField)
	status, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	describe, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(describe, err))
	source, target := c.DefaultQueryNode().GetNodeID(), second.GetNodeID()
	distribution, err := c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: source})
	s.Require().NoError(merr.CheckRPCCall(distribution, err))
	if len(distribution.GetSealedSegmentIDs()) == 0 {
		source, target = target, source
	}
	assertServing := func(node int64) {
		replicas, err := c.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{CollectionID: describe.GetCollectionID(), WithShardNodes: true})
		s.Require().NoError(merr.CheckRPCCall(replicas, err))
		s.Require().Len(replicas.GetReplicas(), 1)
		s.Require().Len(replicas.GetReplicas()[0].GetShardReplicas(), 2)
		for _, shard := range replicas.GetReplicas()[0].GetShardReplicas() {
			var queryNodes []int64
			for _, id := range shard.GetNodeIds() {
				if id != shard.GetLeaderID() {
					queryNodes = append(queryNodes, id)
				}
			}
			s.Require().Equal([]int64{node}, queryNodes, "successful RPC must already have serving target views")
		}
		result, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: name, OutputFields: []string{"count(*)"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		s.Require().Equal([]int64{rows}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	}
	status, err = c.MixCoordClient.TransferSegment(ctx, &querypb.TransferSegmentRequest{SourceNodeID: source, TargetNodeID: target, TransferAll: true})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	assertServing(target)
	status, err = c.MixCoordClient.LoadBalance(ctx, &querypb.LoadBalanceRequest{
		CollectionID: describe.GetCollectionID(), SourceNodeIDs: []int64{target}, DstNodeIDs: []int64{source},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	assertServing(source)
	status, err = c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func TestManualPlacement(t *testing.T) { suite.Run(t, new(ManualPlacementSuite)) }
