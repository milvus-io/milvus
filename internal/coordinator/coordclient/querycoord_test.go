package coordclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestQueryCoordLocalClient(t *testing.T) {
	c := newQueryCoordLocalClient()
	c.setReadyServer(querypb.UnimplementedQueryCoordServer{})

	ctx := context.Background()

	_, err := c.GetComponentStates(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetTimeTickChannel(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetStatisticsChannel(ctx, nil)
	assert.Error(t, err)

	_, err = c.ShowCollections(ctx, nil)
	assert.Error(t, err)

	_, err = c.ShowPartitions(ctx, nil)
	assert.Error(t, err)

	_, err = c.LoadPartitions(ctx, nil)
	assert.Error(t, err)

	_, err = c.ReleasePartitions(ctx, nil)
	assert.Error(t, err)

	_, err = c.LoadCollection(ctx, nil)
	assert.Error(t, err)

	_, err = c.ReleaseCollection(ctx, nil)
	assert.Error(t, err)

	_, err = c.SyncNewCreatedPartition(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetPartitionStates(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetSegmentInfo(ctx, nil)
	assert.Error(t, err)

	_, err = c.LoadBalance(ctx, nil)
	assert.Error(t, err)

	_, err = c.ShowConfigurations(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetMetrics(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetReplicas(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetShardLeaders(ctx, nil)
	assert.Error(t, err)

	_, err = c.CheckHealth(ctx, nil)
	assert.Error(t, err)

	_, err = c.CreateResourceGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.UpdateResourceGroups(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropResourceGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferReplica(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListResourceGroups(ctx, nil)
	assert.Error(t, err)

	_, err = c.DescribeResourceGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListCheckers(ctx, nil)
	assert.Error(t, err)

	_, err = c.ActivateChecker(ctx, nil)
	assert.Error(t, err)

	_, err = c.DeactivateChecker(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListQueryNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetQueryNodeDistribution(ctx, nil)
	assert.Error(t, err)

	_, err = c.SuspendBalance(ctx, nil)
	assert.Error(t, err)

	_, err = c.ResumeBalance(ctx, nil)
	assert.Error(t, err)

	_, err = c.SuspendNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.ResumeNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferSegment(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferChannel(ctx, nil)
	assert.Error(t, err)

	_, err = c.CheckQueryNodeDistribution(ctx, nil)
	assert.Error(t, err)

	_, err = c.UpdateLoadConfig(ctx, nil)
	assert.Error(t, err)

	c.Close()
}

func TestQueryCoordLocalClientWithTimeout(t *testing.T) {
	c := newQueryCoordLocalClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.GetComponentStates(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetTimeTickChannel(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetStatisticsChannel(ctx, nil)
	assert.Error(t, err)

	_, err = c.ShowCollections(ctx, nil)
	assert.Error(t, err)

	_, err = c.ShowPartitions(ctx, nil)
	assert.Error(t, err)

	_, err = c.LoadPartitions(ctx, nil)
	assert.Error(t, err)

	_, err = c.ReleasePartitions(ctx, nil)
	assert.Error(t, err)

	_, err = c.LoadCollection(ctx, nil)
	assert.Error(t, err)

	_, err = c.ReleaseCollection(ctx, nil)
	assert.Error(t, err)

	_, err = c.SyncNewCreatedPartition(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetPartitionStates(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetSegmentInfo(ctx, nil)
	assert.Error(t, err)

	_, err = c.LoadBalance(ctx, nil)
	assert.Error(t, err)

	_, err = c.ShowConfigurations(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetMetrics(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetReplicas(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetShardLeaders(ctx, nil)
	assert.Error(t, err)

	_, err = c.CheckHealth(ctx, nil)
	assert.Error(t, err)

	_, err = c.CreateResourceGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.UpdateResourceGroups(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropResourceGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferReplica(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListResourceGroups(ctx, nil)
	assert.Error(t, err)

	_, err = c.DescribeResourceGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListCheckers(ctx, nil)
	assert.Error(t, err)

	_, err = c.ActivateChecker(ctx, nil)
	assert.Error(t, err)

	_, err = c.DeactivateChecker(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListQueryNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.GetQueryNodeDistribution(ctx, nil)
	assert.Error(t, err)

	_, err = c.SuspendBalance(ctx, nil)
	assert.Error(t, err)

	_, err = c.ResumeBalance(ctx, nil)
	assert.Error(t, err)

	_, err = c.SuspendNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.ResumeNode(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferSegment(ctx, nil)
	assert.Error(t, err)

	_, err = c.TransferChannel(ctx, nil)
	assert.Error(t, err)

	_, err = c.CheckQueryNodeDistribution(ctx, nil)
	assert.Error(t, err)

	_, err = c.UpdateLoadConfig(ctx, nil)
	assert.Error(t, err)
}
