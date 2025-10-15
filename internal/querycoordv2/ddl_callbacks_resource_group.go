package querycoordv2

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (s *Server) broadcastCreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	cfg := req.GetConfig()
	if cfg == nil {
		// Use default config if not set, compatible with old client.
		cfg = meta.NewResourceGroupConfig(0, 0)
	}
	if err := s.meta.ResourceManager.CheckIfResourceGroupAddable(ctx, req.GetResourceGroup(), cfg); err != nil {
		if errors.Is(err, meta.ErrResourceGroupOperationIgnored) {
			return nil
		}
		return err
	}
	msg := message.NewAlterResourceGroupMessageBuilderV2().
		WithHeader(&message.AlterResourceGroupMessageHeader{
			ResourceGroupConfigs: map[string]*rgpb.ResourceGroupConfig{req.GetResourceGroup(): cfg},
		}).
		WithBody(&message.AlterResourceGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (s *Server) broadcastUpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) error {
	if len(req.GetResourceGroups()) == 0 {
		return nil
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := s.meta.ResourceManager.CheckIfResourceGroupsUpdatable(ctx, req.GetResourceGroups()); err != nil {
		return err
	}
	msg := message.NewAlterResourceGroupMessageBuilderV2().
		WithHeader(&message.AlterResourceGroupMessageHeader{
			ResourceGroupConfigs: req.GetResourceGroups(),
		}).
		WithBody(&message.AlterResourceGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (s *Server) broadcastTransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// Move node from source resource group to target resource group.
	rgs, err := s.meta.ResourceManager.CheckIfTransferNode(ctx, req.GetSourceResourceGroup(), req.GetTargetResourceGroup(), int(req.GetNumNode()))
	if err != nil {
		log.Warn("failed to transfer node", zap.Error(err))
		return err
	}

	msg := message.NewAlterResourceGroupMessageBuilderV2().
		WithHeader(&message.AlterResourceGroupMessageHeader{
			ResourceGroupConfigs: rgs,
		}).
		WithBody(&message.AlterResourceGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallbacks) alterResourceGroupV2AckCallback(ctx context.Context, result message.BroadcastResultAlterResourceGroupMessageV2) error {
	return c.meta.ResourceManager.AlterResourceGroups(ctx, result.Message.Header().ResourceGroupConfigs)
}

func (s *Server) broadcastDropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return err
	}

	replicas := s.meta.ReplicaManager.GetByResourceGroup(ctx, req.GetResourceGroup())
	if len(replicas) > 0 {
		err := merr.WrapErrParameterInvalid("empty resource group", fmt.Sprintf("resource group %s has collection %d loaded", req.GetResourceGroup(), replicas[0].GetCollectionID()))
		return errors.Wrap(err,
			fmt.Sprintf("some replicas still loaded in resource group[%s], release it first", req.GetResourceGroup()))
	}

	if err := s.meta.ResourceManager.CheckIfResourceGroupDropable(ctx, req.GetResourceGroup()); err != nil {
		return err
	}

	msg := message.NewDropResourceGroupMessageBuilderV2().
		WithHeader(&message.DropResourceGroupMessageHeader{
			ResourceGroupName: req.GetResourceGroup(),
		}).
		WithBody(&message.DropResourceGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallbacks) dropResourceGroupV2AckCallback(ctx context.Context, result message.BroadcastResultDropResourceGroupMessageV2) error {
	return c.meta.ResourceManager.RemoveResourceGroup(ctx, result.Message.Header().ResourceGroupName)
}
