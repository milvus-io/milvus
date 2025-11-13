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

package querycoordv2

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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
