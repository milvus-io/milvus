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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (s *Server) broadcastDropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

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
	return c.meta.ResourceManager.DropResourceGroup(ctx, result.Message.Header().ResourceGroupName)
}
