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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// broadcastAlterLoadConfigCollectionV2ForTransferReplica broadcasts the alter load config message for transfer replica.
func (s *Server) broadcastAlterLoadConfigCollectionV2ForTransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) error {
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if ok := s.meta.ResourceManager.ContainResourceGroup(ctx, req.GetSourceResourceGroup()); !ok {
		return merr.WrapErrResourceGroupNotFound(req.GetSourceResourceGroup())
	}
	if ok := s.meta.ResourceManager.ContainResourceGroup(ctx, req.GetTargetResourceGroup()); !ok {
		return merr.WrapErrResourceGroupNotFound(req.GetTargetResourceGroup())
	}
	if req.GetSourceResourceGroup() == req.GetTargetResourceGroup() {
		return merr.WrapErrParameterInvalidMsg("source resource group and target resource group should not be the same, resource group: %s", req.GetSourceResourceGroup())
	}
	if req.GetNumReplica() <= 0 {
		return merr.WrapErrParameterInvalid("NumReplica > 0", fmt.Sprintf("invalid NumReplica %d", req.GetNumReplica()))
	}

	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}

	currentLoadConfig := s.getCurrentLoadConfig(ctx, req.GetCollectionID())
	if currentLoadConfig.Collection == nil {
		return merr.WrapErrCollectionNotLoaded(coll.CollectionName)
	}
	if int(req.NumReplica) > len(currentLoadConfig.Replicas) {
		return merr.WrapErrParameterInvalid(int(req.NumReplica), len(currentLoadConfig.Replicas), "the number of replicas to transfer is greater than the number of replicas in the collection")
	}

	replicaNumbers := currentLoadConfig.GetReplicaNumber()
	replicaNumberInSourceRG := replicaNumbers[req.GetSourceResourceGroup()]
	if replicaNumberInSourceRG < int(req.NumReplica) {
		return merr.WrapErrParameterInvalid("NumReplica not greater than the number of replica in source resource group",
			fmt.Sprintf("only found [%d] replicas of collection [%s] in source resource group [%s], but %d require",
				replicaNumberInSourceRG,
				coll.CollectionName,
				req.GetSourceResourceGroup(),
				req.GetNumReplica()))
	}
	// update the replica numbers in the source and target resource groups.
	replicaNumbers[req.GetSourceResourceGroup()] -= int(req.NumReplica)
	replicaNumbers[req.GetTargetResourceGroup()] += int(req.NumReplica)

	alterLoadConfigReq := &job.AlterLoadConfigRequest{
		Meta:           s.meta,
		CollectionInfo: coll,
		Current:        currentLoadConfig,
		Expected: job.ExpectedLoadConfig{
			ExpectedPartitionIDs:             currentLoadConfig.GetPartitionIDs(),
			ExpectedReplicaNumber:            replicaNumbers,
			ExpectedFieldIndexID:             currentLoadConfig.GetFieldIndexID(),
			ExpectedLoadFields:               currentLoadConfig.GetLoadFields(),
			ExpectedPriority:                 commonpb.LoadPriority_LOW,
			ExpectedUserSpecifiedReplicaMode: currentLoadConfig.GetUserSpecifiedReplicaMode(),
		},
	}
	msg, err := job.GenerateAlterLoadConfigMessage(ctx, alterLoadConfigReq)
	if err != nil {
		return err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}
