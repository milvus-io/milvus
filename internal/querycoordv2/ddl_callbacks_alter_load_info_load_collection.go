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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

// broadcastAlterLoadConfigCollectionV2ForLoadCollection is called when the load collection request is received.
func (s *Server) broadcastAlterLoadConfigCollectionV2ForLoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) error {
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// double check if the collection is already dropped
	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}

	partitionIDs, err := s.broker.GetPartitions(ctx, coll.CollectionID)
	if err != nil {
		return err
	}
	// if user specified the replica number in load request, load config changes won't be apply to the collection automatically
	userSpecifiedReplicaMode := req.GetReplicaNumber() > 0
	replicaNumber, resourceGroups, err := s.getDefaultResourceGroupsAndReplicaNumber(ctx, req.GetReplicaNumber(), req.GetResourceGroups(), req.GetCollectionID())
	if err != nil {
		return err
	}

	currentLoadConfig := s.getCurrentLoadConfig(ctx, req.GetCollectionID())
	// only check node number when the collection is not loaded
	expectedReplicasNumber, err := utils.AssignReplica(ctx, s.meta, resourceGroups, replicaNumber, currentLoadConfig.Collection == nil)
	if err != nil {
		return err
	}
	alterLoadConfigReq := &job.AlterLoadConfigRequest{
		Meta:           s.meta,
		CollectionInfo: coll,
		Current:        currentLoadConfig,
		Expected: job.ExpectedLoadConfig{
			ExpectedPartitionIDs:             partitionIDs,
			ExpectedReplicaNumber:            expectedReplicasNumber,
			ExpectedFieldIndexID:             req.GetFieldIndexID(),
			ExpectedLoadFields:               req.GetLoadFields(),
			ExpectedPriority:                 req.GetPriority(),
			ExpectedUserSpecifiedReplicaMode: userSpecifiedReplicaMode,
		},
	}
	msg, err := job.GenerateAlterLoadConfigMessage(ctx, alterLoadConfigReq)
	if err != nil {
		return err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// getDefaultResourceGroupsAndReplicaNumber gets the default resource groups and replica number for the collection.
func (s *Server) getDefaultResourceGroupsAndReplicaNumber(ctx context.Context, replicaNumber int32, resourceGroups []string, collectionID int64) (int32, []string, error) {
	// so only both replica and resource groups didn't set in request, it will turn to use the configured load info
	if replicaNumber <= 0 && len(resourceGroups) == 0 {
		// when replica number or resource groups is not set, use pre-defined load config
		rgs, replicas, err := s.broker.GetCollectionLoadInfo(ctx, collectionID)
		if err != nil {
			log.Warn("failed to get pre-defined load info", zap.Error(err))
		} else {
			replicaNumber = int32(replicas)
			resourceGroups = rgs
		}
	}
	// to be compatible with old sdk, which set replica=1 if replica is not specified
	if replicaNumber <= 0 {
		log.Info("request doesn't indicate the number of replicas, set it to 1")
		replicaNumber = 1
	}
	if len(resourceGroups) == 0 {
		log.Info(fmt.Sprintf("request doesn't indicate the resource groups, set it to %s", meta.DefaultResourceGroupName))
		resourceGroups = []string{meta.DefaultResourceGroupName}
	}
	return replicaNumber, resourceGroups, nil
}

func (s *Server) getCurrentLoadConfig(ctx context.Context, collectionID int64) job.CurrentLoadConfig {
	partitionList := s.meta.CollectionManager.GetPartitionsByCollection(ctx, collectionID)
	loadedPartitions := make(map[int64]*meta.Partition)
	for _, partitioin := range partitionList {
		loadedPartitions[partitioin.PartitionID] = partitioin
	}

	replicas := s.meta.ReplicaManager.GetByCollection(ctx, collectionID)
	loadedReplicas := make(map[int64]*meta.Replica)
	for _, replica := range replicas {
		loadedReplicas[replica.GetID()] = replica
	}
	return job.CurrentLoadConfig{
		Collection: s.meta.CollectionManager.GetCollection(ctx, collectionID),
		Partitions: loadedPartitions,
		Replicas:   loadedReplicas,
	}
}
