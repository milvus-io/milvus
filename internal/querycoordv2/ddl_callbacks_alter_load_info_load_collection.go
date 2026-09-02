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
	"sort"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	replicaNumber, resourceGroups, userSpecifiedReplicaMode, err := s.getLoadReplicaConfigForRequest(
		ctx,
		req.GetReplicaNumber(),
		req.GetResourceGroups(),
		req.GetCollectionID(),
	)
	if err != nil {
		return err
	}

	currentLoadConfig := s.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[req.GetCollectionID()]
	// only check node number when the collection is not loaded
	expectedReplicasNumber, err := utils.AssignReplica(ctx, s.meta, resourceGroups, replicaNumber, currentLoadConfig == nil)
	if err != nil {
		return err
	}
	msg, err := s.generateAlterLoadConfigMessageForLoadCollection(ctx, coll, currentLoadConfig, qviewsExpectedLoadConfig{
		PartitionIDs:             partitionIDs,
		ReplicaNumber:            expectedReplicasNumber,
		FieldIndexID:             req.GetFieldIndexID(),
		LoadFields:               req.GetLoadFields(),
		Priority:                 req.GetPriority(),
		UserSpecifiedReplicaMode: userSpecifiedReplicaMode,
	})
	if err != nil {
		return err
	}
	if msg == nil {
		// load config unchanged, the collection is already loaded as requested.
		mlog.Info(ctx, "load collection ignored, load config is unchanged",
			mlog.Int64("collectionID", req.GetCollectionID()))
		return nil
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (s *Server) getLoadReplicaConfigForRequest(ctx context.Context, replicaNumber int32, resourceGroups []string, collectionID int64) (int32, []string, bool, error) {
	// If force override is enabled with a complete cluster-level load config,
	// new load requests are interpreted as cluster-managed even when the request
	// carries explicit replica/RG parameters.
	if overrideReplicaNumber, overrideResourceGroups, ok := getClusterLevelLoadConfigForForceOverride(); ok {
		mlog.Info(ctx,
			"force override user-specified replica mode for load request",
			mlog.Int64("collectionID", collectionID),
			mlog.Int32("replicaNumber", overrideReplicaNumber),
			mlog.Strings("resourceGroups", overrideResourceGroups))
		return overrideReplicaNumber, overrideResourceGroups, false, nil
	}

	// If user specified the replica number in load request, load config changes
	// won't be applied to the collection automatically.
	userSpecifiedReplicaMode := replicaNumber > 0
	replicaNumber, resourceGroups, err := s.getDefaultResourceGroupsAndReplicaNumber(ctx, replicaNumber, resourceGroups, collectionID)
	return replicaNumber, resourceGroups, userSpecifiedReplicaMode, err
}

func getClusterLevelLoadConfigForForceOverride() (int32, []string, bool) {
	queryCoordCfg := &paramtable.Get().QueryCoordCfg
	if !queryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool() {
		return 0, nil, false
	}

	replicaNumber := queryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt32()
	resourceGroups := queryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	if replicaNumber <= 0 || len(resourceGroups) == 0 {
		return 0, nil, false
	}
	if len(resourceGroups) != 1 && len(resourceGroups) != int(replicaNumber) {
		return 0, nil, false
	}
	return replicaNumber, resourceGroups, true
}

type qviewsExpectedLoadConfig struct {
	PartitionIDs             []int64
	ReplicaNumber            map[string]int
	FieldIndexID             map[int64]int64
	LoadFields               []int64
	Priority                 commonpb.LoadPriority
	UserSpecifiedReplicaMode bool
}

func (s *Server) generateAlterLoadConfigMessageForLoadCollection(
	ctx context.Context,
	coll *milvuspb.DescribeCollectionResponse,
	current *loadmgr.LoadConfig,
	expected qviewsExpectedLoadConfig,
) (message.BroadcastMutableMessage, error) {
	replicas, err := s.generateQViewsReplicaConfigs(ctx, current, expected)
	if err != nil {
		return nil, err
	}
	header := &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     coll.GetDbId(),
		CollectionId:             coll.GetCollectionID(),
		PartitionIds:             sortedInt64s(expected.PartitionIDs),
		LoadFields:               generateQViewsLoadFields(expected.LoadFields, expected.FieldIndexID),
		Replicas:                 replicas,
		UserSpecifiedReplicaMode: expected.UserSpecifiedReplicaMode,
	}
	if proto.Equal(loadConfigIntoAlterLoadConfigHeader(current), header) {
		return nil, nil
	}
	return message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{loadConfigBroadcastChannel()}).
		MustBuildBroadcast(), nil
}

func (s *Server) generateQViewsReplicaConfigs(
	ctx context.Context,
	current *loadmgr.LoadConfig,
	expected qviewsExpectedLoadConfig,
) ([]*messagespb.LoadReplicaConfig, error) {
	existingReplicaNum := make(map[string]int)
	redundantReplicas := make([]int64, 0)
	replicaConfigs := make([]*messagespb.LoadReplicaConfig, 0)
	currentReplicas := sortedReplicaAssignments(current)
	for _, replica := range currentReplicas {
		rgName := replica.ResourceGroup
		if existingReplicaNum[rgName] >= expected.ReplicaNumber[rgName] {
			redundantReplicas = append(redundantReplicas, replica.ReplicaID)
			continue
		}
		replicaConfigs = append(replicaConfigs, newLoadReplicaConfig(replica.ReplicaID, rgName, replica.Priority))
		existingReplicaNum[rgName]++
	}

	rgNames := lo.Keys(expected.ReplicaNumber)
	sort.Strings(rgNames)
	for _, rgName := range rgNames {
		for i := existingReplicaNum[rgName]; i < expected.ReplicaNumber[rgName]; i++ {
			if len(redundantReplicas) > 0 {
				replicaID := redundantReplicas[0]
				redundantReplicas = redundantReplicas[1:]
				replicaConfigs = append(replicaConfigs, newLoadReplicaConfig(replicaID, rgName, expected.Priority))
				continue
			}
			replicaID, err := s.meta.AllocateReplicaID(ctx)
			if err != nil {
				return nil, err
			}
			replicaConfigs = append(replicaConfigs, newLoadReplicaConfig(replicaID, rgName, expected.Priority))
		}
	}
	sort.Slice(replicaConfigs, func(i, j int) bool {
		return replicaConfigs[i].GetReplicaId() < replicaConfigs[j].GetReplicaId()
	})
	return replicaConfigs, nil
}

func newLoadReplicaConfig(replicaID int64, rgName string, priority commonpb.LoadPriority) *messagespb.LoadReplicaConfig {
	return &messagespb.LoadReplicaConfig{
		ReplicaId:         replicaID,
		ResourceGroupName: rgName,
		Priority:          priority,
	}
}

func loadConfigIntoAlterLoadConfigHeader(cfg *loadmgr.LoadConfig) *messagespb.AlterLoadConfigMessageHeader {
	if cfg == nil {
		return nil
	}
	replicas := lo.Map(sortedReplicaAssignments(cfg), func(replica *loadmgr.ReplicaAssignment, _ int) *messagespb.LoadReplicaConfig {
		return &messagespb.LoadReplicaConfig{
			ReplicaId:         replica.ReplicaID,
			ResourceGroupName: replica.ResourceGroup,
			Priority:          replica.Priority,
		}
	})
	return &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     cfg.DbID,
		CollectionId:             cfg.CollectionID,
		PartitionIds:             sortedInt64s(cfg.PartitionIDs),
		LoadFields:               cloneAndSortLoadFields(cfg.LoadFields),
		Replicas:                 replicas,
		UserSpecifiedReplicaMode: cfg.UserSpecifiedReplicaMode,
	}
}

func sortedReplicaAssignments(cfg *loadmgr.LoadConfig) []*loadmgr.ReplicaAssignment {
	if cfg == nil {
		return nil
	}
	replicas := append([]*loadmgr.ReplicaAssignment{}, cfg.Replicas...)
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].ReplicaID < replicas[j].ReplicaID
	})
	return replicas
}

func generateQViewsLoadFields(loadedFields []int64, fieldIndexID map[int64]int64) []*messagespb.LoadFieldConfig {
	loadFields := lo.Map(loadedFields, func(fieldID int64, _ int) *messagespb.LoadFieldConfig {
		return &messagespb.LoadFieldConfig{
			FieldId: fieldID,
			IndexId: fieldIndexID[fieldID],
		}
	})
	sort.Slice(loadFields, func(i, j int) bool {
		return loadFields[i].GetFieldId() < loadFields[j].GetFieldId()
	})
	return loadFields
}

func cloneAndSortLoadFields(fields []*messagespb.LoadFieldConfig) []*messagespb.LoadFieldConfig {
	out := make([]*messagespb.LoadFieldConfig, 0, len(fields))
	for _, field := range fields {
		out = append(out, &messagespb.LoadFieldConfig{
			FieldId: field.GetFieldId(),
			IndexId: field.GetIndexId(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetFieldId() < out[j].GetFieldId()
	})
	return out
}

func sortedInt64s(values []int64) []int64 {
	out := append([]int64{}, values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func loadConfigBroadcastChannel() string {
	return streaming.WAL().ControlChannel()
}

// getDefaultResourceGroupsAndReplicaNumber gets the default resource groups and replica number for the collection.
func (s *Server) getDefaultResourceGroupsAndReplicaNumber(ctx context.Context, replicaNumber int32, resourceGroups []string, collectionID int64) (int32, []string, error) {
	// so only both replica and resource groups didn't set in request, it will turn to use the configured load info
	if replicaNumber <= 0 && len(resourceGroups) == 0 {
		// when replica number or resource groups is not set, use pre-defined load config
		rgs, replicas, err := s.broker.GetCollectionLoadInfo(ctx, collectionID)
		if err != nil {
			mlog.Warn(ctx, "failed to get pre-defined load info", mlog.Err(err))
		} else {
			replicaNumber = int32(replicas)
			resourceGroups = rgs
		}
	}
	// to be compatible with old sdk, which set replica=1 if replica is not specified
	if replicaNumber <= 0 {
		mlog.Info(ctx, "request doesn't indicate the number of replicas, set it to 1")
		replicaNumber = 1
	}
	if len(resourceGroups) == 0 {
		mlog.Info(ctx,
			fmt.Sprintf("request doesn't indicate the resource groups, set it to %s", meta.DefaultResourceGroupName))
		resourceGroups = []string{meta.DefaultResourceGroupName}
	}
	return replicaNumber, resourceGroups, nil
}

func (s *Server) getCurrentLoadConfig(ctx context.Context, collectionID int64) job.CurrentLoadConfig {
	partitionList := s.meta.GetPartitionsByCollection(ctx, collectionID)
	loadedPartitions := make(map[int64]*meta.Partition)
	for _, partitioin := range partitionList {
		loadedPartitions[partitioin.PartitionID] = partitioin
	}

	replicas := s.meta.GetByCollection(ctx, collectionID)
	loadedReplicas := make(map[int64]*meta.Replica)
	for _, replica := range replicas {
		loadedReplicas[replica.GetID()] = replica
	}
	return job.CurrentLoadConfig{
		Collection: s.meta.GetCollection(ctx, collectionID),
		Partitions: loadedPartitions,
		Replicas:   loadedReplicas,
	}
}
