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
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/extension"
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
	replicaNumber, resourceGroups, scopedResourceGroups, userSpecifiedReplicaMode, err := s.getLoadReplicaConfigForRequest(
		ctx,
		req.GetReplicaNumber(),
		req.GetResourceGroups(),
		req.GetCollectionID(),
	)
	if err != nil {
		return err
	}

	legacyLoadConfig := s.getCurrentLoadConfig(ctx, req.GetCollectionID())
	// Node numbers are checked for a first load, and not for a config update
	// on a loaded collection, which is master's rule and stays the stock
	// binary's. With a form installed, a request that names resource groups
	// on a loaded collection and asks one of them for more replicas than it
	// holds is not a config update: it is a scoped expansion into those
	// groups (see completePlacementForOutOfScopeResourceGroups), which
	// places replicas exactly as a first load does, so it is admitted
	// against the same bounds. Without the check, a group with no node would
	// receive replicas that never get a delegator: the scoped task's clock
	// would pause on an unknown progress forever and the group would report 0
	// indefinitely.
	// LoadPartitions has always passed true here for the same reason.
	//
	// Admission runs only for a request that ADDS replicas to a group it
	// names. A scoped request that adds none is not admitted against
	// anything: the same load re-sent, a shrink, or a request that changes
	// only its load fields, partitions, priority or replica mode at the
	// same counts - a config update, which master never admits either. None
	// of these places a replica, and refusing one for a node that is
	// restarting would turn an idempotent retry into a failure against a
	// collection that is still serving.
	//
	// Whether the request is scoped is the decision getLoadReplicaConfigForRequest
	// took, not a second reading of the request: under a cluster-level force
	// override the groups the request named are discarded and the load states
	// the whole placement, which is a config update like any other.
	requestedReplicasNumber, err := utils.ReplicaNumberByResourceGroup(resourceGroups, replicaNumber)
	if err != nil {
		return err
	}
	checkNodeNum := legacyLoadConfig.Collection == nil ||
		(extension.FormInstalled() && len(scopedResourceGroups) > 0 && scopedLoadAddsReplicas(requestedReplicasNumber, legacyLoadConfig))
	expectedReplicasNumber, err := utils.AssignReplica(ctx, s.meta, resourceGroups, replicaNumber, checkNodeNum)
	if err != nil {
		return err
	}
	// With a form installed, a request that names resource groups speaks only
	// for those and leaves the placement of the others alone; a request that
	// names none - and every request on a stock binary - states the whole
	// placement, which is the native behavior, and this returns what
	// AssignReplica just produced. The scoping list comes from the same call
	// that resolved the configuration, so both are decided from one reading of
	// it.
	expectedReplicasNumber = completePlacementForOutOfScopeResourceGroups(
		ctx, req.GetCollectionID(), scopedResourceGroups, expectedReplicasNumber, legacyLoadConfig)
	alterLoadConfigReq := &job.AlterLoadConfigRequest{
		Meta:           s.meta,
		CollectionInfo: coll,
		Current:        legacyLoadConfig,
		Expected: job.ExpectedLoadConfig{
			ExpectedPartitionIDs:             partitionIDs,
			ExpectedReplicaNumber:            expectedReplicasNumber,
			ExpectedFieldIndexID:             req.GetFieldIndexID(),
			ExpectedLoadFields:               req.GetLoadFields(),
			ExpectedPriority:                 req.GetPriority(),
			ExpectedUserSpecifiedReplicaMode: userSpecifiedReplicaMode,
		},
	}

	currentLoadConfig := s.qviewsRuntime.loadConfigStore.GetConfig(req.GetCollectionID())
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
	requiredByRG, err := s.checkLoadResource(ctx, alterLoadConfigReq)
	if err != nil {
		return err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	if err != nil {
		return err
	}
	recordLoadResourceDemand(requiredByRG)
	return nil
}

// getLoadReplicaConfigForRequest resolves the replica configuration a load
// request runs with. It returns, in order: the replica number, the resource
// groups to assign replicas in (defaulted, which is what AssignReplica needs),
// the resource groups the REQUEST itself speaks for, and whether the caller
// named a replica number.
//
// The third return value is the scoping decision, and it is the raw list off
// the request, taken before the defaulting below rewrites an empty one to the
// default resource group. A request naming no group is a request about the
// collection, not a request about the default group: reading the defaulted list
// instead would turn every bare load into a request scoped to
// __default_resource_group, carry every other group's replicas through it, and
// so add a replica nobody asked for to a load_collection and refuse a
// load_partitions for "changing the replica number" of a group the caller never
// mentioned.
//
// Empty means "this request states the whole placement". A cluster-level force
// override states it by definition -- it replaces both the replica number and
// the resource groups of every load -- so it answers empty however the request
// itself was written, and it is read exactly once here, so the two answers
// cannot disagree with each other.
func (s *Server) getLoadReplicaConfigForRequest(ctx context.Context, replicaNumber int32, resourceGroups []string, collectionID int64) (int32, []string, []string, bool, error) {
	// If force override is enabled with a complete cluster-level load config,
	// new load requests are interpreted as cluster-managed even when the request
	// carries explicit replica/RG parameters.
	if overrideReplicaNumber, overrideResourceGroups, ok := getClusterLevelLoadConfigForForceOverride(); ok {
		mlog.Info(ctx,
			"force override user-specified replica mode for load request",
			mlog.Int64("collectionID", collectionID),
			mlog.Int32("replicaNumber", overrideReplicaNumber),
			mlog.Strings("resourceGroups", overrideResourceGroups))
		return overrideReplicaNumber, overrideResourceGroups, nil, false, nil
	}
	scopedResourceGroups := resourceGroups

	// If user specified the replica number in load request, load config changes
	// won't be applied to the collection automatically.
	userSpecifiedReplicaMode := replicaNumber > 0
	replicaNumber, resourceGroups, err := s.getDefaultResourceGroupsAndReplicaNumber(ctx, replicaNumber, resourceGroups, collectionID)
	return replicaNumber, resourceGroups, scopedResourceGroups, userSpecifiedReplicaMode, err
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
		WithControlChannelBroadcast().
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
	if s.qviewsRuntime != nil {
		return qviewsCurrentLoadConfig(s.qviewsRuntime.loadConfigStore.GetConfig(collectionID))
	}
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

// qviewsCurrentLoadConfig adapts desired placement to the existing admission
// checks without consulting the legacy collection/replica caches.
func qviewsCurrentLoadConfig(cfg *loadmgr.LoadConfig) job.CurrentLoadConfig {
	current := job.CurrentLoadConfig{Partitions: make(map[int64]*meta.Partition), Replicas: make(map[int64]*meta.Replica)}
	if cfg == nil {
		return current
	}
	info := &querypb.CollectionLoadInfo{CollectionID: cfg.CollectionID, DbID: cfg.DbID, ReplicaNumber: int32(len(cfg.Replicas)), Status: querypb.LoadStatus_Loaded, UserSpecifiedReplicaMode: cfg.UserSpecifiedReplicaMode, FieldIndexID: make(map[int64]int64)}
	for _, field := range cfg.LoadFields {
		info.LoadFields = append(info.LoadFields, field.GetFieldId())
		if field.GetIndexId() != 0 {
			info.FieldIndexID[field.GetFieldId()] = field.GetIndexId()
		}
	}
	current.Collection = &meta.Collection{CollectionLoadInfo: info}
	for _, id := range cfg.PartitionIDs {
		current.Partitions[id] = &meta.Partition{PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: cfg.CollectionID, PartitionID: id, Status: querypb.LoadStatus_Loaded}}
	}
	for _, replica := range cfg.Replicas {
		current.Replicas[replica.ReplicaID] = meta.NewReplicaWithPriority(&querypb.Replica{ID: replica.ReplicaID, CollectionID: cfg.CollectionID, ResourceGroup: replica.ResourceGroup}, replica.Priority)
	}
	return current
}
