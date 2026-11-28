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

package job

import (
	"context"
	"maps"
	"slices"
	"sort"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type AlterLoadConfigRequest struct {
	Meta           *meta.Meta
	CollectionInfo *metacache.CollectionInfo
	Expected       ExpectedLoadConfig
	Current        CurrentLoadConfig

	// ScopedResourceGroups is the list of resource groups the REQUEST named, as
	// the caller wrote it; empty when it named none. Only a form reads it (see
	// CheckIfLoadPartitionsExecutable): with one installed, a request naming
	// groups speaks only for those, and Expected.ExpectedReplicaNumber carries
	// the other groups' counts through unchanged.
	ScopedResourceGroups []string
}

// CheckIfLoadPartitionsExecutable checks if the load partitions is executable:
// loading more partitions of a loaded collection may not change its replica
// number.
//
// On a stock binary that is the total, as it always was. With a form installed
// and a request that names resource groups, the request speaks only for those
// groups (the completed placement carries the others through unchanged, see
// completePlacementForOutOfScopeResourceGroups), so the rule is applied to the
// named groups alone: a named group that already holds replicas must keep its
// count, while a named group that holds none is being added, which is the
// expansion a scoped load exists for and not a replica-number change.
// Comparing the total there would refuse every such expansion.
func (req *AlterLoadConfigRequest) CheckIfLoadPartitionsExecutable() error {
	if req.Current.Collection == nil {
		return nil
	}
	if extension.FormInstalled() && len(req.ScopedResourceGroups) > 0 {
		current := req.Current.GetReplicaNumber()
		for _, rgName := range req.ScopedResourceGroups {
			held := current[rgName]
			if held == 0 {
				continue // the request adds this group
			}
			if expected := req.Expected.ExpectedReplicaNumber[rgName]; held != expected {
				return merr.WrapErrParameterInvalid(held, expected,
					"can't change the replica number for loaded partitions in resource group "+rgName)
			}
		}
		return nil
	}
	expectedReplicaNumber := 0
	for _, num := range req.Expected.ExpectedReplicaNumber {
		expectedReplicaNumber += num
	}
	if len(req.Current.Replicas) != expectedReplicaNumber {
		return merr.WrapErrParameterInvalid(len(req.Current.Replicas), expectedReplicaNumber, "can't change the replica number for loaded partitions")
	}
	return nil
}

type ExpectedLoadConfig struct {
	ExpectedAutoLoad                 bool
	ExpectedPartitionIDs             []int64
	ExpectedReplicaNumber            map[string]int // map resource group name to replica number in resource group
	ExpectedFieldIndexID             map[int64]int64
	ExpectedLoadFields               []int64 // Explicit preloads, already resolved by the request adapter.
	ExpectedRequiredFields           []*messagespb.LoadFieldConfig
	ExpectedPriority                 commonpb.LoadPriority
	ExpectedUserSpecifiedReplicaMode bool
}

type CurrentLoadConfig struct {
	AutoLoad   bool
	Collection *meta.Collection
	Partitions map[int64]*meta.Partition
	Replicas   map[int64]*meta.Replica
}

func (c *CurrentLoadConfig) GetLoadPriority() commonpb.LoadPriority {
	for _, replica := range c.Replicas {
		return replica.LoadPriority()
	}
	return commonpb.LoadPriority_HIGH
}

func (c *CurrentLoadConfig) GetFieldIndexID() map[int64]int64 {
	return c.Collection.FieldIndexID
}

func (c *CurrentLoadConfig) GetLoadFields() []int64 {
	return c.Collection.LoadFields
}

func (c *CurrentLoadConfig) GetUserSpecifiedReplicaMode() bool {
	return c.Collection.UserSpecifiedReplicaMode
}

func (c *CurrentLoadConfig) GetReplicaNumber() map[string]int {
	replicaNumber := make(map[string]int)
	for _, replica := range c.Replicas {
		replicaNumber[replica.GetResourceGroup()]++
	}
	return replicaNumber
}

func (c *CurrentLoadConfig) GetPartitionIDs() []int64 {
	partitionIDs := make([]int64, 0, len(c.Partitions))
	for _, partition := range c.Partitions {
		partitionIDs = append(partitionIDs, partition.GetPartitionID())
	}
	return partitionIDs
}

// IntoLoadConfigMessageHeader converts the current load config into a load config message header.
func (c *CurrentLoadConfig) IntoLoadConfigMessageHeader() *messagespb.AlterLoadConfigMessageHeader {
	if c.Collection == nil {
		return nil
	}
	partitionIDs := make([]int64, 0, len(c.Partitions))
	partitionIDs = append(partitionIDs, c.GetPartitionIDs()...)
	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})

	loadFields := generateLoadFields(c.GetLoadFields(), c.GetFieldIndexID())

	return &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     c.Collection.DbID,
		CollectionId:             c.Collection.CollectionID,
		PartitionIds:             partitionIDs,
		LoadFields:               loadFields,
		RequiredFields:           c.Collection.GetRequiredFields(),
		Replicas:                 loadmgr.ReplicasToMessage(c.sortedReplicas()),
		UserSpecifiedReplicaMode: c.GetUserSpecifiedReplicaMode(),
		AutoLoad:                 c.AutoLoad,
	}
}

// GenerateAlterLoadConfigMessage generates the alter load config message for the collection.
// It returns a nil message (with a nil error) when the expected load config is identical to
// the current one, i.e. there is nothing to broadcast and the operation is a no-op.
func GenerateAlterLoadConfigMessage(ctx context.Context, req *AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
	loadFields := generateLoadFields(req.Expected.ExpectedLoadFields, req.Expected.ExpectedFieldIndexID)
	loadReplicaConfigs, err := req.generateReplicas(ctx)
	if err != nil {
		return nil, err
	}

	partitionIDs := make([]int64, 0, len(req.Expected.ExpectedPartitionIDs))
	partitionIDs = append(partitionIDs, req.Expected.ExpectedPartitionIDs...)
	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})
	header := &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     req.CollectionInfo.DatabaseID,
		CollectionId:             req.CollectionInfo.ID,
		PartitionIds:             partitionIDs,
		LoadFields:               loadFields,
		RequiredFields:           req.Expected.ExpectedRequiredFields,
		Replicas:                 loadReplicaConfigs,
		UserSpecifiedReplicaMode: req.Expected.ExpectedUserSpecifiedReplicaMode,
		AutoLoad:                 req.Expected.ExpectedAutoLoad,
	}
	// check if the load configuration is changed; nothing to broadcast if not.
	if previousHeader := req.Current.IntoLoadConfigMessageHeader(); proto.Equal(previousHeader, header) {
		return nil, nil
	}
	return message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithControlChannelBroadcast().
		MustBuildBroadcast(), nil
}

// generateLoadFields generates the load fields for the collection.
func generateLoadFields(loadedFields []int64, fieldIndexID map[int64]int64) []*messagespb.LoadFieldConfig {
	loadFields := lo.Map(loadedFields, func(fieldID int64, _ int) *messagespb.LoadFieldConfig {
		if indexID, ok := fieldIndexID[fieldID]; ok {
			return &messagespb.LoadFieldConfig{
				FieldId: fieldID,
				IndexId: indexID,
			}
		}
		return &messagespb.LoadFieldConfig{
			FieldId: fieldID,
			IndexId: 0,
		}
	})
	sort.Slice(loadFields, func(i, j int) bool {
		return loadFields[i].GetFieldId() < loadFields[j].GetFieldId()
	})
	return loadFields
}

// generateReplicas generates the replicas for the collection.
func (req *AlterLoadConfigRequest) generateReplicas(ctx context.Context) ([]*messagespb.LoadReplicaConfig, error) {
	replicas, err := loadmgr.ReconcileReplicas(req.Current.sortedReplicas(), req.Expected.ExpectedReplicaNumber, req.Expected.ExpectedPriority, func() (int64, error) {
		return req.Meta.AllocateReplicaID(ctx)
	})
	if err != nil {
		return nil, err
	}
	return loadmgr.ReplicasToMessage(replicas), nil
}

func (c *CurrentLoadConfig) sortedReplicas() []*loadmgr.ReplicaAssignment {
	replicas := make([]*loadmgr.ReplicaAssignment, 0, len(c.Replicas))
	for _, id := range slices.Sorted(maps.Keys(c.Replicas)) {
		r := c.Replicas[id]
		replicas = append(replicas, &loadmgr.ReplicaAssignment{ReplicaID: r.GetID(), ResourceGroup: r.GetResourceGroup(), Priority: r.LoadPriority()})
	}
	return replicas
}
