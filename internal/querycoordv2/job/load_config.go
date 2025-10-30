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
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

var ErrIgnoredAlterLoadConfig = errors.New("ignored alter load config")

type AlterLoadConfigRequest struct {
	Meta           *meta.Meta
	CollectionInfo *milvuspb.DescribeCollectionResponse
	Expected       ExpectedLoadConfig
	Current        CurrentLoadConfig
}

// CheckIfLoadPartitionsExecutable checks if the load partitions is executable.
func (req *AlterLoadConfigRequest) CheckIfLoadPartitionsExecutable() error {
	if req.Current.Collection == nil {
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
	ExpectedPartitionIDs             []int64
	ExpectedReplicaNumber            map[string]int // map resource group name to replica number in resource group
	ExpectedFieldIndexID             map[int64]int64
	ExpectedLoadFields               []int64
	ExpectedPriority                 commonpb.LoadPriority
	ExpectedUserSpecifiedReplicaMode bool
}

type CurrentLoadConfig struct {
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

	replicas := make([]*messagespb.LoadReplicaConfig, 0, len(c.Replicas))
	for _, replica := range c.Replicas {
		replicas = append(replicas, &messagespb.LoadReplicaConfig{
			ReplicaId:         replica.GetID(),
			ResourceGroupName: replica.GetResourceGroup(),
			Priority:          replica.LoadPriority(),
		})
	}
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].GetReplicaId() < replicas[j].GetReplicaId()
	})
	return &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     c.Collection.DbID,
		CollectionId:             c.Collection.CollectionID,
		PartitionIds:             partitionIDs,
		LoadFields:               loadFields,
		Replicas:                 replicas,
		UserSpecifiedReplicaMode: c.GetUserSpecifiedReplicaMode(),
	}
}

// GenerateAlterLoadConfigMessage generates the alter load config message for the collection.
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
		DbId:                     req.CollectionInfo.DbId,
		CollectionId:             req.CollectionInfo.CollectionID,
		PartitionIds:             partitionIDs,
		LoadFields:               loadFields,
		Replicas:                 loadReplicaConfigs,
		UserSpecifiedReplicaMode: req.Expected.ExpectedUserSpecifiedReplicaMode,
	}
	// check if the load configuration is changed
	if previousHeader := req.Current.IntoLoadConfigMessageHeader(); proto.Equal(previousHeader, header) {
		return nil, ErrIgnoredAlterLoadConfig
	}
	return message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
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
	// fill up the existsReplicaNum found the redundant replicas and the replicas that should be kept
	existsReplicaNum := make(map[string]int)
	keptReplicas := make(map[int64]struct{}) // replica that should be kept
	redundantReplicas := make([]int64, 0)    // replica that should be removed
	loadReplicaConfigs := make([]*messagespb.LoadReplicaConfig, 0)
	for _, replica := range req.Current.Replicas {
		if existsReplicaNum[replica.GetResourceGroup()] >= req.Expected.ExpectedReplicaNumber[replica.GetResourceGroup()] {
			redundantReplicas = append(redundantReplicas, replica.GetID())
			continue
		}
		keptReplicas[replica.GetID()] = struct{}{}
		loadReplicaConfigs = append(loadReplicaConfigs, &messagespb.LoadReplicaConfig{
			ReplicaId:         replica.GetID(),
			ResourceGroupName: replica.GetResourceGroup(),
			Priority:          replica.LoadPriority(),
		})
		existsReplicaNum[replica.GetResourceGroup()]++
	}

	// check if there should generate new incoming replicas.
	for rg, num := range req.Expected.ExpectedReplicaNumber {
		for i := existsReplicaNum[rg]; i < num; i++ {
			if len(redundantReplicas) > 0 {
				// reuse the replica from redundant replicas.
				// make a transfer operation from a resource group to another resource group.
				replicaID := redundantReplicas[0]
				redundantReplicas = redundantReplicas[1:]
				loadReplicaConfigs = append(loadReplicaConfigs, &messagespb.LoadReplicaConfig{
					ReplicaId:         replicaID,
					ResourceGroupName: rg,
					Priority:          req.Expected.ExpectedPriority,
				})
			} else {
				// allocate a new replica.
				newID, err := req.Meta.ReplicaManager.AllocateReplicaID(ctx)
				if err != nil {
					return nil, err
				}
				loadReplicaConfigs = append(loadReplicaConfigs, &messagespb.LoadReplicaConfig{
					ReplicaId:         newID,
					ResourceGroupName: rg,
					Priority:          req.Expected.ExpectedPriority,
				})
			}
		}
	}
	sort.Slice(loadReplicaConfigs, func(i, j int) bool {
		return loadReplicaConfigs[i].GetReplicaId() < loadReplicaConfigs[j].GetReplicaId()
	})
	return loadReplicaConfigs, nil
}
