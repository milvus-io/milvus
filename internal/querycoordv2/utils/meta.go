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

package utils

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReplicaNumberByResourceGroup is the layout a load request asks for: how
// many replicas in which resource group. It is the first thing AssignReplica
// decides, and a caller that has to compare the request with what the
// collection already holds before deciding whether to admit it reads the
// same answer from here.
func ReplicaNumberByResourceGroup(resourceGroups []string, replicaNumber int32) (map[string]int, error) {
	return loadmgr.ReplicaNumberByResourceGroup(resourceGroups, replicaNumber)
}

func AssignReplica(ctx context.Context, m *meta.Meta, resourceGroups []string, replicaNumber int32, checkNodeNum bool) (map[string]int, error) {
	replicaNumInRG, err := ReplicaNumberByResourceGroup(resourceGroups, replicaNumber)
	if err != nil {
		return nil, err
	}

	// Preserve the existing load admission policy independently of delegator
	// placement. Form loads are bounded by resource-group QueryNodes below.
	if !extension.FormInstalled() && checkNodeNum {
		streamingNodeCount := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Len()
		if replicaNumber > int32(streamingNodeCount) {
			return nil, merr.WrapErrStreamingNodeNotEnough(streamingNodeCount, int(replicaNumber), fmt.Sprintf("when load %d replica count", replicaNumber))
		}
	}

	// TODO: !!!Warning, ResourceManager and ReplicaManager doesn't protected with each other in concurrent operation.
	// 1. replica1 got rg1's node snapshot but doesn't spawn finished.
	// 2. rg1 is removed.
	// 3. replica1 spawn finished, but cannot find related resource group.
	for rgName, num := range replicaNumInRG {
		if !m.ContainResourceGroup(ctx, rgName) {
			return nil, merr.WrapErrResourceGroupNotFound(rgName)
		}
		nodes, err := m.GetNodes(ctx, rgName)
		if err != nil {
			return nil, err
		}

		if num > len(nodes) {
			mlog.Warn(ctx, "failed to check resource group", mlog.Err(err))
			if checkNodeNum {
				err := merr.WrapErrResourceGroupNodeNotEnough(rgName, len(nodes), num)
				return nil, err
			}
		}
	}
	return replicaNumInRG, nil
}
