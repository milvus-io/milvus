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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

// GetReplicationRole reports this cluster's live replication role (primary/secondary) and
// whether it is part of a CDC replication topology at all. It is modeled on
// DDLCallbacks.ValidateImportReplication and reads the same balancer channel assignment. A non-nil
// error means the role could not be determined (a transient balancer error, or
// OnShutdownError while streamingcoord stops before datacoord); the caller must treat
// that as indeterminate and NOT allocate, because a secondary that allocated its own
// range would diverge from the primary's authoritative range. A nil assignment or a
// non-replicating configuration is an unambiguous "not replicating" → (RolePrimary,
// false, nil), so a standalone cluster proceeds to allocate and broadcast.
func GetReplicationRole(ctx context.Context) (replicateutil.Role, bool, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return replicateutil.RolePrimary, false, err
	}
	assignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return replicateutil.RolePrimary, false, err
	}
	if assignment == nil {
		return replicateutil.RolePrimary, false, nil
	}
	cfg := assignment.ReplicateConfiguration
	if !isReplicatingCluster(cfg) {
		return replicateutil.RolePrimary, false, nil
	}
	helper, err := replicateutil.NewConfigHelper(Params.CommonCfg.ClusterPrefix.GetValue(), cfg)
	if err != nil {
		return replicateutil.RolePrimary, true, err
	}
	return helper.GetCurrentCluster().Role(), true, nil
}

func isReplicatingCluster(cfg *commonpb.ReplicateConfiguration) bool {
	return cfg != nil && (len(cfg.GetCrossClusterTopology()) > 0 || len(cfg.GetClusters()) > 1)
}
