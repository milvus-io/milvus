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

//go:build test && dynamic

package utils

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RecoverReplicaOfCollection recovers all replica of collection with latest resource group.
func RecoverReplicaOfCollection(ctx context.Context, m *meta.Meta, collectionID typeutil.UniqueID) {
	logger := mlog.With(mlog.FieldCollectionID(collectionID))
	rgNames := m.GetResourceGroupByCollection(ctx, collectionID)
	if rgNames.Len() == 0 {
		logger.Error(ctx, "no resource group found for collection")
		return
	}
	rgs, err := m.GetResourceGroups(ctx, rgNames.Collect())
	if err != nil {
		logger.Error(ctx, "unreachable code as expected, fail to get resource group for replica", mlog.Err(err))
		return
	}

	if err := m.RecoverNodesInCollection(ctx, collectionID, rgs); err != nil {
		logger.Warn(ctx, "fail to set available nodes in replica", mlog.Err(err))
	}
}

// RecoverAllCollectionrecovers all replica of all collection in resource group.
func RecoverAllCollection(m *meta.Meta) {
	for _, collection := range m.GetAll(context.TODO()) {
		RecoverReplicaOfCollection(context.TODO(), m, collection)
	}
}
