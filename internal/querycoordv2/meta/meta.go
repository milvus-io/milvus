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

package meta

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

type Meta struct {
	*CollectionManager
	*ReplicaManager
	*ResourceManager
}

func NewMeta(
	idAllocator func() (int64, error),
	catalog metastore.QueryCoordCatalog,
	nodeMgr *session.NodeManager,
) *Meta {
	return &Meta{
		NewCollectionManager(catalog),
		NewReplicaManager(idAllocator, catalog),
		NewResourceManager(catalog, nodeMgr),
	}
}

// RecoverReplicaTargets upgrades legacy collection metadata once, before serving
// requests. Old replica records were the persisted RG load intent. Recover that
// intent only when their total agrees with the independently stored replica count;
// incomplete legacy metadata remains unknown until a load-config update repairs it.
func (m *Meta) RecoverReplicaTargets(ctx context.Context) error {
	for _, collection := range m.GetAllCollections(ctx) {
		if len(collection.GetResourceGroupReplicaNumbers()) != 0 {
			continue
		}
		replicas := m.GetByCollection(ctx, collection.GetCollectionID())
		if len(replicas) != int(collection.GetReplicaNumber()) || len(replicas) == 0 {
			continue
		}
		counts := make(map[string]int32)
		for _, replica := range replicas {
			counts[replica.GetResourceGroup()]++
		}
		if err := m.UpdateReplicaConfig(ctx, collection.GetCollectionID(), collection.GetReplicaNumber(), collection.GetUserSpecifiedReplicaMode(), counts); err != nil {
			return err
		}
	}
	return nil
}
