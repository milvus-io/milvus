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

package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// QueryCoordCatalog is the persistence interface for querycoord metadata. It is
// pure proto (no model types), so it lives in the shared pkg/v3 module and can be
// reused by the pooled catalog service; internal/metastore keeps a type-alias
// shim so the existing call sites compile unchanged.
type QueryCoordCatalog interface {
	SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error
	SavePartition(ctx context.Context, info ...*querypb.PartitionLoadInfo) error
	SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error
	GetCollections(ctx context.Context) ([]*querypb.CollectionLoadInfo, error)
	GetPartitions(ctx context.Context, collectionIDs []int64) (map[int64][]*querypb.PartitionLoadInfo, error)
	GetReplicas(ctx context.Context) ([]*querypb.Replica, error)
	ReleaseCollection(ctx context.Context, collection int64) error
	ReleasePartition(ctx context.Context, collection int64, partitions ...int64) error
	ReleaseReplicas(ctx context.Context, collectionID int64) error
	ReleaseReplica(ctx context.Context, collection int64, replicas ...int64) error
	SaveResourceGroup(ctx context.Context, rgs ...*querypb.ResourceGroup) error
	RemoveResourceGroup(ctx context.Context, rgName string) error
	GetResourceGroups(ctx context.Context) ([]*querypb.ResourceGroup, error)

	SaveCollectionTargets(ctx context.Context, target ...*querypb.CollectionTarget) error
	RemoveCollectionTarget(ctx context.Context, collectionID int64) error
	RemoveCollectionTargets(ctx context.Context) error
	GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error)
}
