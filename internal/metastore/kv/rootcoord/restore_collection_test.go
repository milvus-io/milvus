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

package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCatalogRestoringCollectionPersistsAndReloads(t *testing.T) {
	ctx := t.Context()
	store := memkv.NewMemoryKV()
	catalog := NewCatalog(store)
	collection := &model.Collection{
		DBID: 1, CollectionID: 10, Name: "restored",
		State:      pb.CollectionState_CollectionRestoring,
		Partitions: []*model.Partition{{PartitionID: 20, PartitionName: "_default", CollectionID: 10}},
	}
	require.NoError(t, catalog.CreateCollection(ctx, collection, 100))
	require.NoError(t, catalog.CreateCollection(ctx, collection, 100), "callback replay is idempotent")
	recovered, err := NewCatalog(store).GetCollectionByID(ctx, 1, typeutil.MaxTimestamp, 10)
	require.NoError(t, err)
	require.Equal(t, pb.CollectionState_CollectionRestoring, recovered.State)
	require.False(t, recovered.Available(), "persistence must not expose incomplete restore")
	require.Len(t, recovered.Partitions, 1)
	require.EqualValues(t, 20, recovered.Partitions[0].PartitionID)
}
