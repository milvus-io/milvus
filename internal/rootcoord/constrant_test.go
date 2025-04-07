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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	"github.com/milvus-io/milvus/internal/util/testutil"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestCheckGeneralCapacity(t *testing.T) {
	testutil.ResetEnvironment()
	ctx := context.Background()

	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCollections(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAliases(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().CreateDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	allocator := mocktso.NewAllocator(t)
	allocator.EXPECT().GenerateTSO(mock.Anything).Return(1000, nil)

	meta, err := NewMetaTable(ctx, catalog, allocator)
	assert.NoError(t, err)
	core := newTestCore(withMeta(meta))
	assert.Equal(t, 0, meta.GetGeneralCount(ctx))

	Params.Save(Params.RootCoordCfg.MaxGeneralCapacity.Key, "512")
	defer Params.Reset(Params.RootCoordCfg.MaxGeneralCapacity.Key)

	assert.Equal(t, 0, meta.GetGeneralCount(ctx))
	err = checkGeneralCapacity(ctx, 1, 2, 256, core)
	assert.NoError(t, err)
	assert.Equal(t, 0, meta.GetGeneralCount(ctx))
	err = checkGeneralCapacity(ctx, 2, 4, 256, core)
	assert.Error(t, err)

	catalog.EXPECT().CreateCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err = meta.CreateDatabase(ctx, &model.Database{}, typeutil.MaxTimestamp)
	assert.NoError(t, err)
	err = meta.AddCollection(ctx, &model.Collection{
		CollectionID: 1,
		State:        pb.CollectionState_CollectionCreating,
		ShardsNum:    256,
		Partitions: []*model.Partition{
			{PartitionID: 100, State: pb.PartitionState_PartitionCreated},
			{PartitionID: 200, State: pb.PartitionState_PartitionCreated},
		},
	})
	assert.NoError(t, err)

	assert.Equal(t, 0, meta.GetGeneralCount(ctx))
	err = checkGeneralCapacity(ctx, 1, 2, 256, core)
	assert.NoError(t, err)

	catalog.EXPECT().AlterCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err = meta.ChangeCollectionState(ctx, 1, pb.CollectionState_CollectionCreated, typeutil.MaxTimestamp)
	assert.NoError(t, err)

	assert.Equal(t, 512, meta.GetGeneralCount(ctx))
	err = checkGeneralCapacity(ctx, 1, 1, 1, core)
	assert.Error(t, err)

	err = meta.ChangeCollectionState(ctx, 1, pb.CollectionState_CollectionDropping, typeutil.MaxTimestamp)
	assert.NoError(t, err)

	assert.Equal(t, 0, meta.GetGeneralCount(ctx))
	err = checkGeneralCapacity(ctx, 1, 2, 256, core)
	assert.NoError(t, err)
}
