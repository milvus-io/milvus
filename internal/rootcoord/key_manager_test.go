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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type mockCipherWithState struct {
	hookutil.CipherWithState
	getStatesFunc            func() (map[int64]string, error)
	registerRotationCallback func(callback func(ezID int64) error)
}

func (m *mockCipherWithState) GetStates() (map[int64]string, error) {
	if m.getStatesFunc != nil {
		return m.getStatesFunc()
	}
	return nil, nil
}

func (m *mockCipherWithState) RegisterRotationCallback(callback func(ezID int64) error) {
	if m.registerRotationCallback != nil {
		m.registerRotationCallback(callback)
	}
}

func TestNewKeyManager(t *testing.T) {
	ctx := context.Background()
	meta := mockrootcoord.NewIMetaTable(t)
	mixCoord := mocks.NewMixCoord(t)

	km := NewKeyManager(ctx, meta, mixCoord)

	assert.NotNil(t, km)
	assert.Equal(t, ctx, km.ctx)
	assert.Equal(t, meta, km.meta)
	assert.Equal(t, mixCoord, km.mixCoord)
	assert.Equal(t, hookutil.GetCipherWithState() != nil, km.enabled)
}

func TestKeyManager_Init(t *testing.T) {
	t.Run("disabled cipher", func(t *testing.T) {
		ctx := context.Background()
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  false,
		}

		err := km.Init()
		assert.NoError(t, err)
	})
}

func TestKeyManager_GetDatabaseEzStates(t *testing.T) {
	t.Run("disabled cipher", func(t *testing.T) {
		ctx := context.Background()
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  false,
		}

		dbIDs, err := km.GetDatabaseEzStates()
		assert.NoError(t, err)
		assert.Nil(t, dbIDs)
	})
}

func TestKeyManager_GetDatabaseByEzID(t *testing.T) {
	ctx := context.Background()

	t.Run("success get database", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		expectedDB := &model.Database{
			ID:   123,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(expectedDB, nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		db, err := km.getDatabaseByEzID(123)
		assert.NoError(t, err)
		assert.Equal(t, expectedDB, db)
	})

	t.Run("fallback to default database", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		defaultDB := &model.Database{
			ID:   util.DefaultDBID,
			Name: util.DefaultDBName,
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(nil, errors.New("db not found")).Once()
		meta.EXPECT().GetDatabaseByID(ctx, util.DefaultDBID, uint64(0)).Return(defaultDB, nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		db, err := km.getDatabaseByEzID(123)
		assert.NoError(t, err)
		assert.Equal(t, defaultDB, db)
	})
}

func TestKeyManager_ReleaseLoadedCollections(t *testing.T) {
	ctx := context.Background()

	t.Run("empty revoked db ids", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{})
		assert.NoError(t, err)
	})

	t.Run("failed to get database metadata", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(nil, errors.New("db not found")).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.NoError(t, err)
	})

	t.Run("failed to list collections", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   1,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(db, nil).Once()
		meta.EXPECT().ListCollections(ctx, "test_db", uint64(0), true).Return(nil, errors.New("list failed")).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.NoError(t, err)
	})

	t.Run("no collections to check", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   1,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(db, nil).Once()
		meta.EXPECT().ListCollections(ctx, "test_db", uint64(0), true).Return([]*model.Collection{}, nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.NoError(t, err)
	})

	t.Run("no collections loaded", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   1,
			Name: "test_db",
		}

		colls := []*model.Collection{
			{CollectionID: 100},
			{CollectionID: 101},
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(db, nil).Once()
		meta.EXPECT().ListCollections(ctx, "test_db", uint64(0), true).Return(colls, nil).Once()

		resp := &querypb.ShowCollectionsResponse{
			Status:        merr.Status(merr.ErrCollectionNotLoaded),
			CollectionIDs: []int64{},
		}
		mixCoord.EXPECT().ShowLoadCollections(ctx, mock.Anything).Return(resp, nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.NoError(t, err)
	})

	t.Run("failed to show loaded collections", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   1,
			Name: "test_db",
		}

		colls := []*model.Collection{
			{CollectionID: 100},
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(db, nil).Once()
		meta.EXPECT().ListCollections(ctx, "test_db", uint64(0), true).Return(colls, nil).Once()

		mixCoord.EXPECT().ShowLoadCollections(ctx, mock.Anything).Return(nil, errors.New("show failed")).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.Error(t, err)
	})

	t.Run("release collections successfully", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   1,
			Name: "test_db",
		}

		colls := []*model.Collection{
			{CollectionID: 100},
			{CollectionID: 101},
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(db, nil).Once()
		meta.EXPECT().ListCollections(ctx, "test_db", uint64(0), true).Return(colls, nil).Once()

		resp := &querypb.ShowCollectionsResponse{
			Status:        merr.Success(),
			CollectionIDs: []int64{100, 101},
		}
		mixCoord.EXPECT().ShowLoadCollections(ctx, mock.Anything).Return(resp, nil).Once()
		mixCoord.EXPECT().ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{CollectionID: int64(100)}).Return(merr.Success(), nil).Once()
		mixCoord.EXPECT().ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{CollectionID: int64(101)}).Return(merr.Success(), nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.NoError(t, err)
	})

	t.Run("release collection with partial failures", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   1,
			Name: "test_db",
		}

		colls := []*model.Collection{
			{CollectionID: 100},
			{CollectionID: 101},
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(1), uint64(0)).Return(db, nil).Once()
		meta.EXPECT().ListCollections(ctx, "test_db", uint64(0), true).Return(colls, nil).Once()

		resp := &querypb.ShowCollectionsResponse{
			Status:        merr.Success(),
			CollectionIDs: []int64{100, 101},
		}
		mixCoord.EXPECT().ShowLoadCollections(ctx, mock.Anything).Return(resp, nil).Once()
		mixCoord.EXPECT().ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{CollectionID: int64(100)}).Return(nil, errors.New("release failed")).Once()
		mixCoord.EXPECT().ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{CollectionID: int64(101)}).Return(merr.Success(), nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.releaseLoadedCollections([]int64{1})
		assert.NoError(t, err)
	})
}

func TestKeyManager_OnKeyRotated(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled cipher", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  false,
		}

		err := km.onKeyRotated(123)
		assert.NoError(t, err)
	})

	t.Run("failed to get database", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(nil, errors.New("db not found")).Once()
		meta.EXPECT().GetDatabaseByID(ctx, util.DefaultDBID, uint64(0)).Return(nil, errors.New("default db not found")).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.onKeyRotated(123)
		assert.Error(t, err)
	})

	t.Run("failed to alter database", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   123,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(db, nil).Once()

		expectedReq := &rootcoordpb.AlterDatabaseRequest{
			DbName: "test_db",
			Properties: []*commonpb.KeyValuePair{
				{
					Key: common.InternalCipherKeyRotatedKey,
				},
			},
		}

		mixCoord.EXPECT().AlterDatabase(ctx, expectedReq).Return(nil, errors.New("alter failed")).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.onKeyRotated(123)
		assert.Error(t, err)
	})

	t.Run("alter database with error status", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   123,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(db, nil).Once()

		expectedReq := &rootcoordpb.AlterDatabaseRequest{
			DbName: "test_db",
			Properties: []*commonpb.KeyValuePair{
				{
					Key: common.InternalCipherKeyRotatedKey,
				},
			},
		}

		mixCoord.EXPECT().AlterDatabase(ctx, expectedReq).Return(merr.Status(errors.New("alter error")), nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.onKeyRotated(123)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		mixCoord := mocks.NewMixCoord(t)

		db := &model.Database{
			ID:   123,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(db, nil).Once()

		expectedReq := &rootcoordpb.AlterDatabaseRequest{
			DbName: "test_db",
			Properties: []*commonpb.KeyValuePair{
				{
					Key: common.InternalCipherKeyRotatedKey,
				},
			},
		}

		mixCoord.EXPECT().AlterDatabase(ctx, expectedReq).Return(merr.Success(), nil).Once()

		km := &KeyManager{
			ctx:      ctx,
			meta:     meta,
			mixCoord: mixCoord,
			enabled:  true,
		}

		err := km.onKeyRotated(123)
		assert.NoError(t, err)
	})
}
