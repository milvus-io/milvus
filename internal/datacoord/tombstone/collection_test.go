package tombstone

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestCollectionTombstone(t *testing.T) {
	catalog := mock_metastore.NewMockDataCoordCatalog(t)
	catalog.EXPECT().ListCollectionTombstone(mock.Anything).Return(nil, errors.New("list collection tombstone error"))
	tombstone, err := recoverCollectionTombstone(context.Background(), catalog)
	assert.Error(t, err)
	assert.Nil(t, tombstone)

	catalog.EXPECT().ListCollectionTombstone(mock.Anything).Unset()
	catalog.EXPECT().ListCollectionTombstone(mock.Anything).Return([]*model.CollectionTombstone{
		model.NewCollectionTombstone(&datapb.CollectionTombstoneImpl{
			CollectionId: 1,
			Vchannels:    []string{"v1", "v2"},
		}),
		model.NewParititionTombstone(&datapb.PartitionTombstoneImpl{
			CollectionId: 1,
			PartitionId:  2,
		}),
		model.NewParititionTombstone(&datapb.PartitionTombstoneImpl{
			CollectionId: 2,
			PartitionId:  2,
		}),
	}, nil)
	tombstone, err = recoverCollectionTombstone(context.Background(), catalog)
	assert.NoError(t, err)
	assert.NotNil(t, tombstone)

	err = tombstone.CheckIfPartitionDropped(1, 1)
	assert.Error(t, err)
	assert.True(t, merr.IsCollectionOrPartitionDropped(err))
	err = tombstone.CheckIfPartitionDropped(1, 2)
	assert.Error(t, err)
	assert.True(t, merr.IsCollectionOrPartitionDropped(err))
	err = tombstone.CheckIfPartitionDropped(2, 2)
	assert.Error(t, err)
	assert.True(t, merr.IsCollectionOrPartitionDropped(err))
	err = tombstone.CheckIfPartitionDropped(2, 1)
	assert.NoError(t, err)

	err = tombstone.CheckIfVChannelDropped("v1")
	assert.Error(t, err)
	assert.True(t, merr.IsCollectionOrPartitionDropped(err))
	err = tombstone.CheckIfVChannelDropped("v3")
	assert.NoError(t, err)

	/// catalog works incorrectly
	catalog.EXPECT().DropCollectionTombstone(mock.Anything, mock.Anything).Return(errors.New("drop collection tombstone error"))
	catalog.EXPECT().SaveCollectionTombstone(mock.Anything, mock.Anything).Return(errors.New("save collection tombstone error"))

	// Drop a collection already dropped
	err = tombstone.MarkCollectionAsDropping(context.Background(), &DroppingCollection{
		CollectionId: 1,
		Vchannels:    []string{"v1", "v2"},
	})
	assert.NoError(t, err)

	// Drop a partition that collection is already dropped
	err = tombstone.MarkPartitionAsDropping(context.Background(), &DroppingPartition{
		CollectionId: 1,
		PartitionId:  2,
	})
	assert.NoError(t, err)

	// Drop a partition that partition is already dropped
	err = tombstone.MarkPartitionAsDropping(context.Background(), &DroppingPartition{
		CollectionId: 2,
		PartitionId:  2,
	})
	assert.NoError(t, err)

	// Drop a partition that partition is already dropped
	err = tombstone.MarkPartitionAsDropping(context.Background(), &DroppingPartition{
		CollectionId: 2,
		PartitionId:  1,
	})
	assert.Error(t, err)

	err = tombstone.MarkCollectionAsDropping(context.Background(), &DroppingCollection{
		CollectionId: 3,
		Vchannels:    []string{"v31", "v32"},
	})
	assert.Error(t, err)

	// expire tombstone should not success because catalog error
	tombstone.expire(context.Background(), 0)
	err = tombstone.CheckIfPartitionDropped(1, 1)
	assert.Error(t, err)

	/// catalog works correctly
	catalog.EXPECT().DropCollectionTombstone(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().DropCollectionTombstone(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveCollectionTombstone(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().SaveCollectionTombstone(mock.Anything, mock.Anything).Return(nil)

	err = tombstone.MarkCollectionAsDropping(context.Background(), &DroppingCollection{
		CollectionId: 3,
		Vchannels:    []string{"v31", "v32"},
	})
	assert.NoError(t, err)
	err = tombstone.CheckIfPartitionDropped(3, 1)
	assert.Error(t, err)
	err = tombstone.CheckIfVChannelDropped("v31")
	assert.Error(t, err)

	err = tombstone.MarkPartitionAsDropping(context.Background(), &DroppingPartition{
		CollectionId: 2,
		PartitionId:  1,
	})
	assert.NoError(t, err)
	err = tombstone.CheckIfPartitionDropped(2, 1)
	assert.Error(t, err)

	// expire works
	tombstone.expire(context.Background(), 0)
	err = tombstone.CheckIfVChannelDropped("v31")
	assert.NoError(t, err)
	tombstone.Close()
}

func TestSingleton(t *testing.T) {
	catalog := mock_metastore.NewMockDataCoordCatalog(t)
	catalog.EXPECT().ListCollectionTombstone(mock.Anything).Return([]*model.CollectionTombstone{}, nil)
	err := RecoverCollectionTombstone(context.Background(), catalog)
	assert.NoError(t, err)
	assert.NotNil(t, CollectionTombstone())
}
