package vchantempstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func TestVChannelTempStorage(t *testing.T) {
	rcf := syncutil.NewFuture[types.RootCoordClient]()
	ts := NewVChannelTempStorage(rcf)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := ts.GetVChannelByPChannelOfCollection(ctx, 1, "test")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	ctx = context.Background()
	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:               merr.Success(),
		CollectionID:         1,
		PhysicalChannelNames: []string{"test1", "test2"},
		VirtualChannelNames:  []string{"test1-v0", "test2-v0"},
	}, nil)
	rcf.Set(rc)

	v, err := ts.GetVChannelByPChannelOfCollection(ctx, 1, "test1")
	assert.NoError(t, err)
	assert.Equal(t, "test1-v0", v)

	v, err = ts.GetVChannelByPChannelOfCollection(ctx, 1, "test2")
	assert.NoError(t, err)
	assert.Equal(t, "test2-v0", v)

	assert.Panics(t, func() {
		ts.GetVChannelByPChannelOfCollection(ctx, 1, "test3")
	})

	rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Unset()
	rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound)

	v, err = ts.GetVChannelByPChannelOfCollection(ctx, 1, "test1")
	assert.NoError(t, err)
	assert.Equal(t, "test1-v0", v)
	v, err = ts.GetVChannelByPChannelOfCollection(ctx, 1, "test2")
	assert.NoError(t, err)
	assert.Equal(t, "test2-v0", v)

	v, err = ts.GetVChannelByPChannelOfCollection(ctx, 2, "test2")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Equal(t, "", v)
}
