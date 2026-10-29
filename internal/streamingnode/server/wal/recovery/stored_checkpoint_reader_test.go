package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStoredCheckpointReader(t *testing.T) {
	coord := mocks.NewMockMixCoordClient(t)
	reader := storedCheckpointReader{coord: coord}

	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, &datapb.GetChannelRecoveryInfoRequest{Vchannel: "v1"}).
		Return(&datapb.GetChannelRecoveryInfoResponse{
			Status: merr.Success(),
			Info:   &datapb.VchannelInfo{SeekPosition: &msgpb.MsgPosition{Timestamp: 42}},
		}, nil).Once()
	timeTick, err := reader.StoredChannelCheckpoint(context.Background(), "v1")
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), timeTick)

	// A transport failure is reported as is; the collection is not asked
	// about, which the mock enforces by having no expectation for it.
	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).
		Return(nil, errors.New("coordinator down")).Once()
	_, err = reader.StoredChannelCheckpoint(context.Background(), "v1")
	assert.Error(t, err)
	assert.False(t, errors.Is(err, vchannel.ErrCollectionGone))
}

// TestStoredCheckpointReaderCollectionGone: a channel DataCoord no longer has
// any position for is reported as its collection being gone only when
// RootCoord confirms the collection does not exist; while the collection is
// alive, or when RootCoord cannot be asked, the channel is just not available.
func TestStoredCheckpointReaderCollectionGone(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_100v0"
	notAvailable := &datapb.GetChannelRecoveryInfoResponse{
		Status: merr.Status(merr.WrapErrChannelNotAvailable(channel, "start position is nil")),
	}
	coord := mocks.NewMockMixCoordClient(t)
	reader := storedCheckpointReader{coord: coord}

	// The collection is still there: DataCoord has no position for the channel
	// yet, which is not the same as the collection being gone.
	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(notAvailable, nil).Once()
	coord.EXPECT().DescribeCollection(mock.Anything, &milvuspb.DescribeCollectionRequest{CollectionID: 100}).
		Return(&milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 100}, nil).Once()
	_, err := reader.StoredChannelCheckpoint(context.Background(), channel)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrChannelNotAvailable))
	assert.False(t, errors.Is(err, vchannel.ErrCollectionGone))

	// RootCoord cannot be asked: not gone, the wait goes on.
	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(notAvailable, nil).Once()
	coord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, errors.New("rootcoord down")).Once()
	_, err = reader.StoredChannelCheckpoint(context.Background(), channel)
	require.Error(t, err)
	assert.False(t, errors.Is(err, vchannel.ErrCollectionGone))

	// The collection is gone from the user's perspective, which is what
	// DataCoord's garbage collector tests before it drops the checkpoint.
	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(notAvailable, nil).Once()
	coord.EXPECT().DescribeCollection(mock.Anything, &milvuspb.DescribeCollectionRequest{CollectionID: 100}).
		Return(&milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(100))}, nil).Once()
	_, err = reader.StoredChannelCheckpoint(context.Background(), channel)
	require.Error(t, err)
	assert.True(t, errors.Is(err, vchannel.ErrCollectionGone))

	// A vchannel name no collection id can be read from is never gone.
	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(&datapb.GetChannelRecoveryInfoResponse{
		Status: merr.Status(merr.WrapErrChannelNotAvailable("v1", "start position is nil")),
	}, nil).Once()
	_, err = reader.StoredChannelCheckpoint(context.Background(), "v1")
	require.Error(t, err)
	assert.False(t, errors.Is(err, vchannel.ErrCollectionGone))
}
