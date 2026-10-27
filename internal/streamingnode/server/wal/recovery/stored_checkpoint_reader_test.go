package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
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

	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).
		Return(&datapb.GetChannelRecoveryInfoResponse{
			Status: merr.Status(merr.WrapErrChannelNotAvailable("v1", "start position is nil")),
		}, nil).Once()
	_, err = reader.StoredChannelCheckpoint(context.Background(), "v1")
	assert.Error(t, err)

	coord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).
		Return(nil, errors.New("coordinator down")).Once()
	_, err = reader.StoredChannelCheckpoint(context.Background(), "v1")
	assert.Error(t, err)
}
