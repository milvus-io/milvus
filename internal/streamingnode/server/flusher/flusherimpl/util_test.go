//go:build test
// +build test

package flusherimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// An empty message id paired with a real timestamp is not the dropped-channel
// marker -- that one carries math.MaxUint64 -- so the vchannel survives
// getRecoveryInfo and reaches the seekability check. The failure that comes back
// from there has to carry a merr code, so that the recovery path fails with a
// coded error rather than an opaque string.
func TestGetRecoveryInfosRejectsUnseekablePosition(t *testing.T) {
	const vchannel = "unseekable-vchannel"

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(
		&datapb.GetChannelRecoveryInfoResponse{
			Status: merr.Status(nil),
			Info: &datapb.VchannelInfo{
				ChannelName:  vchannel,
				SeekPosition: &msgpb.MsgPosition{Timestamp: 42},
			},
		}, nil).Once()
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)

	impl := &WALFlusherImpl{logger: mlog.With(), wal: syncutil.NewFuture[wal.WAL]()}
	impl.wal.Set(newMockWAL(t, true))

	infos, checkpoint, err := impl.getRecoveryInfos(context.Background(), []string{vchannel})
	assert.Nil(t, infos)
	assert.Nil(t, checkpoint)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Contains(t, err.Error(), vchannel)
}
