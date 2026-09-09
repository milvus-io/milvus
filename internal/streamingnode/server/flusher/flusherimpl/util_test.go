//go:build test
// +build test

package flusherimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// An empty message id paired with a real timestamp is not the dropped-channel
// marker -- that one carries math.MaxUint64 -- so the vchannel survives
// getRecoveryInfo and reaches the checkpoint computation. It is the shape
// datacoord answers with when a vchannel has no channel checkpoint and its
// earliest segment came out of a compaction or an import, and it has to resolve
// to the EARLIEST message id the WAL retains, on every backend: WoodPecker used
// to get that by accident of its serialization, every other backend used to
// panic, and a guard keyed on the position's never-set WALName rejected both.
func TestGetRecoveryInfosSeeksEarliestWhenThePositionHasNoMessageID(t *testing.T) {
	const vchannel = "no-message-id-vchannel"

	for _, tc := range []struct {
		name    string
		walName message.WALName
		common  commonpb.WALName
	}{
		{"rocksmq, whose deserializer panics on empty bytes", message.WALNameRocksmq, commonpb.WALName_RocksMQ},
		{"woodpecker, whose deserializer reads empty bytes as earliest", message.WALNameWoodpecker, commonpb.WALName_WoodPecker},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			w := mock_wal.NewMockWAL(t)
			w.EXPECT().WALName().Return(tc.walName).Maybe()
			impl := &WALFlusherImpl{logger: mlog.With(), wal: syncutil.NewFuture[wal.WAL]()}
			impl.wal.Set(w)

			infos, checkpoint, err := impl.getRecoveryInfos(context.Background(), []string{vchannel})
			require.NoError(t, err)
			require.Contains(t, infos, vchannel)
			require.NotNil(t, checkpoint)

			earliestID, _ := adaptor.MustGetEarliestMessageIDFromMQType(tc.common)
			earliest := adaptor.MustGetMessageIDFromMQWrapperID(earliestID)
			assert.True(t, checkpoint.EQ(earliest),
				"an unknown position must resolve to the earliest the WAL retains; seeking later loses data")
		})
	}
}
