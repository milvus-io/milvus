package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestResolveReadWALOpensHistoricalBackend(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRW}
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()

	var openedWALName message.WALName
	roWAL := &roWALAdaptorImpl{
		roWALImpls: currentWAL,
		historicalWALOpener: func(
			_ context.Context,
			walName message.WALName,
			gotChannel types.PChannelInfo,
		) (walimpls.ROWALImpls, error) {
			openedWALName = walName
			assert.Equal(t, channel, gotChannel)
			return historicalWAL, nil
		},
	}
	roWAL.SetLogger(mlog.With())

	readWAL, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	})
	require.NoError(t, err)
	assert.Same(t, historicalWAL, readWAL)
	assert.Equal(t, message.WALNameRocksmq, openedWALName)
}

func TestOpenHistoricalWALForcesReadOnlyMode(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRW}
	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	openerImpl := mock_walimpls.NewMockOpenerImpls(t)
	openerImpl.EXPECT().Open(mock.Anything, mock.MatchedBy(func(opt *walimpls.OpenOption) bool {
		return opt.Channel.Name == channel.Name &&
			opt.Channel.Term == channel.Term &&
			opt.Channel.AccessMode == types.AccessModeRO
	})).Return(historicalWAL, nil).Once()

	opener := &openerAdaptorImpl{
		openerCache: map[message.WALName]walimpls.OpenerImpls{
			message.WALNameRocksmq: openerImpl,
		},
	}
	openedWAL, err := opener.openHistoricalWAL(context.Background(), message.WALNameRocksmq, channel)
	require.NoError(t, err)
	assert.Same(t, historicalWAL, openedWAL)
}
