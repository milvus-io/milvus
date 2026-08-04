package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	resolved, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	})
	require.NoError(t, err)
	assert.Same(t, historicalWAL, resolved.wal)
	assert.Nil(t, resolved.exclusiveStartMessageID)
	assert.Equal(t, message.WALNameRocksmq, openedWALName)
}

func TestResolveReadWALNormalizesHistoricalStartAfter(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRW}
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	startMessageID := rmq.NewRmqID(2)
	roWAL := &roWALAdaptorImpl{
		roWALImpls: currentWAL,
		historicalWALOpener: func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return historicalWAL, nil
		},
	}
	roWAL.SetLogger(mlog.With())

	resolved, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartAfter(startMessageID),
	})
	require.NoError(t, err)
	assert.True(t, resolved.exclusiveStartMessageID.EQ(startMessageID))
	policy, ok := resolved.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
	require.True(t, ok)
	normalizedMessageID, err := message.UnmarshalMessageID(policy.StartFrom)
	require.NoError(t, err)
	assert.True(t, normalizedMessageID.EQ(startMessageID))
}

func TestResolveReadWALFallsBackWhenHistoricalWALIsMissing(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRW}
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	openCount := 0
	roWAL := &roWALAdaptorImpl{
		roWALImpls: currentWAL,
		historicalWALOpener: func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			openCount++
			return nil, merr.WrapErrMqTopicNotFound(channel.Name)
		},
	}
	roWAL.SetLogger(mlog.With())

	resolved, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	})
	require.NoError(t, err)
	assert.Same(t, currentWAL, resolved.wal)
	_, ok := resolved.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
	assert.True(t, ok)
	assert.Equal(t, 1, openCount)
}

func TestResolveReadWALBoundsTransientHistoricalOpenRetries(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRW}
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	openCount := 0
	roWAL := &roWALAdaptorImpl{
		roWALImpls:                   currentWAL,
		historicalWALFallbackTimeout: 20 * time.Millisecond,
		historicalWALOpener: func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			openCount++
			return nil, errors.New("temporary backend failure")
		},
	}
	roWAL.SetLogger(mlog.With())

	resolved, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	})
	require.NoError(t, err)
	assert.Same(t, currentWAL, resolved.wal)
	_, ok := resolved.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
	assert.True(t, ok)
	assert.GreaterOrEqual(t, openCount, 1)
}

func TestResolveReadWALFallsBackInsideStreamingNode(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRW}
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	roWAL := &roWALAdaptorImpl{
		roWALImpls: currentWAL,
	}
	roWAL.SetLogger(mlog.With())

	resolved, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	})
	require.NoError(t, err)
	assert.Same(t, currentWAL, resolved.wal)
	_, ok := resolved.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
	assert.True(t, ok)
}

func TestResolveReadWALKeepsCurrentBackend(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", Term: 10, AccessMode: types.AccessModeRO}
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	roWAL := &roWALAdaptorImpl{
		roWALImpls: currentWAL,
		historicalWALOpener: func(
			context.Context,
			message.WALName,
			types.PChannelInfo,
		) (walimpls.ROWALImpls, error) {
			t.Fatal("historical WAL opener should not be called")
			return nil, nil
		},
	}

	currentID := walimplstest.NewTestMessageID(1)
	policies := map[string]options.DeliverPolicy{
		"all":         options.DeliverPolicyAll(),
		"latest":      options.DeliverPolicyLatest(),
		"start-from":  options.DeliverPolicyStartFrom(currentID),
		"start-after": options.DeliverPolicyStartAfter(currentID),
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			resolved, err := roWAL.resolveReadWAL(context.Background(), wal.ReadOption{DeliverPolicy: policy})
			require.NoError(t, err)
			assert.Same(t, currentWAL, resolved.wal)
		})
	}
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
