package channel

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestChannelManager(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))

	ctx := context.Background()
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, errors.New("recover failure"))
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	m, err := RecoverChannelManager(ctx)
	assert.Nil(t, m)
	assert.Error(t, err)

	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel",
					Term: 1,
				},
				Node: &streamingpb.StreamingNodeInfo{
					ServerId: 1,
				},
			},
		}, nil
	})
	m, err = RecoverChannelManager(ctx)
	assert.NotNil(t, m)
	assert.NoError(t, err)

	// Test update non exist pchannel
	modified, err := m.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("non-exist-channel"): {
		Channel: types.PChannelInfo{
			Name:       "non-exist-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	assert.Nil(t, modified)
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newChannelID("non-exist-channel")})
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "non-exist-channel",
		Term: 2,
	}})
	assert.ErrorIs(t, err, ErrChannelNotExist)

	// Test success.
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pm []*streamingpb.PChannelMeta) error {
		if rand.Int31n(3) == 0 {
			return errors.New("save meta failure")
		}
		return nil
	})
	modified, err = m.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("test-channel"): {
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	assert.NotNil(t, modified)
	assert.NoError(t, err)
	assert.Len(t, modified, 1)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newChannelID("test-channel")})
	assert.NoError(t, err)

	nodeID, ok := m.GetLatestWALLocated(ctx, "test-channel")
	assert.True(t, ok)
	assert.NotZero(t, nodeID)

	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	assert.NoError(t, err)

	view := m.CurrentPChannelsView()
	assert.NotNil(t, view)
	assert.Equal(t, len(view.Channels), 1)
	channel, ok := view.Channels[newChannelID("test-channel")]
	assert.True(t, ok)
	assert.NotNil(t, channel)

	nodeID, ok = m.GetLatestWALLocated(ctx, "test-channel")
	assert.False(t, ok)
	assert.Zero(t, nodeID)
}

func TestStreamingEnableChecker(t *testing.T) {
	ctx := context.Background()
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	m, err := RecoverChannelManager(ctx, "test-channel")
	assert.NoError(t, err)

	assert.False(t, m.IsStreamingEnabledOnce())

	n := syncutil.NewAsyncTaskNotifier[struct{}]()
	m.RegisterStreamingEnabledNotifier(n)
	assert.NoError(t, n.Context().Err())

	go func() {
		defer n.Finish(struct{}{})
		<-n.Context().Done()
	}()

	err = m.MarkStreamingHasEnabled(ctx)
	assert.NoError(t, err)

	n2 := syncutil.NewAsyncTaskNotifier[struct{}]()
	m.RegisterStreamingEnabledNotifier(n2)
	assert.Error(t, n.Context().Err())
	assert.Error(t, n2.Context().Err())
}

func TestChannelManagerWatch(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel",
					Term: 1,
				},
				Node: &streamingpb.StreamingNodeInfo{
					ServerId: 1,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}, nil
	})
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	manager, err := RecoverChannelManager(context.Background())
	assert.NoError(t, err)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	called := make(chan struct{}, 1)
	go func() {
		defer close(done)
		err := manager.WatchAssignmentResult(ctx, func(param WatchChannelAssignmentsCallbackParam) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	manager.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("test-channel"): {
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	manager.AssignPChannelsDone(ctx, []ChannelID{newChannelID("test-channel")})

	<-called
	manager.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	<-called
	cancel()
	<-done
}

func newChannelID(name string) ChannelID {
	return ChannelID{
		Name: name,
	}
}
