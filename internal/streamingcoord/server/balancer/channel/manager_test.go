package channel

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestChannelManager(t *testing.T) {
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))

	ctx := context.Background()
	// Test recover failure.
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, errors.New("recover failure"))
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

	// Test save meta failure
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(errors.New("save meta failure"))
	modified, err := m.AssignPChannels(ctx, map[ChannelID]types.StreamingNodeInfo{newRWChannelID("test-channel"): {ServerID: 2}})
	assert.Nil(t, modified)
	assert.Error(t, err)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newRWChannelID("test-channel")})
	assert.Error(t, err)
	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	assert.Error(t, err)

	// Test update non exist pchannel
	modified, err = m.AssignPChannels(ctx, map[ChannelID]types.StreamingNodeInfo{newRWChannelID("non-exist-channel"): {ServerID: 2}})
	assert.Nil(t, modified)
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newRWChannelID("non-exist-channel")})
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "non-exist-channel",
		Term: 2,
	}})
	assert.ErrorIs(t, err, ErrChannelNotExist)

	// Test success.
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	modified, err = m.AssignPChannels(ctx, map[ChannelID]types.StreamingNodeInfo{newRWChannelID("test-channel"): {ServerID: 2}})
	assert.NotNil(t, modified)
	assert.NoError(t, err)
	assert.Len(t, modified, 1)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newRWChannelID("test-channel")})
	assert.NoError(t, err)
	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	assert.NoError(t, err)

	view := m.CurrentPChannelsView()
	assert.NotNil(t, view)
	assert.Equal(t, view.Count(), 1)
	channel, ok := view.GetChannel(newRWChannelID("test-channel"))
	assert.True(t, ok)
	assert.NotNil(t, channel)
}

func TestChannelManagerWatch(t *testing.T) {
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))

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

	manager, err := RecoverChannelManager(context.Background())
	assert.NoError(t, err)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	called := make(chan struct{}, 1)
	go func() {
		defer close(done)
		err := manager.WatchAssignmentResult(ctx, func(version typeutil.VersionInt64Pair, assignments []types.PChannelInfoAssigned) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	manager.AssignPChannels(ctx, map[ChannelID]types.StreamingNodeInfo{newRWChannelID("test-channel"): {ServerID: 2}})
	manager.AssignPChannelsDone(ctx, []ChannelID{newRWChannelID("test-channel")})

	<-called
	manager.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	<-called
	cancel()
	<-done
}

func newRWChannelID(name string) ChannelID {
	return ChannelID{
		Name:      name,
		ReplicaID: 0,
	}
}
