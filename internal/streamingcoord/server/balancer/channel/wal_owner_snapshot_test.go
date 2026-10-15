package channel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func newWALSnapshotManager(t *testing.T, metas ...*streamingpb.PChannelMeta) (*ChannelManager, *mock_metastore.MockStreamingCoordCataLog) {
	t.Helper()
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager(nil)
	t.Cleanup(ResetStaticPChannelStatsManager)
	session := sessionutil.NewMockSession(t)
	session.EXPECT().GetRegisteredRevision().Return(int64(1)).Once()
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "wal"}, nil).Once()
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: 1}, nil).Once()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().ListPChannel(mock.Anything).Return(metas, nil).Once()
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(session))
	manager, err := RecoverChannelManager(context.Background())
	require.NoError(t, err)
	return manager, catalog
}

func walSnapshotMeta(name string, owner int64, state streamingpb.PChannelMetaState) *streamingpb.PChannelMeta {
	return &streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: name, Term: 1, AccessMode: streamingpb.PChannelAccessMode(types.AccessModeRW)},
		Node:    &streamingpb.StreamingNodeInfo{ServerId: owner},
		State:   state,
	}
}

func walSnapshotAssignment(owner int64) map[ChannelID]types.PChannelInfoAssigned {
	return map[ChannelID]types.PChannelInfoAssigned{
		{Name: "wal"}: {
			Channel: types.PChannelInfo{Name: "wal", AccessMode: types.AccessModeRW},
			Node:    types.StreamingNodeInfo{ServerID: owner},
		},
	}
}

func TestChannelManagerWALSnapshotRecoveryAndTransitions(t *testing.T) {
	manager, catalog := newWALSnapshotManager(t,
		walSnapshotMeta("wal", 1, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED),
		walSnapshotMeta("assigning", 2, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING),
		walSnapshotMeta("unavailable", 3, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE),
		walSnapshotMeta("uninitialized", 0, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED),
	)
	ctx := context.Background()
	for channel, expected := range map[string]int64{"wal": 1, "assigning": 2} {
		owner, ok := manager.GetLatestWALLocated(ctx, channel)
		require.True(t, ok)
		require.Equal(t, expected, owner)
	}
	for _, channel := range []string{"unavailable", "uninitialized", "missing"} {
		owner, ok := manager.GetLatestWALLocated(ctx, channel)
		require.False(t, ok)
		require.Zero(t, owner)
	}
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	updates, err := manager.AssignPChannels(ctx, walSnapshotAssignment(4))
	require.NoError(t, err)
	require.False(t, updates[ChannelID{Name: "wal"}].IsAssigned())
	owner, ok := manager.GetLatestWALLocated(ctx, "wal")
	require.True(t, ok, "ASSIGNING must expose the newly committed owner before the watcher publishes ASSIGNED")
	require.EqualValues(t, 4, owner)
	require.NoError(t, manager.AssignPChannelsDone(ctx, []ChannelID{{Name: "wal"}}))
	owner, ok = manager.GetLatestWALLocated(ctx, "wal")
	require.True(t, ok)
	require.EqualValues(t, 4, owner)
	require.NoError(t, manager.MarkAsUnavailable(ctx, []types.PChannelInfo{updates[ChannelID{Name: "wal"}].ChannelInfo()}))
	_, ok = manager.GetLatestWALLocated(ctx, "wal")
	require.False(t, ok)
	require.NoError(t, manager.AddPChannels(ctx, []string{"dynamic"}))
	_, ok = manager.GetLatestWALLocated(ctx, "dynamic")
	require.False(t, ok, "adding an uninitialized channel must not invent an owner")
}

func TestChannelManagerWALSnapshotDoesNotWaitForPersistence(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		name := "commit"
		if failWrite {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			manager, catalog := newWALSnapshotManager(t, walSnapshotMeta("wal", 1, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED))
			entered, release, writerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			lookupDone := make(chan struct{})
			var unblock sync.Once
			var writeErr error
			var lookupStarted bool
			t.Cleanup(func() {
				unblock.Do(func() { close(release) })
				<-writerDone
				if lookupStarted {
					<-lookupDone
				}
			})
			persistErr := errors.New("catalog unavailable")
			catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, []*streamingpb.PChannelMeta) error {
				close(entered)
				<-release
				if failWrite {
					return persistErr
				}
				return nil
			}).Once()
			go func() {
				_, writeErr = manager.AssignPChannels(context.Background(), walSnapshotAssignment(2))
				close(writerDone)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("assignment did not reach catalog persistence")
			}
			var owner int64
			var ok bool
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lookupStarted = true
			go func() {
				owner, ok = manager.GetLatestWALLocated(ctx, "wal")
				close(lookupDone)
			}()
			cancel()
			select {
			case <-lookupDone:
			case <-time.After(time.Second):
				t.Fatal("WAL snapshot lookup waited for the catalog write despite canceled context")
			}
			require.True(t, ok)
			require.EqualValues(t, 1, owner, "uncommitted reassignment must remain invisible")
			unblock.Do(func() { close(release) })
			<-writerDone
			owner, ok = manager.GetLatestWALLocated(context.Background(), "wal")
			require.True(t, ok)
			if failWrite {
				require.ErrorIs(t, writeErr, persistErr)
				require.EqualValues(t, 1, owner, "failed persistence must not publish the new owner")
			} else {
				require.NoError(t, writeErr)
				require.EqualValues(t, 2, owner)
			}
		})
	}
}

func TestChannelManagerWALSnapshotConcurrentReassignment(t *testing.T) {
	manager, catalog := newWALSnapshotManager(t, walSnapshotMeta("wal", 1, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED))
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	stop, started, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	invalid := make(chan struct{}, 1)
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	go func() {
		defer close(done)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				owner, ok := manager.GetLatestWALLocated(context.Background(), "wal")
				if !ok || (owner != 1 && owner != 2) {
					invalid <- struct{}{}
					return
				}
			}
		}
	}()
	<-started
	for i := 0; i < 100; i++ {
		_, err := manager.AssignPChannels(context.Background(), walSnapshotAssignment(int64(i%2+1)))
		require.NoError(t, err)
	}
	select {
	case <-invalid:
		t.Fatal("reader observed an incomplete assignment snapshot")
	default:
	}
}
