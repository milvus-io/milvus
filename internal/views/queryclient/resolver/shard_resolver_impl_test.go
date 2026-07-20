package resolver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestShardResolverImplResolvesShardReplicas(t *testing.T) {
	const collectionID int64 = 100
	primaryShard := qviews.ShardID{ReplicaID: 10, VChannel: funcutil.GetVirtualChannel("p0", collectionID, 0)}
	secondaryShard := qviews.ShardID{ReplicaID: 20, VChannel: funcutil.GetVirtualChannel("p0", collectionID, 0)}
	otherShard := qviews.ShardID{ReplicaID: 20, VChannel: funcutil.GetVirtualChannel("p1", collectionID, 1)}

	resolver := NewShardResolverImpl(&staticAssignmentWatcher{
		assignments: []*types.VersionedStreamingNodeAssignments{
			{
				StreamingVersion: &streamingpb.StreamingVersion{},
				Version:          typeutil.VersionInt64Pair{Global: 1, Local: 1},
				Assignments: map[int64]types.StreamingNodeAssignment{
					1: {
						NodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
						Channels: map[string]types.PChannelInfo{
							"p0": {Name: "p0", Term: 1, AccessMode: types.AccessModeRW},
						},
						SecondaryChannels: map[string]types.PChannelInfo{},
						ShardAssignment: types.ShardAssignmentInfo{
							PChannelAssignments: []types.PChannelShardAssignment{
								{
									PChannel: "p0",
									Entries: []types.ShardAssignmentEntry{
										{CollectionID: collectionID, ShardIndex: 0, ReplicaID: primaryShard.ReplicaID},
									},
								},
							},
						},
					},
					2: {
						NodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"},
						Channels: map[string]types.PChannelInfo{
							"p1": {Name: "p1", Term: 1, AccessMode: types.AccessModeRW},
						},
						SecondaryChannels: map[string]types.PChannelInfo{
							"p0": {Name: "p0", Term: 2, AccessMode: types.AccessModeRO},
						},
						ShardAssignment: types.ShardAssignmentInfo{
							PChannelAssignments: []types.PChannelShardAssignment{
								{
									PChannel: "p0",
									Entries: []types.ShardAssignmentEntry{
										{CollectionID: collectionID, ShardIndex: 0, ReplicaID: secondaryShard.ReplicaID},
									},
								},
								{
									PChannel: "p1",
									Entries: []types.ShardAssignmentEntry{
										{CollectionID: collectionID, ShardIndex: 1, ReplicaID: otherShard.ReplicaID},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	defer resolver.Close()

	ctx := context.Background()
	vchannels, err := resolver.ResolveVChannels(ctx, collectionID)
	require.NoError(t, err)
	assert.Equal(t, []string{primaryShard.VChannel, otherShard.VChannel}, vchannels)

	replicas, err := resolver.ResolveShard(ctx, collectionID, primaryShard.VChannel)
	require.NoError(t, err)
	assert.Equal(t, primaryShard.VChannel, replicas.VChannel)
	assert.Equal(t, primaryShard, replicas.PrimaryShardID)
	assert.ElementsMatch(t, []qviews.ShardID{primaryShard, secondaryShard}, replicas.ShardIDs)
}

func TestShardResolverImplIgnoresShardAssignmentsForUnknownPChannel(t *testing.T) {
	resolver := NewShardResolverImpl(&staticAssignmentWatcher{
		assignments: []*types.VersionedStreamingNodeAssignments{
			{
				Version: typeutil.VersionInt64Pair{Global: 1, Local: 1},
				Assignments: map[int64]types.StreamingNodeAssignment{
					1: {
						NodeInfo:          types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
						Channels:          map[string]types.PChannelInfo{"p0": {Name: "p0", Term: 1, AccessMode: types.AccessModeRW}},
						SecondaryChannels: map[string]types.PChannelInfo{},
						ShardAssignment: types.ShardAssignmentInfo{
							PChannelAssignments: []types.PChannelShardAssignment{
								{
									PChannel: "missing",
									Entries: []types.ShardAssignmentEntry{
										{CollectionID: 100, ShardIndex: 0, ReplicaID: 10},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	defer resolver.Close()

	vchannels, err := resolver.ResolveVChannels(context.Background(), 100)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	assert.Nil(t, vchannels)
}

func TestShardResolverImplReturnsNotLoadedForUnassignedCollection(t *testing.T) {
	resolver := NewShardResolverImpl(&staticAssignmentWatcher{
		assignments: []*types.VersionedStreamingNodeAssignments{
			versionedAssignment(1, "localhost:1", "p0", 100, 0, 10),
		},
	})
	defer resolver.Close()

	vchannels, err := resolver.ResolveVChannels(context.Background(), 200)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	assert.Nil(t, vchannels)
}

func TestShardResolverImplReplacesCacheOnAssignmentUpdate(t *testing.T) {
	const collectionID int64 = 100
	oldVChannel := funcutil.GetVirtualChannel("p0", collectionID, 0)
	newVChannel := funcutil.GetVirtualChannel("p1", collectionID, 1)
	resolver := NewShardResolverImpl(&staticAssignmentWatcher{
		assignments: []*types.VersionedStreamingNodeAssignments{
			versionedAssignment(1, "localhost:1", "p0", collectionID, 0, 10),
			versionedAssignment(1, "localhost:1", "p1", collectionID, 1, 11),
		},
	})
	defer resolver.Close()

	vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.NoError(t, err)
	assert.Equal(t, []string{newVChannel}, vchannels)

	_, err = resolver.ResolveShard(context.Background(), collectionID, oldVChannel)
	assert.Error(t, err)
}

func TestShardResolverImplBlocksUntilFirstServiceDiscoverySuccess(t *testing.T) {
	const collectionID int64 = 100
	vchannel := funcutil.GetVirtualChannel("p0", collectionID, 0)
	watcher := newWaitableAssignmentWatcher(versionedAssignment(1, "localhost:1", "p0", collectionID, 0, 10))
	resolver := NewShardResolverImpl(watcher)
	defer resolver.Close()

	resultCh := make(chan []string, 1)
	errCh := make(chan error, 1)
	go func() {
		vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- vchannels
	}()

	select {
	case result := <-resultCh:
		t.Fatalf("ResolveVChannels returned before service discovery was ready: %v", result)
	case err := <-errCh:
		t.Fatalf("ResolveVChannels failed before service discovery was ready: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	watcher.ready()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case vchannels := <-resultCh:
		assert.Equal(t, []string{vchannel}, vchannels)
	case <-time.After(time.Second):
		t.Fatal("ResolveVChannels did not return after service discovery became ready")
	}
}

func TestShardResolverImplReturnsContextErrorWhileWaitingForReady(t *testing.T) {
	watcher := newWaitableAssignmentWatcher(versionedAssignment(1, "localhost:1", "p0", 100, 0, 10))
	resolver := NewShardResolverImpl(watcher)
	defer resolver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := resolver.ResolveVChannels(ctx, 100)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

type staticAssignmentWatcher struct {
	assignments []*types.VersionedStreamingNodeAssignments
}

func (w *staticAssignmentWatcher) AssignmentDiscover(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
	for _, assignment := range w.assignments {
		if err := cb(assignment); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

func (w *staticAssignmentWatcher) ReportAssignmentError(ctx context.Context, pchannel types.PChannelInfo, err error) error {
	return nil
}

type waitableAssignmentWatcher struct {
	assignment *types.VersionedStreamingNodeAssignments
	readyCh    chan struct{}
}

func newWaitableAssignmentWatcher(assignment *types.VersionedStreamingNodeAssignments) *waitableAssignmentWatcher {
	return &waitableAssignmentWatcher{
		assignment: assignment,
		readyCh:    make(chan struct{}),
	}
}

func (w *waitableAssignmentWatcher) ready() {
	close(w.readyCh)
}

func (w *waitableAssignmentWatcher) AssignmentDiscover(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
	select {
	case <-w.readyCh:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if err := cb(w.assignment); err != nil {
		return err
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

func (w *waitableAssignmentWatcher) ReportAssignmentError(ctx context.Context, pchannel types.PChannelInfo, err error) error {
	return nil
}

func versionedAssignment(
	serverID int64,
	address string,
	pchannel string,
	collectionID int64,
	shardIndex int32,
	replicaID int64,
) *types.VersionedStreamingNodeAssignments {
	return &types.VersionedStreamingNodeAssignments{
		Version: typeutil.VersionInt64Pair{Global: serverID, Local: int64(shardIndex)},
		Assignments: map[int64]types.StreamingNodeAssignment{
			serverID: {
				NodeInfo: types.StreamingNodeInfo{ServerID: serverID, Address: address},
				Channels: map[string]types.PChannelInfo{
					pchannel: {Name: pchannel, Term: 1, AccessMode: types.AccessModeRW},
				},
				SecondaryChannels: map[string]types.PChannelInfo{},
				ShardAssignment: types.ShardAssignmentInfo{
					PChannelAssignments: []types.PChannelShardAssignment{
						{
							PChannel: pchannel,
							Entries: []types.ShardAssignmentEntry{
								{CollectionID: collectionID, ShardIndex: shardIndex, ReplicaID: replicaID},
							},
						},
					},
				},
			},
		},
	}
}
