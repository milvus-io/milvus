package replica

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSnapshotChangesAndReplacement(t *testing.T) {
	p := NewPublisher(&streamingpb.WALCheckpoint{TimeTick: 5, Term: 7})
	topology := &streamingpb.WALReplicaTopology{WriterReplicaId: 1, Members: []*streamingpb.WALReplicaPlacement{{WalReplicaId: 1}, {WalReplicaId: 2}}}
	p.SetMembers(topology, false)
	_, _, err := p.Subscribe(3)
	require.ErrorIs(t, err, io.EOF)
	key := qviews.QueryViewKey{ShardID: qviews.ShardID{VChannel: "p", ReplicaID: 20}, QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
	p.PublishView(key, viewpb.QueryViewState_QueryViewStatePreparing)
	first, snapshot, err := p.Subscribe(2)
	require.NoError(t, err)
	require.Len(t, snapshot.QueryViews, 1)
	require.EqualValues(t, 5, snapshot.Checkpoint.TimeTick)
	require.Zero(t, snapshot.Checkpoint.Term)
	const ready = viewpb.QueryViewState_QueryViewStateReady
	report := &streamingpb.WALQueryViewReport{View: snapshot.QueryViews[0].View, LocalState: ready}
	first.Report(report)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, p.WaitReports(ctx, key, ready), context.Canceled)
	p.SetMembers(topology, true)
	require.NoError(t, p.WaitReports(t.Context(), key, ready))
	second, snapshot, err := p.Subscribe(2)
	require.NoError(t, err)
	require.Len(t, snapshot.QueryViews, 1)
	first.Report(report)
	require.ErrorIs(t, p.WaitReports(ctx, key, ready), context.Canceled)
	first.Close()
	_, err = first.Next(t.Context())
	require.ErrorIs(t, err, io.EOF)
	p.PublishCheckpoint(&streamingpb.WALCheckpoint{TimeTick: 10})
	p.PublishCheckpoint(&streamingpb.WALCheckpoint{TimeTick: 9})
	p.PublishView(key, viewpb.QueryViewState_QueryViewStateUp)
	p.PublishView(key, viewpb.QueryViewState_QueryViewStateDown)
	delta, err := second.Next(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 10, delta.Checkpoint.TimeTick)
	require.Len(t, delta.QueryViews, 1)
	require.Equal(t, viewpb.QueryViewState_QueryViewStateDown, delta.QueryViews[0].State)
	p.PublishView(key, viewpb.QueryViewState_QueryViewStateDropped)
	delta, err = second.Next(t.Context())
	require.NoError(t, err)
	require.Nil(t, delta.Checkpoint)
	require.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, delta.QueryViews[0].State)
	_, err = second.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	wait := make(chan error, 1)
	go func() { _, err := second.Next(t.Context()); wait <- err }()
	p.SetMembers(nil, false)
	select {
	case err := <-wait:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("retired reader was not released")
	}
	p.SetMembers(topology, true)
	third, snapshot, err := p.Subscribe(2)
	require.NoError(t, err)
	require.Empty(t, snapshot.QueryViews)
	require.EqualValues(t, 10, snapshot.Checkpoint.TimeTick)
	third.Close()
	fourth, _, err := p.Subscribe(2)
	require.NoError(t, err)
	p.Close()
	_, err = fourth.Next(t.Context())
	require.ErrorIs(t, err, io.EOF)
	require.ErrorIs(t, p.WaitReports(t.Context(), key, ready), io.EOF)
	p.PublishView(key, ready)
	p.PublishCheckpoint(nil)
}
