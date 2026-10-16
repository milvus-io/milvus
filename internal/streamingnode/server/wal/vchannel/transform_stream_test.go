package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
)

func TestTransformStreamsHaveIndependentLifetimes(t *testing.T) {
	manager := &PChannelRecoveryManager{pchannel: "p0", closeCh: make(chan struct{}), config: PChannelManagerConfig{SummaryManager: walsummary.NewManager(walsummary.ManagerConfig{})}}
	first, err := manager.AcquireStream(context.Background(), "p0")
	require.NoError(t, err)
	second, err := manager.AcquireStream(context.Background(), "p0")
	require.NoError(t, err)
	defer second.Close()
	require.NoError(t, first.Close())
	select {
	case <-second.Done():
		t.Fatal("closing one stream closed another")
	default:
	}
	_, err = manager.AcquireStream(context.Background(), "other")
	require.ErrorIs(t, err, wal.ErrTransformLogVChannelUnavailable)
	close(manager.closeCh)
	select {
	case <-second.Done():
	case <-time.After(time.Second):
		t.Fatal("pchannel shutdown did not close its stream")
	}
	_, err = manager.AcquireStream(context.Background(), "p0")
	require.ErrorIs(t, err, wal.ErrTransformLogVChannelUnavailable)
}

func TestTransformStreamFollowsCallerCancellation(t *testing.T) {
	manager := &PChannelRecoveryManager{pchannel: "p0", closeCh: make(chan struct{}), config: PChannelManagerConfig{SummaryManager: walsummary.NewManager(walsummary.ManagerConfig{})}}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := manager.AcquireStream(ctx, "p0")
	require.NoError(t, err)
	defer stream.Close()
	cancel()
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not close its stream")
	}
	_, err = manager.AcquireStream(ctx, "p0")
	require.ErrorIs(t, err, context.Canceled)
}
