package manager

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	streamingresolver "github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryNodePublicationReplayAndUpdates(t *testing.T) {
	updates := make(chan streamingresolver.VersionedState)
	applied := make(chan struct{})
	patch := mockey.Mock((*fakeResolver).Watch).To(func(_ *fakeResolver, ctx context.Context, cb func(streamingresolver.VersionedState) error) error {
		for {
			select {
			case state := <-updates:
				if err := cb(state); err != nil {
					return err
				}
				applied <- struct{}{}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}).Build()
	defer patch.UnPatch()
	m := &managerClientImpl{lifetime: typeutil.NewLifetime(), stopped: make(chan struct{}), rb: &fakeResolverBuilder{resolver: &fakeResolver{}}, service: &fakeViewSyncService{}}
	defer m.Close()
	// Cancellation before discovery must not publish an empty ready snapshot.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := m.RegisterQueryNodeListener(canceled, func(int64, *NodeInfo) { t.Error("unexpected replay") })
	require.ErrorIs(t, err, context.Canceled)
	updates <- newQueryNodeVersionedState(1, map[int64]queryNodeSessionInfo{10: {labels: map[string]string{sessionutil.LabelResourceGroup: "rg"}}})
	<-applied
	type event struct {
		id   int64
		info *NodeInfo
	}
	events := make(chan event, 10)
	stop, err := m.RegisterQueryNodeListener(t.Context(), func(id int64, info *NodeInfo) { events <- event{id, info} })
	require.NoError(t, err)
	first := <-events
	require.Equal(t, int64(10), first.id)
	require.Equal(t, "rg", first.info.ServerLabels[sessionutil.LabelResourceGroup])
	updates <- newQueryNodeVersionedState(2, map[int64]queryNodeSessionInfo{10: {stopping: true}})
	<-applied
	require.True(t, (<-events).info.Stopping)
	require.False(t, first.info.Stopping, "published facts must remain immutable")
	updates <- newQueryNodeVersionedState(2, map[int64]queryNodeSessionInfo{10: {stopping: true}})
	<-applied
	require.Empty(t, events, "identical discovery must not republish node facts")
	updates <- newQueryNodeVersionedState(3, nil)
	<-applied
	require.Nil(t, (<-events).info)
	stop()
	updates <- newQueryNodeVersionedState(4, map[int64]queryNodeSessionInfo{20: {}})
	<-applied
	require.Empty(t, events)
}

func TestQueryNodePublicationWaitsForInitialDiscovery(t *testing.T) {
	entered := make(chan struct{})
	patch := mockey.Mock((*fakeResolver).Watch).To(func(_ *fakeResolver, ctx context.Context, _ func(streamingresolver.VersionedState) error) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}).Build()
	defer patch.UnPatch()
	m := &managerClientImpl{lifetime: typeutil.NewLifetime(), stopped: make(chan struct{}), rb: &fakeResolverBuilder{resolver: &fakeResolver{}}, service: &fakeViewSyncService{}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.RegisterQueryNodeListener(ctx, func(int64, *NodeInfo) { t.Error("uninitialized discovery") })
		done <- err
	}()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("registration returned before discovery: %v", err)
	default:
	}
	m.Close()
	_, closedErr := m.RegisterQueryNodeListener(t.Context(), func(int64, *NodeInfo) { t.Error("closed manager replayed discovery") })
	require.Error(t, closedErr)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("registration did not stop")
	}
}
