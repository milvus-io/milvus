package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestAlterWALWaitsForSyncBeforeTakingAppendLock(t *testing.T) {
	impl := newTestInterceptor()
	impl.buildParam = &interceptors.InterceptorBuildParam{}
	impl.buildParam.TimeTickSyncMu.Lock()
	// Model Sync's sampling-to-publication interval. Its own WAL append must
	// remain able to acquire the PChannel read lock while AlterWAL waits.
	alter := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable()
	done := make(chan error, 1)
	go func() {
		_, err := impl.DoAppend(context.Background(), alter, func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, context.Canceled })
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("AlterWAL bypassed running Sync")
	case <-time.After(30 * time.Millisecond):
	}
	syncDone := make(chan error, 1)
	go func() {
		_, err := impl.DoAppend(context.Background(), message.CreateTestTimeTickSyncMessage(t, 1, 10, walimplstest.NewTestMessageID(0)), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(1), nil
		})
		syncDone <- err
	}()
	select {
	case err := <-syncDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("AlterWAL held append lock while waiting for Sync")
	}
	impl.buildParam.TimeTickSyncMu.Unlock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("AlterWAL failed to proceed after Sync")
	}
}
