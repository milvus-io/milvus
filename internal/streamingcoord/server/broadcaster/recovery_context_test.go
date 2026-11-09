package broadcaster

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRecoveryContextDoesNotOwnBroadcasterLifetime(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()
	t.Cleanup(registry.ResetRegistration)
	type recoveryKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), recoveryKey{}, "recovery-only"), time.Hour)
	defer cancel()
	task := createTruncateBroadcastTask(109, []string{"v1", "v2"})
	// A recovered replicated task waits for incoming ACKs without appending WAL.
	task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
	meta := catalog.NewCataLog(nil)
	listPatch := mockey.Mock(mockey.GetMethod(meta, "ListBroadcastTask")).To(func(got context.Context) ([]*streamingpb.BroadcastTask, error) {
		require.Same(t, ctx, got, "catalog recovery must use the caller context")
		return []*streamingpb.BroadcastTask{task}, nil
	}).Build()
	t.Cleanup(func() { listPatch.UnPatch() })
	savePatch := mockey.Mock(mockey.GetMethod(meta, "SaveBroadcastTask")).Return(nil).Build()
	t.Cleanup(func() { savePatch.UnPatch() })
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	bc, err := RecoverBroadcaster(ctx)
	require.NoError(t, err)
	bm := bc.(*broadcastTaskManager)
	t.Cleanup(func() {
		bc.Close()
		require.ErrorIs(t, bm.ctx.Err(), context.Canceled, "Close still owns cancellation")
	})
	cancel()
	require.NoError(t, bm.ctx.Err())
	_, hasDeadline := bm.ctx.Deadline()
	require.False(t, hasDeadline)
	require.Nil(t, bm.ctx.Value(recoveryKey{}))
	called := false
	registry.RegisterTruncateCollectionV2AckOnceCallback(func(ctx context.Context, _ message.AckResultTruncateCollectionMessageV2) error {
		called = true
		return ctx.Err()
	})
	require.NoError(t, bc.Ack(context.Background(), newTruncateCollectionAckMessage(109, "v1")))
	require.True(t, called, "canceling recovery must not prevent subsequent ACKs")
}

func TestCancelContextDuringBroadcasterRecovery(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	meta := catalog.NewCataLog(nil)
	patch := mockey.Mock(mockey.GetMethod(meta, "ListBroadcastTask")).To(func(got context.Context) ([]*streamingpb.BroadcastTask, error) {
		close(entered)
		<-got.Done()
		return nil, got.Err()
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	type recoveryResult struct {
		bc  Broadcaster
		err error
	}
	done := make(chan recoveryResult, 1)
	go func() {
		bc, err := RecoverBroadcaster(ctx)
		done <- recoveryResult{bc, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not enter the catalog read")
	}
	cancel()
	select {
	case result := <-done:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.bc)
	case <-time.After(5 * time.Second):
		t.Fatal("canceling recovery did not interrupt the catalog read")
	}
}
