package broadcaster

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRefreshPendingTargetAndAcceptance(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
	msg := message.NewRefreshExternalCollectionMessageBuilderV2().WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 100, JobId: 7}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: &schemapb.CollectionSchema{Version: 2, ExternalSource: "s3://new"}}).WithBroadcast([]string{"v1"}).MustBuildBroadcast().WithBroadcastID(9)
	bm := newBroadcastTaskManager(nil)
	defer bm.Close()
	task, _ := bm.getOrAddBroadcastTask(t.Context(), msg, 9, bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey()))
	bm.schemaTasks.update(task)
	target, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 7, target.RefreshJobID)
	require.EqualValues(t, 2, target.Schema.GetVersion())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = task.waitForResponse(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = task.waitForResponse(WithWaitForAcknowledgement(ctx))
	require.ErrorIs(t, err, context.Canceled)
	ack := mockey.Mock((*broadcastTask).BlockUntilAllAck).Return(nil).Build()
	defer ack.UnPatch()
	result := mockey.Mock((*broadcastTask).BroadcastResult).Return(msg, map[string]*types.AppendResult{"v1": {TimeTick: 10}}).Build()
	defer result.UnPatch()
	accepted, err := task.waitForResponse(WithWaitForAcknowledgement(t.Context()))
	require.NoError(t, err)
	require.EqualValues(t, 9, accepted.BroadcastID)
	select {
	case <-task.done:
		t.Fatal("ACK must not complete callback")
	default:
	}
	task.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE
	bm.schemaTasks.update(task)
	target, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Nil(t, target)
}
