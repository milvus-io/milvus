package broadcaster

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func schemaTestMessage(collection int64, id uint64) message.BroadcastMutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
		CollectionId: collection, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
	}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Version: 2}}}).
		WithBroadcast([]string{"p" + funcutil.ControlChannelSuffix, "v1"}).MustBuildBroadcast().WithBroadcastID(id)
}

type schemaTestCatalog struct {
	metastore.StreamingCoordCataLog
}

func (*schemaTestCatalog) SaveBroadcastTask(context.Context, uint64, *streamingpb.BroadcastTask) error {
	panic("unpatched")
}

func (*schemaTestCatalog) ListBroadcastTask(context.Context) ([]*streamingpb.BroadcastTask, error) {
	panic("unpatched")
}

func TestSchemaTargetPersistenceBoundary(t *testing.T) {
	mockey.PatchConvey("only saved schema targets are visible", t, func() {
		paramtable.Init()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		bm := newBroadcastTaskManager(nil)
		t.Cleanup(bm.Close)
		const collectionID = 100
		const broadcastID = 1
		msg := schemaTestMessage(collectionID, broadcastID)
		task, created := bm.getOrAddBroadcastTask(t.Context(), msg, broadcastID, bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey()))
		require.True(t, created)
		check := func(expectedVersion int32) {
			t.Helper()
			target, err := bm.GetPendingSchemaTarget(collectionID)
			require.NoError(t, err)
			if expectedVersion == 0 {
				require.Nil(t, target)
				return
			}
			require.NotNil(t, target)
			require.EqualValues(t, broadcastID, target.BroadcastID)
			require.Equal(t, expectedVersion, target.Schema.GetVersion())
		}
		check(0)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		failure := mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(context.Canceled).Build()
		require.ErrorIs(t, task.InitializeRecovery(ctx), context.Canceled)
		check(0)
		failure.UnPatch()
		saved := mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		require.NoError(t, task.InitializeRecovery(t.Context()))
		check(2)

		saved.UnPatch()
		failure = mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(context.Canceled).Build()
		require.ErrorIs(t, task.MarkAckCallbackDone(ctx), context.Canceled)
		check(2)
		failure.UnPatch()
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		require.NoError(t, task.MarkAckCallbackDone(t.Context()))
		check(0)
	})
}
