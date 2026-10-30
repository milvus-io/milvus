package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCreatePartitionCallbackWaitsBeforeExpiringCaches(t *testing.T) {
	mockey.PatchConvey("partition publication precedes coordinator handoff and cache expiry", t, func() {
		mockey.Mock(qvresource.Resource).Return(&qvresource.Resources{}).Build()
		var calls []string
		var handoffErr error
		mockey.Mock((*MetaTable).AddPartition).To(func(_ *MetaTable, _ context.Context, partition *model.Partition) error {
			require.Equal(t, int64(2), partition.PartitionID)
			calls = append(calls, "meta")
			return nil
		}).Build()
		mockey.Mock((*qvresource.Resources).AddPartition).To(func(_ *qvresource.Resources, _ context.Context, collectionID, partitionID int64) error {
			require.Equal(t, int64(1), collectionID)
			require.Equal(t, int64(2), partitionID)
			calls = append(calls, "handoff")
			return handoffErr
		}).Build()
		mockey.Mock((*DDLCallback).ExpireCaches).To(func(_ *DDLCallback, _ context.Context, _ any) error { calls = append(calls, "expire"); return nil }).Build()
		ctrl := funcutil.GetControlChannel("test")
		msg := message.NewCreatePartitionMessageBuilderV1().WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 1, PartitionId: 2}).WithBody(&message.CreatePartitionRequest{PartitionName: "p"}).WithBroadcast([]string{ctrl}).MustBuildBroadcast()
		result := message.BroadcastResultCreatePartitionMessageV1{Message: message.MustAsBroadcastCreatePartitionMessageV1(msg), Results: map[string]*message.AppendResult{ctrl: {TimeTick: 10}}}
		callback := &DDLCallback{Core: &Core{meta: &MetaTable{}, broker: &ServerBroker{}}}
		handoffErr = merr.WrapErrServiceUnavailableMsg("waiting for QueryView")
		require.ErrorIs(t, callback.createPartitionV1AckCallback(t.Context(), result), merr.ErrServiceUnavailable)
		require.Equal(t, []string{"meta", "handoff"}, calls)
		calls = nil
		handoffErr = nil
		require.NoError(t, callback.createPartitionV1AckCallback(t.Context(), result))
		require.Equal(t, []string{"meta", "handoff", "expire"}, calls)
	})
}

func mockQueryViewDDLResource(t *testing.T) {
	t.Helper()
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(qvresource.Resource).Return(&qvresource.Resources{}).Build(),
		mockey.Mock((*qvresource.Resources).AddPartition).Return(nil).Build(),
		mockey.Mock((*qvresource.Resources).Refresh).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
}
