//go:build dynamic && test

package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestSplitTargetGateRequiresLocalPublicationProof(t *testing.T) {
	mockey.PatchConvey("gate stays closed across transient failures, opens monotonically", t, func() {
		impl := &shardInterceptor{}
		impl.addSplitTargetGate("p_1v1", 1, "s_1v0")
		msg := message.NewDeleteMessageBuilderV1().WithVChannel("p_1v1").WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).WithBody(&msgpb.DeleteRequest{}).MustBuildMutable()
		allowed := false
		var failure error
		calls := 0
		patch := mockey.Mock(querySplitTargetAdmission).To(func(context.Context, int64, string, string) (bool, error) { calls++; return allowed, failure }).Build()
		defer patch.UnPatch()
		require.True(t, status.AsStreamingError(impl.checkSplitTargetAdmission(t.Context(), msg)).IsShardFenced())
		failure = errors.New("coordinator restarting")
		allowed = true
		require.True(t, status.AsStreamingError(impl.checkSplitTargetAdmission(t.Context(), msg)).IsShardFenced())
		failure = nil
		require.NoError(t, impl.checkSplitTargetAdmission(t.Context(), msg))
		require.Equal(t, 3, calls)
		allowed = false
		require.NoError(t, impl.checkSplitTargetAdmission(t.Context(), msg))
		require.Equal(t, 3, calls)
		// Rebuilding from durable target origin starts closed again.
		reopened := &shardInterceptor{}
		reopened.addSplitTargetGate("p_1v1", 1, "s_1v0")
		require.True(t, status.AsStreamingError(reopened.checkSplitTargetAdmission(t.Context(), msg)).IsShardFenced())
		before := calls
		msg.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "primary", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(0), TimeTick: 100, VChannel: msg.VChannel()})
		require.NoError(t, reopened.checkSplitTargetAdmission(t.Context(), msg), "replicated DML never waits for local read preparation")
		require.Equal(t, before, calls)
	})
}
