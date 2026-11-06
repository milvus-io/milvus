package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func envelopeTestUpsert() message.MutableMessage {
	return message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}}}).
		WithBody(&message.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}, RowIds: []int64{7}, RequestRowMapping: &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{0}}}).
		MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID()
}

func TestUpsertEnvelopeAdmissionBeforeInterceptors(t *testing.T) {
	for _, mode := range []string{"local", "idempotency duplicate", "replicated duplicate"} {
		t.Run(mode, func(t *testing.T) {
			entered := &countingAppendInterceptor{}
			persisted := 0
			w := newFenceTestWAL(t, newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
				extra := utility.GetExtraAppendResult(ctx)
				extra.TimeTick = msg.TimeTick()
				extra.LastConfirmedMessageID = walimplstest.NewTestMessageID(0)
				persisted++
				return walimplstest.NewTestMessageID(1), nil
			}), entered)
			gate := &paramtable.Get().StreamingCfg.EnableUpsertMessage
			old := gate.SwapTempValue("true")
			defer gate.SwapTempValue(old)
			build := envelopeTestUpsert
			if mode == "idempotency duplicate" {
				interceptor := idempotency.NewInterceptorBuilder().Build(nil)
				t.Cleanup(interceptor.Close)
				w.interceptorBuildResult.Interceptor = interceptors.NewChainedInterceptor(entered, interceptor)
				build = func() message.MutableMessage {
					msg := envelopeTestUpsert()
					msg.Properties().ToRawMap()["_ik"] = "same-key"
					return msg
				}
			}
			if mode == "replicated duplicate" {
				manager, err := replicates.RecoverReplicateManager(&replicates.ReplicateManagerRecoverParam{
					ChannelInfo: w.Channel(), CurrentClusterID: "target", InitialRecoverSnapshot: &recovery.RecoverySnapshot{},
				})
				require.NoError(t, err)
				config := &commonpb.ReplicateConfiguration{
					Clusters:             []*commonpb.MilvusCluster{{ClusterId: "source", Pchannels: []string{"source-pchannel"}}, {ClusterId: "target", Pchannels: []string{w.Channel().Name}}},
					CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "source", TargetClusterId: "target"}},
				}
				require.NoError(t, manager.SwitchReplicateMode(t.Context(), message.MustAsMutableAlterReplicateConfigMessageV2(message.NewAlterReplicateConfigMessageBuilderV2().WithAllVChannel().WithHeader(&message.AlterReplicateConfigMessageHeader{ReplicateConfiguration: config}).WithBody(&message.AlterReplicateConfigMessageBody{}).MustBuildMutable())))
				interceptor := replicate.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{ReplicateManager: manager})
				t.Cleanup(interceptor.Close)
				w.interceptorBuildResult.Interceptor = interceptors.NewChainedInterceptor(entered, interceptor)
				build = func() message.MutableMessage {
					return envelopeTestUpsert().WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", VChannel: "source_1v0", MessageID: walimplstest.NewTestMessageID(10), LastConfirmedMessageID: walimplstest.NewTestMessageID(9), TimeTick: 10})
				}
			}
			first, err := w.Append(t.Context(), build())
			require.NoError(t, err)
			require.NotNil(t, first)
			if mode != "local" {
				duplicate, err := w.Append(t.Context(), build())
				if mode == "idempotency duplicate" {
					require.NoError(t, err)
					require.True(t, first.MessageID.EQ(duplicate.MessageID))
				} else {
					require.True(t, status.AsStreamingError(err).IsIgnoredOperation())
				}
				require.Equal(t, 1, persisted)
			}
			calls := entered.calls.Load()
			for _, marker := range []string{"_np", "_pcl", "_bh", "_ur"} {
				msg := build()
				msg.Properties().ToRawMap()[marker] = ""
				result, err := w.Append(t.Context(), msg)
				require.Nil(t, result, marker)
				require.Error(t, err, marker)
				require.True(t, status.AsStreamingError(err).IsUnrecoverable(), "%s: %v", marker, err)
				require.Equal(t, calls, entered.calls.Load(), "invalid %s reached the interceptor chain", marker)
				require.Equal(t, 1, persisted)
			}
			// Rejecting malformed traffic must not poison later valid appends.
			valid := build().WithTimeTick(20)
			if header := valid.ReplicateHeader(); header != nil {
				header.TimeTick = 20
				header.MessageID = walimplstest.NewTestMessageID(20)
				header.LastConfirmedMessageID = walimplstest.NewTestMessageID(19)
				valid = envelopeTestUpsert().WithTimeTick(20).WithReplicateHeader(header)
			}

			_, err = w.Append(t.Context(), valid)
			require.NoError(t, err)
		})
	}
}
