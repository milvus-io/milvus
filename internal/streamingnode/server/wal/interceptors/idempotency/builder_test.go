package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestBuilderRestoresOriginalAppendResult(t *testing.T) {
	paramtable.Init()
	commitTT := tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute))
	originalID := newTestMessageID(10)
	lastConfirmed := newTestMessageID(9)
	result := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0, 2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 103}}}},
	}
	for _, ordered := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "ordered"}[ordered], func(t *testing.T) {
			param := &interceptors.InterceptorBuildParam{
				InitialRecoverSnapshot: &recovery.RecoverySnapshot{
					SummarySnapshots: map[string]*idempotencyview.Snapshot{
						"v1": {
							PChannel: "p1", VChannel: "v1",
							Records: []*idempotencyview.Record{{
								IdempotencyKey:         "restored-key",
								SourceMessageID:        message.MustMarshalMessageID(originalID),
								SourceTimeTick:         commitTT,
								LastConfirmedMessageID: message.MustMarshalMessageID(lastConfirmed),
								InsertResult:           result,
							}},
						},
					},
				},
			}
			if ordered {
				sections := []*idempotencyview.Snapshot{param.InitialRecoverSnapshot.SummarySnapshots["v1"]}
				param.InitialRecoverSnapshot.SummaryRecovery = &idempotencyview.RecoverySnapshot{
					VChannels: param.InitialRecoverSnapshot.SummarySnapshots, Chunks: [][]*idempotencyview.Snapshot{sections},
				}
				patch := mockey.Mock((*windowManager).restore).To(func(*windowManager, map[string]*idempotencyview.Snapshot) map[string]*Window {
					t.Fatal("startup must not use the unordered sorting fallback")
					return nil
				}).Build()
				defer patch.UnPatch()
			}
			interceptor := NewInterceptorBuilder().Build(param)
			defer interceptor.Close()
			ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
			id, err := interceptor.DoAppend(ctx, newIdempotentInsertMessage(t, "v1", "restored-key"),
				func(context.Context, message.MutableMessage) (message.MessageID, error) {
					t.Fatal("a restored IK must not append again")
					return nil, nil
				})
			require.NoError(t, err)
			require.True(t, originalID.EQ(id))
			extra := utility.GetExtraAppendResult(ctx)
			require.Equal(t, commitTT, extra.TimeTick)
			require.True(t, lastConfirmed.EQ(extra.LastConfirmedMessageID))
			require.True(t, proto.Equal(result, extra.Extra))
		})
	}
}
