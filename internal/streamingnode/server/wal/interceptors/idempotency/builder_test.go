package idempotency

import (
	"context"
	"testing"

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
)

func TestBuilderRestoresOriginalAppendResult(t *testing.T) {
	paramtable.Init()
	originalID := newTestMessageID(10)
	lastConfirmed := newTestMessageID(9)
	result := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0, 2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 103}}}},
	}
	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			SummarySnapshots: map[string]*idempotencyview.Snapshot{
				"v1": {
					PChannel: "p1", VChannel: "v1",
					Records: []*idempotencyview.Record{{
						IdempotencyKey:         "restored-key",
						SourceMessageID:        message.MustMarshalMessageID(originalID),
						SourceTimeTick:         100,
						LastConfirmedMessageID: message.MustMarshalMessageID(lastConfirmed),
						InsertResult:           result,
					}},
				},
			},
		},
	})
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
	require.Equal(t, uint64(100), extra.TimeTick)
	require.True(t, lastConfirmed.EQ(extra.LastConfirmedMessageID))
	require.True(t, proto.Equal(result, extra.Extra))
}
