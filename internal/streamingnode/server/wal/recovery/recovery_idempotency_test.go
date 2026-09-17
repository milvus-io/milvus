package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRecoveryRestoresIdempotencyHistoryAtBarrier(t *testing.T) {
	resource.InitForTest(t)
	for _, tc := range []struct {
		name          string
		flushMaxBytes uint64
		missingChunk  bool
	}{
		{name: "staged replay"},
		{name: "sealed replay", flushMaxBytes: 1},
		{name: "unreadable history", missingChunk: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			store := walsummary.NewStore(cm, "test-pchannel", 1)
			previous := walsummary.NewManager(walsummary.ManagerConfig{
				PChannel: "test-pchannel", Term: 1, Store: store,
				Runtime: moduleapi.Runtime{Scheduler: immediateTaskScheduler{}},
			})
			require.NoError(t, previous.Restore(ctx))
			result := &messagespb.IdempotentInsertResult{
				RowOffsets: []uint32{0, 2},
				Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 103}}}},
			}
			durable := newRecoveryIdempotentInsert("historical-vchannel", "durable-key", 10, result)
			previous.ObserveMessage(ctx, durable)
			previous.RequestFlushThrough(10)
			require.Equal(t, uint64(10), previous.LastAcked())

			// The checkpoint is newer than this retained history, and the write
			// path has no collection for either vchannel. Neither may filter IKs.
			rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(20), TimeTick: 20,
			})
			defer rs.metrics.Close()
			defer rs.taskScheduler.Close()
			rs.summaryManager = walsummary.NewManager(walsummary.ManagerConfig{
				PChannel: "test-pchannel", Term: 2,
				Store:         walsummary.NewStore(cm, "test-pchannel", 2),
				FlushMaxBytes: tc.flushMaxBytes,
				// No scheduler: sealed replay must be readable before upload.
			})
			require.NoError(t, rs.summaryManager.Restore(ctx))
			if tc.missingChunk {
				require.NoError(t, store.DeleteChunk(ctx, 0, 1))
			}
			replayed := newRecoveryIdempotentInsert("replayed-vchannel", "replayed-key", 30, result)
			barrier := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
				WithHeader(&message.RecoveryBarrierMessageHeader{}).
				WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable().
				WithTimeTick(40).WithLastConfirmed(walimplstest.NewTestMessageID(39)).
				IntoImmutableMessage(walimplstest.NewTestMessageID(40))
			stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 3), closed: make(chan struct{})}
			defer stream.Close()
			stream.ch <- replayed
			stream.ch <- barrier
			stream.ch <- newRecoveryIdempotentInsert("replayed-vchannel", "after-barrier", 50, result)
			snapshot, err := rs.runBoundedRecovery(ctx, &recordingRecoveryStreamBuilder{stream: stream}, barrier)
			if tc.missingChunk {
				require.ErrorContains(t, err, "failed to read the idempotency summary")
				require.Nil(t, snapshot, "incomplete history must fail WAL open")
				return
			}
			require.NoError(t, err)
			require.Empty(t, snapshot.WritePathRecovery.VChannels)
			require.Len(t, snapshot.SummarySnapshots, 2)
			for _, original := range []message.ImmutableMessage{durable, replayed} {
				restored := snapshot.SummarySnapshots[original.VChannel()]
				require.NotNil(t, restored)
				require.Equal(t, "test-pchannel", restored.PChannel)
				require.Equal(t, original.VChannel(), restored.VChannel)
				require.Len(t, restored.Records, 1)
				record := restored.Records[0]
				require.Equal(t, string(message.IdempotencyKeyOf(original)), record.IdempotencyKey)
				require.Equal(t, original.TimeTick(), record.SourceTimeTick)
				require.True(t, proto.Equal(message.MustMarshalMessageID(original.MessageID()), record.SourceMessageID))
				require.True(t, proto.Equal(message.MustMarshalMessageID(original.LastConfirmedMessageID()), record.LastConfirmedMessageID))
				require.True(t, proto.Equal(result, record.InsertResult))
			}
			require.Len(t, stream.ch, 1, "startup must stop observation at the barrier")
		})
	}
}

func newRecoveryIdempotentInsert(vchannel, key string, tt uint64, result *messagespb.IdempotentInsertResult) message.ImmutableMessage {
	header := &message.InsertMessageHeader{CollectionId: 1}
	message.SetInsertHeaderIdempotentInsertResult(header, result)
	return message.NewInsertMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(header).WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		WithIdempotencyKey(message.IdempotencyKey(key)).MustBuildMutable().
		WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
}
