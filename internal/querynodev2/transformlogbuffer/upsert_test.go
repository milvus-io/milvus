//go:build test && dynamic

package transformlogbuffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type payloadTransformSegment struct {
	fakeSegment
	entries   []*streamingpb.TransformLogEntry
	blockTick uint64
	entered   chan struct{}
	resume    chan struct{}
}

func (s *payloadTransformSegment) ApplyTransform(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	if entry.GetTimeTick() == s.blockTick {
		close(s.entered)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, proto.Clone(entry).(*streamingpb.TransformLogEntry))
	s.applied = append(s.applied, entry.GetTimeTick())
	return nil
}

func (s *payloadTransformSegment) appliedEntries() []*streamingpb.TransformLogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*streamingpb.TransformLogEntry(nil), s.entries...)
}

func transformUpsert(tick uint64) message.ImmutableMessage {
	return message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, DeleteRows: 2}).
		WithBody(&message.UpsertMessageBody{
			Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 2, NumRows: 2},
			Delete: &msgpb.DeleteRequest{
				CollectionID: 1, PartitionID: -1, NumRows: 2, Timestamps: []uint64{999, 999},
				PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"one", "two"}}}},
			},
		}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
}

func transformDelete(tick uint64, partition int64, key string) message.ImmutableMessage {
	return message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			CollectionID: 1, PartitionID: partition, NumRows: 1,
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{key}}}},
		}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
}

func TestUpsertTransformEntriesReplayAndLiveApplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	defer buffer.Close()
	guard, err := buffer.Acquire(ctx, newTestQueryView("p_1v0", 10))
	require.NoError(t, err)
	defer guard.Release()
	stream := streams.stream("p")
	emit := func(entry *streamingpb.TransformLogEntry) {
		stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", Entry: entry})
	}
	entryFor := func(raw message.ImmutableMessage) *streamingpb.TransformLogEntry {
		entry := messageutil.BuildTransformLogEntry(raw, messageutil.TransformEntryOption{})
		require.NotNil(t, entry)
		require.NotNil(t, entry.GetDelete())
		return entry
	}
	oldEntry := entryFor(transformDelete(20, 2, "old-delete"))
	upsertEntry := entryFor(transformUpsert(30))
	emit(oldEntry)
	emit(upsertEntry)
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 30}})
	segment := &payloadTransformSegment{fakeSegment: fakeSegment{id: 1, vchannel: "p_1v0", startAfter: 10}, blockTick: 40, entered: make(chan struct{}), resume: make(chan struct{})}
	var resume sync.Once
	defer resume.Do(func() { close(segment.resume) })
	registration, err := buffer.RegisterSegment(ctx, segment)
	require.NoError(t, err)
	defer func() {
		resume.Do(func() { close(segment.resume) })
		registration.Unregister()
	}()
	require.NoError(t, registration.WaitCatchup(ctx))
	require.Equal(t, []uint64{20, 30}, segment.appliedTicks())

	// A repeated delivered entry must not reapply its delete block.
	emit(upsertEntry)
	require.Equal(t, []uint64{20, 30}, segment.appliedTicks())
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(31).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(31))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	builder.Add(transformUpsert(32))
	builder.Add(transformDelete(33, 4, "txn-delete"))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).
		WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(40).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(40))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	txnEntry := entryFor(txn)
	finished := make(chan struct{})
	go func() { emit(txnEntry); close(finished) }()
	select {
	case <-segment.entered:
	case <-ctx.Done():
		t.Fatal("transaction transform application did not start")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, registration.WaitTransformVisible(canceled, 40), context.Canceled, "physical instance cannot pass a blocked compound entry")
	require.ErrorIs(t, guard.WaitTransformVisible(canceled, 40), context.Canceled, "shared frontier cannot pass live application")
	require.Equal(t, []uint64{20, 30}, segment.appliedTicks())
	resume.Do(func() { close(segment.resume) })
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("transaction transform application did not finish")
	}
	require.NoError(t, registration.WaitTransformVisible(ctx, 40))
	emit(txnEntry)
	emit(entryFor(transformDelete(50, 2, "later-delete")))
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 60}})
	require.NoError(t, registration.WaitTransformVisible(ctx, 60))
	require.Equal(t, []uint64{20, 30, 40, 50}, segment.appliedTicks())
	entries := segment.appliedEntries()
	for _, index := range []int{1, 2} {
		blocks := entries[index].GetDelete().GetBlocks()
		require.Equal(t, int64(-1), blocks[0].GetPartitionId())
		require.Equal(t, []string{"one", "two"}, blocks[0].GetPrimaryKeys().GetStrId().GetData())
	}
	require.Equal(t, uint64(30), entries[1].GetTimeTick())
	require.Equal(t, uint64(40), entries[2].GetTimeTick(), "body timestamps must project to Commit visibility")
	require.Len(t, entries[2].GetDelete().GetBlocks(), 2)
	require.Equal(t, int64(4), entries[2].GetDelete().GetBlocks()[1].GetPartitionId())
	require.Equal(t, []string{"txn-delete"}, entries[2].GetDelete().GetBlocks()[1].GetPrimaryKeys().GetStrId().GetData())

	// A replacement physical instance replays the retained mix without losing
	// legacy entries, duplicating Upsert blocks, or stopping at a body timestamp.
	replacement := &payloadTransformSegment{fakeSegment: fakeSegment{id: 1, vchannel: "p_1v0", startAfter: 10}}
	replayed, err := buffer.RegisterSegment(ctx, replacement)
	require.NoError(t, err)
	defer replayed.Unregister()
	require.NoError(t, replayed.WaitCatchup(ctx))
	require.NoError(t, replayed.WaitTransformVisible(ctx, 60))
	require.Equal(t, []uint64{20, 30, 40, 50}, replacement.appliedTicks())
	for index, entry := range replacement.appliedEntries() {
		require.True(t, proto.Equal(entries[index], entry))
	}
}
