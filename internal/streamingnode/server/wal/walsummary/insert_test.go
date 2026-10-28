package walsummary

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func plainSummaryInsert(tt uint64) message.ImmutableMessage {
	return message.NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).MustBuildMutable().
		WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
}

func TestKeylessInsertPreservesCoverageAcrossRestart(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.NoError(t, manager.Restore(ctx))
	manager.InitLastAcked(50)
	manager.ObserveMessage(ctx, plainSummaryInsert(100))
	observeReadBarrier(manager, 200)
	require.Equal(t, uint64(50), manager.LastAcked(), "a keyless insert must pin confirmation until persisted")

	require.NoError(t, persistSummary(ctx, manager))
	require.Equal(t, uint64(200), manager.LastAcked())
	require.Equal(t, uint64(51), manager.Manifest().GetCoverage().GetStartTimeTick())

	restored := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, restored.Restore(ctx))
	restored.InitLastAcked(200)
	batch, err := restored.ReadTransform(ctx, "v1", 99, 200, ReadLimits{})
	require.NoError(t, err)
	require.Zero(t, batch.FastForwardTimeTick, "restart must preserve the original coverage start")
	require.Equal(t, uint64(200), batch.CoveredThrough)
	require.Empty(t, batch.Entries)
	sections, err := restored.ReadIdempotencyEntries(ctx, "v1", 0, math.MaxUint64)
	require.NoError(t, err)
	records, err := idempotencyview.RecordsFromSections(sections.Idempotency, sections.Inserts)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Empty(t, records[0].IdempotencyKey)
	require.Nil(t, records[0].InsertResult)
	require.Equal(t, uint64(100), records[0].SourceTimeTick)
	require.Equal(t, messageIDProto(plainSummaryInsert(100).MessageID()), records[0].SourceMessageID)
	require.Equal(t, messageIDProto(plainSummaryInsert(100).LastConfirmedMessageID()), records[0].LastConfirmedMessageID)
}

func TestInsertFactsFollowCommittedTransactionContents(t *testing.T) {
	for _, tc := range []struct {
		name       string
		key        string
		withInsert bool
	}{
		{name: "keyless insert", withInsert: true},
		{name: "keyed insert without result", key: "cannot-rebuild-result", withInsert: true},
		{name: "delete only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			manager, store := newTestManagerWithStore(t)
			source := message.AsImmutableTxnMessage(newTestIdempotentTxnMessage(t, "v1", 100, tc.key, [][]int64{{1}, {2}}))
			builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(source.Begin()))
			if tc.withInsert {
				builder.Add(plainSummaryInsert(101))
			}
			builder.Add(newTestDeleteMessage(t, "v1", 102, 1, 9))
			txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(source.Commit()))
			require.NoError(t, err)
			manager.ObserveMessage(ctx, txn)
			require.NoError(t, persistSummary(ctx, manager))
			restored := newTestManager(t, nextTermStore(store), 1<<30)
			require.NoError(t, restored.Restore(ctx))
			sections, err := restored.ReadIdempotencyEntries(ctx, "v1", 0, math.MaxUint64)
			require.NoError(t, err)
			if tc.withInsert {
				require.Len(t, sections.Inserts, 1)
				require.Equal(t, txn.TimeTick(), sections.Inserts[0].GetSourceTimetick())
				require.Nil(t, sections.Inserts[0].GetIds())
				for _, annotation := range sections.Idempotency {
					require.Empty(t, annotation.GetKey(), "no incomplete duplicate response may be restored")
				}
			} else {
				require.Empty(t, sections.Inserts)
			}
			batch, err := restored.ReadTransform(ctx, "v1", 0, txn.TimeTick(), ReadLimits{})
			require.NoError(t, err)
			require.Len(t, batch.Entries, 1)
			require.Equal(t, txn.TimeTick(), batch.Entries[0].GetTimeTick())
		})
	}
}
