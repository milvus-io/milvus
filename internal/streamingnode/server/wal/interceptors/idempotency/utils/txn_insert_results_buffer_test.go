package utils

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newTestTxnMessage(t *testing.T, vchannel string, txnID message.TxnID) message.MutableMessage {
	t.Helper()
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{TxnID: txnID, Keepalive: 10 * time.Second})
}

func replicatedTestTxnBody(t *testing.T, vchannel string, txnID message.TxnID) message.MutableMessage {
	t.Helper()
	return newTestTxnMessage(t, vchannel, txnID).
		WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "source-cluster",
			MessageID:              walimplstest.NewTestMessageID(1),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
			TimeTick:               1,
			VChannel:               vchannel,
		})
}

func intIDsResult(offsets []uint32, data ...int64) *messagespb.IdempotentInsertResult {
	return &messagespb.IdempotentInsertResult{
		RowOffsets: offsets,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}}},
	}
}

func TestTxnInsertResultBuffersDropsEmptyVChannel(t *testing.T) {
	buffers := NewTxnInsertResultBuffers(nil, nil)
	msg := newTestTxnMessage(t, "v1", 1)

	buffers.Add(msg, intIDsResult([]uint32{0}, 100), 0)
	require.Len(t, buffers.builders, 1)
	require.NotNil(t, buffers.Build(msg))

	buffers.Remove(msg)
	require.Empty(t, buffers.builders, "an empty vchannel buffer must be dropped so the map does not leak one entry per vchannel")
}

// BuildIfComplete merges only when every body contributed: a partial merge would
// answer a later retry with missing primary keys.
func TestTxnInsertResultBuffersBuildIfCompleteRequiresEveryBody(t *testing.T) {
	buffers := NewTxnInsertResultBuffers(nil, nil)
	msg := newTestTxnMessage(t, "v1", 1)
	buffers.Add(msg, intIDsResult([]uint32{0}, 100), 0)

	require.Nil(t, buffers.BuildIfComplete(msg, 2), "one of two bodies is buffered")
	require.Nil(t, buffers.BuildIfComplete(msg, 0), "an unknown body count cannot prove completeness")
	require.Nil(t, buffers.BuildIfComplete(newTestTxnMessage(t, "v1", 2), 1), "nothing is buffered for this txn")

	buffers.Add(msg, intIDsResult([]uint32{1}, 101), 0)
	merged := buffers.BuildIfComplete(msg, 2)
	require.NotNil(t, merged)
	require.ElementsMatch(t, []int64{100, 101}, merged.GetIds().GetIntId().GetData())
}

func TestTxnInsertResultBuffersRemoveVChannel(t *testing.T) {
	buffers := NewTxnInsertResultBuffers(nil, nil)
	msg1 := newTestTxnMessage(t, "v1", 1)
	msg2 := newTestTxnMessage(t, "v1", 2)
	msgOther := newTestTxnMessage(t, "v2", 3)

	buffers.Add(msg1, intIDsResult([]uint32{0}, 100), 0)
	buffers.Add(msg2, intIDsResult([]uint32{0}, 200), 0)
	buffers.Add(msgOther, intIDsResult([]uint32{0}, 300), 0)
	require.Len(t, buffers.builders["v1"], 2)
	require.Len(t, buffers.builders["v2"], 1)

	buffers.RemoveVChannel("v1")
	require.NotContains(t, buffers.builders, "v1")
	require.Contains(t, buffers.builders, "v2")
	require.Nil(t, buffers.Build(msg1))
	require.NotNil(t, buffers.Build(msgOther))

	buffers.RemoveVChannel("")
	require.Contains(t, buffers.builders, "v2")
}

func TestTxnInsertResultBuffersMergesBodiesAndCleansUp(t *testing.T) {
	buffers := NewTxnInsertResultBuffers(nil, nil)
	msg := newTestTxnMessage(t, "v1", 7)

	buffers.Add(msg, intIDsResult([]uint32{0}, 100), 0)
	buffers.Add(msg, intIDsResult([]uint32{2, 1}, 102, 101), 0)

	merged := buffers.Build(msg)
	require.Equal(t, []uint32{0, 2, 1}, merged.GetRowOffsets())
	require.Equal(t, []int64{100, 102, 101}, merged.GetIds().GetIntId().GetData())

	buffers.Remove(msg)
	require.Empty(t, buffers.builders)
}

// TestTxnInsertResultBuffersConcurrentAddBuildRemove exercises the per-vchannel
// reclamation under concurrency: each txn must read back exactly its own buffered
// result (no cross-txn loss while empty buffers are being dropped), and the map
// must be empty once every txn is removed. Run with -race.
func TestTxnInsertResultBuffersConcurrentAddBuildRemove(t *testing.T) {
	buffers := NewTxnInsertResultBuffers(nil, nil)
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(txnID message.TxnID) {
			defer wg.Done()
			msg := newTestTxnMessage(t, "v1", txnID)
			buffers.Add(msg, intIDsResult([]uint32{0}, int64(txnID)), 0)
			built := buffers.Build(msg)
			require.NotNil(t, built)
			require.Equal(t, []int64{int64(txnID)}, built.GetIds().GetIntId().GetData())
			buffers.Remove(msg)
		}(message.TxnID(i + 1))
	}
	wg.Wait()
	require.Empty(t, buffers.builders, "all txns removed: the vchannel buffer must be dropped")
}

// A WAL reopen places the vchannel MVCC at the recovery timetick, which can sit
// far ahead of a body written more than one keepalive before the restart. A
// replicated body must survive that timetick: the source owns the transaction's
// lifecycle, so its buffer is reclaimed only through the txn-active check.
func TestTxnInsertResultBuffersReplicatedBodySurvivesAheadMVCC(t *testing.T) {
	currentTimeTick := tsoutil.ComposeTS(20000, 0)
	buffers := NewTxnInsertResultBuffers(func(string) (uint64, bool) {
		return currentTimeTick, true
	}, nil)

	restored := replicatedTestTxnBody(t, "v1", 7).WithTimeTick(tsoutil.ComposeTS(0, 11))
	buffers.Restore(restored.IntoImmutableMessage(walimplstest.NewTestMessageID(11)), intIDsResult([]uint32{0}, 100))
	require.NotNil(t, buffers.BuildIfComplete(restored, 1),
		"a restored replicated body must not expire just because the recovery MVCC is ahead")

	added := replicatedTestTxnBody(t, "v1", 8).WithTimeTick(tsoutil.ComposeTS(0, 11))
	buffers.Add(added, intIDsResult([]uint32{0}, 200), tsoutil.ComposeTS(0, 11))
	require.NotNil(t, buffers.BuildIfComplete(added, 1),
		"a replicated body buffered live must also ignore a MVCC that moved ahead")
}

// The same ahead MVCC still reclaims a native body: only replicated transactions
// are immune to timetick-based expiry.
func TestTxnInsertResultBuffersNativeBodyExpiresWhenMVCCAhead(t *testing.T) {
	currentTimeTick := tsoutil.ComposeTS(20000, 0)
	buffers := NewTxnInsertResultBuffers(func(string) (uint64, bool) {
		return currentTimeTick, true
	}, nil)

	msg := newTestTxnMessage(t, "v1", 7).WithTimeTick(tsoutil.ComposeTS(0, 11))
	buffers.Add(msg, intIDsResult([]uint32{0}, 100), tsoutil.ComposeTS(0, 11))
	require.Nil(t, buffers.Build(msg))
}
