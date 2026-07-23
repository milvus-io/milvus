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
