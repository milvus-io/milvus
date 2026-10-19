package replicates

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func txnHeader(id, previous int64) *message.ReplicateHeader {
	return &message.ReplicateHeader{
		MessageID: walimplstest.NewTestMessageID(id), PrevTxnMessageID: walimplstest.NewTestMessageID(previous),
	}
}

func TestReplicateTxnPrefix(t *testing.T) {
	helper := newReplicateTxnHelper()
	txn := &message.TxnContext{TxnID: 1}
	begin, first, second, commit := txnHeader(10, 10), txnHeader(20, 10), txnHeader(30, 20), txnHeader(40, 30)
	require.NoError(t, helper.StartBegin(txn, begin))
	helper.BeginDone(txn, begin, "p0_v0")
	// A larger MessageID alone does not prove that the prefix is complete.
	require.True(t, status.AsStreamingError(helper.AddNewMessage(txn, second)).IsTxnExpired())
	require.NoError(t, helper.AddNewMessage(txn, first))
	helper.AddNewMessageDone(first)
	require.True(t, status.AsStreamingError(helper.StartCommit(txn, commit)).IsTxnExpired())
	require.True(t, status.AsStreamingError(helper.StartBegin(txn, begin)).IsIgnoredOperation())
	require.True(t, status.AsStreamingError(helper.AddNewMessage(txn, first)).IsIgnoredOperation())
	require.NoError(t, helper.AddNewMessage(txn, second))
	// A failed append does not advance the prefix.
	require.Error(t, helper.StartCommit(txn, commit))
	helper.AddNewMessageDone(second)
	require.NoError(t, helper.StartCommit(txn, commit))
	require.Error(t, helper.StartCommit(txn, &message.ReplicateHeader{MessageID: commit.MessageID}))
	helper.CommitDone(txn)
	require.Nil(t, helper.CurrentTxn())
}

func TestReplicateTxnAbortAndReplay(t *testing.T) {
	helper := newReplicateTxnHelper()
	txn := &message.TxnContext{TxnID: 1}
	begin, body, commit := txnHeader(10, 10), txnHeader(20, 10), txnHeader(30, 20)
	helper.BeginDone(txn, begin, "p0_v0")
	helper.AddNewMessageDone(body)
	helper.clear() // durable Flush
	require.True(t, status.AsStreamingError(helper.StartCommit(txn, commit)).IsTxnExpired())
	require.True(t, status.AsStreamingError(helper.AddNewMessage(txn, body)).IsTxnExpired())
	require.NoError(t, helper.StartBegin(txn, begin))
	helper.BeginDone(txn, begin, "p0_v0")
	require.Error(t, helper.StartCommit(txn, commit))
	require.NoError(t, helper.AddNewMessage(txn, body), "body must be rewritten after abort")
	helper.AddNewMessageDone(body)
	require.NoError(t, helper.StartCommit(txn, commit), "a commit from either stream is safe once the prefix is complete")
}

func TestReplicateEmptyTxn(t *testing.T) {
	helper := newReplicateTxnHelper()
	txn := &message.TxnContext{TxnID: 1}
	require.Error(t, helper.StartBegin(txn, txnHeader(10, 9)))
	helper.BeginDone(txn, txnHeader(10, 10), "p0_v0")
	require.NoError(t, helper.StartCommit(txn, txnHeader(20, 10)))
}
