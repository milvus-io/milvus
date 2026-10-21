package replicates

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func txnHeader(id int64) *message.ReplicateHeader {
	return &message.ReplicateHeader{MessageID: walimplstest.NewTestMessageID(id)}
}

func TestReplicateTxnBodyCount(t *testing.T) {
	helper := newReplicateTxnHelper()
	txn := &message.TxnContext{TxnID: 1}
	first, second := txnHeader(20), txnHeader(30)
	require.NoError(t, helper.StartBegin(txn))
	helper.BeginDone(txn, "p0_v0")
	require.NoError(t, helper.StartCommit(txn, 0), "legacy count skips validation")
	require.NoError(t, helper.AddNewMessage(txn, first))
	helper.AddNewMessageDone(first)
	require.True(t, status.AsStreamingError(helper.StartCommit(txn, 2)).IsTxnExpired())
	require.True(t, status.AsStreamingError(helper.StartBegin(txn)).IsIgnoredOperation())
	require.True(t, status.AsStreamingError(helper.AddNewMessage(txn, first)).IsIgnoredOperation())
	require.Len(t, helper.messageIDs, 1, "duplicate delivery cannot fill a missing body")
	require.NoError(t, helper.AddNewMessage(txn, second))
	require.Error(t, helper.StartCommit(txn, 2), "failed append cannot increase count")
	helper.AddNewMessageDone(second)
	require.NoError(t, helper.StartCommit(txn, 2))
	require.Error(t, helper.StartCommit(txn, 1), "extra bodies also reject Commit")
	require.NoError(t, helper.StartCommit(txn, 0), "legacy Commit remains unchecked")
	helper.CommitDone()
	require.Nil(t, helper.CurrentTxn())
}

func TestReplicateTxnAbortAndReplay(t *testing.T) {
	helper := newReplicateTxnHelper()
	txn := &message.TxnContext{TxnID: 1}
	body := txnHeader(20)
	helper.BeginDone(txn, "p0_v0")
	helper.AddNewMessageDone(body)
	helper.clear() // durable Flush
	require.True(t, status.AsStreamingError(helper.StartCommit(txn, 1)).IsTxnExpired())
	require.True(t, status.AsStreamingError(helper.AddNewMessage(txn, body)).IsTxnExpired())
	require.NoError(t, helper.StartBegin(txn))
	helper.BeginDone(txn, "p0_v0")
	require.Error(t, helper.StartCommit(txn, 1))
	require.NoError(t, helper.AddNewMessage(txn, body), "body must be rewritten after abort")
	helper.AddNewMessageDone(body)
	require.NoError(t, helper.StartCommit(txn, 1), "Commit from either stream is safe once all bodies exist")
}
