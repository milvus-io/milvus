package replicates

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestReplicateCommitFailurePreservesTransaction(t *testing.T) {
	helper := newReplicateTxnHelper()
	txn := &message.TxnContext{TxnID: 1}
	rh := &message.ReplicateHeader{MessageID: walimplstest.NewTestMessageID(10)}
	require.NoError(t, helper.StartBegin(txn, rh))
	helper.BeginDone(txn)
	require.NoError(t, helper.AddNewMessage(txn, rh))
	helper.AddNewMessageDone(rh)
	require.NoError(t, helper.StartCommit(txn))
	// No CommitDone on append failure: both reconnect dedup and commit retry work.
	require.Same(t, txn, helper.CurrentTxn())
	require.Error(t, helper.AddNewMessage(txn, rh))
	require.Error(t, helper.StartBegin(&message.TxnContext{TxnID: 2}, rh))
	require.NoError(t, helper.StartCommit(txn))
	helper.CommitDone(txn)
	require.Nil(t, helper.CurrentTxn())
	require.NoError(t, helper.StartBegin(&message.TxnContext{TxnID: 2}, rh))
}
