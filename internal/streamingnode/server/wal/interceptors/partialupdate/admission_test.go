package partialupdate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func requirePartialUpdateCode(t *testing.T, err error, code streamingpb.StreamingCode) {
	t.Helper()
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.NotNil(t, streamingErr)
	require.Equal(t, code, streamingErr.Code)
}

func requirePartialUpdateRetryable(t *testing.T, err error) {
	t.Helper()
	requirePartialUpdateCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_PARTIAL_UPDATE_RETRYABLE)
}

func requireUnrecoverable(t *testing.T, err error) {
	t.Helper()
	requirePartialUpdateCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE)
}

func TestValidateCommitRejectsPKConflict(t *testing.T) {
	state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
	state.pkVersions.UpdateAll("v1", []any{int64(10)}, 200)
	state.recordTxnBegin(1)
	require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
	state.recordTxnWrites(1, []any{int64(10)})

	_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 300), 1)
	requirePartialUpdateRetryable(t, err)
}

func TestValidateCommitPublishesAfterSuccessfulAppend(t *testing.T) {
	state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
	state.recordTxnBegin(1)
	require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
	state.recordTxnWrites(1, []any{int64(10)})

	commit := newCASCommitTxnMessage(t, "v1", 1, 300)
	txnState, err := state.validateCommit(commit, 1)
	require.NoError(t, err)
	state.publishCommit(commit, txnState)

	requirePartialUpdateRetryable(t, state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 299, 301))
}

func TestValidateCommitMarkerConsistency(t *testing.T) {
	t.Run("ordinary commit without runtime state", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})

		txnState, err := state.validateCommit(newCommitTxnMessage("v1", 1, 300), 1)
		require.NoError(t, err)
		require.Nil(t, txnState)
		state.publishCommit(newCommitTxnMessage("v1", 1, 300), txnState)
		requirePartialUpdateRetryable(t, state.incompleteTxnFences.Verify("v1", 299))
		require.NoError(t, state.incompleteTxnFences.Verify("v1", 300))
	})

	t.Run("runtime CAS without marker", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})

		_, err := state.validateCommit(newCommitTxnMessage("v1", 1, 300), 1)
		requireUnrecoverable(t, err)
	})

	t.Run("marker without runtime state", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 300), 1)
		requirePartialUpdateRetryable(t, err)
	})

	t.Run("marker without CAS metadata", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.recordTxnBegin(1)
		state.recordTxnWrites(1, []any{int64(10)})

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 300), 1)
		requireUnrecoverable(t, err)
	})

	t.Run("empty vchannel", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "", 1, 300), 1)
		requireUnrecoverable(t, err)
	})
}

func TestPublishCommitAdvancesRetentionAndFence(t *testing.T) {
	state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
	commit := newCommitTxnMessage("v1", 1, 300)

	state.publishCommit(commit, &pendingTxn{fenceCollection: 10, observedBegin: true})

	requirePartialUpdateRetryable(t, state.fences.Verify("v1", 10, 299))
}

func TestValidateCommitRejectsInvalidProof(t *testing.T) {
	t.Run("missing derived CAS scope", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.txns[1] = &pendingTxn{
			meta:          validCASMeta(100, 1),
			pks:           primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}},
			observedBegin: true,
		}

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 300), 1)
		requireUnrecoverable(t, err)
	})

	t.Run("missing primary keys", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 300), 1)
		requireUnrecoverable(t, err)
	})

	t.Run("mixed collection ids", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})
		state.recordTxnFence(1, 2)

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 300), 1)
		requireUnrecoverable(t, err)
	})
}

func TestValidateCommitRejectsTermAndReadConflicts(t *testing.T) {
	t.Run("term mismatch", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 2})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 120), 1)
		requirePartialUpdateRetryable(t, err)
	})

	t.Run("stale read", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		state.pkVersions.channel("v1").retainedSinceTS = 200
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 220), 1)
		requirePartialUpdateRetryable(t, err)
	})

	t.Run("collection fence", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
		scope := casInsertScope{collectionID: 2, schemaVersion: 1}
		state.fences.Update("v1", scope.collectionID, 150)
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), scope))
		state.recordTxnWrites(1, []any{int64(10)})

		_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 160), 1)
		requirePartialUpdateRetryable(t, err)
	})
}

func TestValidateCommitRejectsIncompleteTransactionFence(t *testing.T) {
	state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 1})
	state.incompleteTxnFences.Update("v1", 150)
	state.recordTxnBegin(1)
	require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
	state.recordTxnWrites(1, []any{int64(10)})

	_, err := state.validateCommit(newCASCommitTxnMessage(t, "v1", 1, 160), 1)
	requirePartialUpdateRetryable(t, err)
}

func TestValidateCommitReplicatedCASBypassesSourceProof(t *testing.T) {
	state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 2})
	state.recordTxnBegin(1)
	require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
	state.recordTxnWrites(1, []any{int64(10)})

	commit := newCASCommitTxnMessage(t, "v1", 1, 160)
	msgID := walimplstest.NewTestMessageID(1)
	commit = commit.WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "cluster-a",
		MessageID:              msgID,
		LastConfirmedMessageID: msgID,
		TimeTick:               160,
		VChannel:               "v1",
	})

	txnState, err := state.validateCommit(commit, 1)
	require.NoError(t, err)
	state.publishCommit(commit, txnState)
	requirePartialUpdateRetryable(t, state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 159, 161))
}

func TestValidateCommitReplicatedCASWithoutRuntimeStateUsesFence(t *testing.T) {
	state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 2})
	commit := newCASCommitTxnMessage(t, "v1", 1, 160)
	msgID := walimplstest.NewTestMessageID(1)
	commit = commit.WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "cluster-a",
		MessageID:              msgID,
		LastConfirmedMessageID: msgID,
		TimeTick:               160,
		VChannel:               "v1",
	})

	txnState, err := state.validateCommit(commit, 1)
	require.NoError(t, err)
	require.Nil(t, txnState)
	state.publishCommit(commit, txnState)
	requirePartialUpdateRetryable(t, state.incompleteTxnFences.Verify("v1", 159))
}

func TestValidateCommitReplicatedCASRequiresCompleteWriteSet(t *testing.T) {
	newReplicatedCommit := func(t *testing.T) message.MutableMessage {
		t.Helper()
		msgID := walimplstest.NewTestMessageID(1)
		return newCASCommitTxnMessage(t, "v1", 1, 160).WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "cluster-a",
			MessageID:              msgID,
			LastConfirmedMessageID: msgID,
			TimeTick:               160,
			VChannel:               "v1",
		})
	}

	t.Run("missing primary keys", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 2})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))

		_, err := state.validateCommit(newReplicatedCommit(t), 1)
		requireUnrecoverable(t, err)
	})

	t.Run("mixed collection ids", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 2})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})
		state.recordTxnFence(1, 2)

		_, err := state.validateCommit(newReplicatedCommit(t), 1)
		requireUnrecoverable(t, err)
	})

	t.Run("missing commit marker", func(t *testing.T) {
		state := newTestAdmissionState(types.PChannelInfo{Name: "p1", Term: 2})
		state.recordTxnBegin(1)
		require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
		state.recordTxnWrites(1, []any{int64(10)})
		msgID := walimplstest.NewTestMessageID(1)
		commit := newCommitTxnMessage("v1", 1, 160).WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "cluster-a",
			MessageID:              msgID,
			LastConfirmedMessageID: msgID,
			TimeTick:               160,
			VChannel:               "v1",
		})

		_, err := state.validateCommit(commit, 1)
		requireUnrecoverable(t, err)
	})
}

func TestPartialUpdateStateStoresTxnCASProofAndScope(t *testing.T) {
	state := newPartialUpdateState(time.Second, versionIndexBudgetForEntries(100))
	require.NoError(t, state.recordTxnCAS(1, nil, casInsertScope{}))
	require.Error(t, state.recordTxnCAS(2, validCASMeta(100, 1), casInsertScope{}))

	meta := validCASMeta(100, 1)
	require.NoError(t, state.recordTxnCAS(1, meta, validCASScope()))
	meta.ReadTs = 999
	require.NoError(t, state.recordTxnCAS(1, validCASMeta(100, 1), validCASScope()))
	require.Error(t, state.recordTxnCAS(1, validCASMeta(101, 1), validCASScope()))
	require.Error(t, state.recordTxnCAS(1, validCASMeta(100, 1), casInsertScope{
		collectionID:  2,
		schemaVersion: 1,
	}))
	require.Error(t, state.recordTxnCAS(1, validCASMeta(100, 1), casInsertScope{
		collectionID:  1,
		schemaVersion: 2,
	}))
	require.NoError(t, state.recordTxnCAS(3, validCASMeta(100, 1), casInsertScope{
		collectionID:  1,
		schemaVersion: 0,
	}))

	txnState := state.getTxn(1)
	require.EqualValues(t, 100, txnState.meta.GetReadTs())
	require.EqualValues(t, 1, txnState.collectionID)
	require.EqualValues(t, 1, txnState.schemaVersion)
	require.True(t, txnState.casScopeSet)
	txnState.meta.ReadTs = 999
	require.EqualValues(t, 100, state.getTxn(1).meta.GetReadTs())
}

func newTestAdmissionState(channel types.PChannelInfo) *partialUpdateState {
	state := newPartialUpdateState(30*time.Second, versionIndexBudgetForEntries(100))
	state.channel = channel
	return state
}

func newCommitTxnMessage(vchannel string, txnID message.TxnID, timetick uint64) message.MutableMessage {
	return message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{TxnID: txnID, Keepalive: time.Second}).
		WithTimeTick(timetick)
}

func newCASCommitTxnMessage(t *testing.T, vchannel string, txnID message.TxnID, timetick uint64) message.MutableMessage {
	t.Helper()
	msg := newCommitTxnMessage(vchannel, txnID, timetick)
	require.NoError(t, message.MarkPartialUpdateCASCommit(msg))
	return msg
}

func validCASMeta(readTS uint64, term int64) *messagespb.PartialUpdateCAS {
	return &messagespb.PartialUpdateCAS{
		ReadTs:               readTS,
		ObservedPchannelTerm: term,
	}
}

func validCASScope() casInsertScope {
	return casInsertScope{collectionID: 1, schemaVersion: 1}
}
