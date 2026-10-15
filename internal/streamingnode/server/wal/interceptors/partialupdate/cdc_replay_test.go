package partialupdate

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// Exercise the production replicate -> TimeTick -> partial-update chain. A TSO
// failure before handleCommit leaves the old session alive after replication has
// reset its transaction helper, so CDC can replay Begin with the same source ID.
func TestPartialUpdateCDCReplayAfterTSOFailure(t *testing.T) {
	env, manager := newCDCReplayTestEnv(t)
	recovered := env.partial.state.getTxn(1)
	require.False(t, recovered.observedBegin)
	require.True(t, recovered.recovered)
	require.NotNil(t, recovered.meta)
	require.Equal(t, 1, recovered.pks.Len())
	_, durableBodies := env.buildParam.InitialRecoverSnapshot.TxnBuffer.GetUncommittedMessageBuilder()[1].Messages()
	require.Len(t, durableBodies, 2, "optional recovery decoding must retain both durable bodies")

	var committed []message.ImmutableMessage
	walAppend := func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		id := env.nextMessageID()
		committed = append(committed, env.buildParam.InitialRecoverSnapshot.TxnBuffer.HandleImmutableMessages(
			[]message.ImmutableMessage{msg.IntoImmutableMessage(id)}, msg.TimeTick())...)
		return id, nil
	}
	failCommit := func() {
		t.Helper()
		failure := errors.New("temporary TSO allocation failure")
		patch := mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "Allocate")).
			To(func(context.Context) (uint64, error) { return 0, failure }).Build()
		defer patch.UnPatch()
		_, err := env.append(cdcReplayCommit(t), walAppend)
		require.ErrorIs(t, err, failure)
		require.EqualValues(t, 1, patch.MockTimes(), "fail the first provisional timestamp allocation")
		session, err := env.txnManager.GetSessionOfTxn(1)
		require.NoError(t, err)
		require.Equal(t, message.TxnStateInFlight, session.State(), "failure must precede handleCommit")
		require.NotNil(t, env.partial.state.getTxn(1), "partial-update admission has not run")
		checkpoint, err := manager.GetReplicateCheckpoint()
		require.NoError(t, err)
		require.EqualValues(t, 1, checkpoint.TimeTick)
	}
	failCommit()

	// Reconnect and replay the full source transaction repeatedly. Each failed
	// attempt gets exactly one TSO fault; the final attempt commits normally.
	for round := 0; round < 5; round++ {
		oldSession, err := env.txnManager.GetSessionOfTxn(1)
		require.NoError(t, err)
		oldPending := env.partial.state.txns[1]
		_, err = env.append(cdcReplayBegin(), walAppend)
		require.NoError(t, err)
		currentSession, err := env.txnManager.GetSessionOfTxn(1)
		require.NoError(t, err)
		require.NotSame(t, oldSession, currentSession)
		require.NotSame(t, oldPending, env.partial.state.txns[1], "new Begin must replace the entire old write set")
		fresh := env.partial.state.getTxn(1)
		require.True(t, fresh.observedBegin)
		require.False(t, fresh.recovered)
		require.Nil(t, fresh.meta)
		require.Zero(t, fresh.collectionID)
		require.Zero(t, fresh.schemaVersion)
		require.False(t, fresh.casScopeSet)
		require.Zero(t, fresh.fenceCollection)
		require.Zero(t, fresh.pks.Len())
		require.True(t, fresh.cleanupRegistered)

		oldSession.Cleanup()
		require.Equal(t, fresh, env.partial.state.getTxn(1), "old cleanup must not delete the replacement")
		for _, body := range cdcReplayBodies(t) {
			_, err = env.append(body, walAppend)
			require.NoError(t, err)
		}
		require.Equal(t, 2, env.partial.state.getTxn(1).pks.Len(), "PKs must not accumulate across retries")
		// A duplicate Begin in the same replication attempt is deduplicated and
		// must preserve the already accepted bodies and their cleanup owner.
		pending := env.partial.state.txns[1]
		_, err = env.append(cdcReplayBegin(), walAppend)
		require.True(t, status.AsStreamingError(err).IsIgnoredOperation())
		require.Same(t, pending, env.partial.state.txns[1])
		if round < 4 {
			failCommit()
		}
	}
	_, err := env.append(cdcReplayCommit(t), walAppend)
	require.NoError(t, err)
	require.Nil(t, env.partial.state.getTxn(1))
	require.Len(t, committed, 1, "only the final successful Commit may publish the transaction")
	assembled := committed[0].(message.ImmutableTxnMessage)
	require.Equal(t, 2, assembled.Size(), "full replay must preserve both source bodies exactly once")
	checkpoint, err := manager.GetReplicateCheckpoint()
	require.NoError(t, err)
	require.EqualValues(t, 100, checkpoint.TimeTick)
	require.True(t, env.hasPKVersionAfter("v1", 10, 1))
	require.True(t, env.hasPKVersionAfter("v1", 20, 1))
}

func TestPartialUpdateCDCReplayFailedBeginCleanup(t *testing.T) {
	env, _ := newCDCReplayTestEnv(t)
	failure := errors.New("temporary TSO allocation failure")
	patch := mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "Allocate")).
		To(func(context.Context) (uint64, error) { return 0, failure }).Build()
	_, err := env.append(cdcReplayCommit(t), nil)
	patch.UnPatch()
	require.ErrorIs(t, err, failure)
	oldSession, err := env.txnManager.GetSessionOfTxn(1)
	require.NoError(t, err)
	beginFailure := errors.New("Begin WAL append failed")
	_, err = env.append(cdcReplayBegin(), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, beginFailure
	})
	require.ErrorIs(t, err, beginFailure)
	pending := env.partial.state.getTxn(1)
	require.NotNil(t, pending)
	require.False(t, pending.observedBegin, "failed Begin must not authorize Commit")
	require.Zero(t, pending.pks.Len())
	require.Nil(t, pending.meta)
	oldSession.Cleanup()
	require.NotNil(t, env.partial.state.getTxn(1))
	failedSession, err := env.txnManager.GetSessionOfTxn(1)
	require.NoError(t, err)
	_, err = env.append(cdcReplayBegin(), nil)
	require.NoError(t, err)
	failedSession.Cleanup()
	require.NotNil(t, env.partial.state.getTxn(1), "failed Begin cleanup must not delete a later successful Begin")
	// No body or Commit is needed for cleanup ownership: a client that vanishes
	// immediately after Begin must still release the new pending record.
	env.txnManager.FailTxnAtVChannel("v1")
	require.Nil(t, env.partial.state.getTxn(1), "the current session must own cleanup immediately after Begin")
}

func newCDCReplayTestEnv(t *testing.T) (*partialUpdateChainTestEnv, replicates.ReplicatesManager) {
	t.Helper()
	begin := cdcReplayBegin().WithTimeTick(10).IntoImmutableMessage(walimplstest.NewTestMessageID(10))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	for idx, body := range cdcReplayBodies(t) {
		builder.Add(body.WithTimeTick(uint64(11 + idx)).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(11 + idx))))
	}
	// Model an optional payload/KMS decode failure only during WAL recovery;
	// the original durable payload remains intact and decodes on full replay.
	patch := mockey.Mock(decodeDeleteBody).To(func(context.Context, message.BasicMessage) (*message.DeleteRequest, error) {
		return nil, errors.New("temporary recovery payload decode failure")
	}).Build()
	env := newPartialUpdateChainTestEnvWithBuilders(t, map[message.TxnID]*message.ImmutableTxnMessageBuilder{1: builder})
	patch.UnPatch()
	env.nextID.Store(20)
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"source-p1"}},
			{ClusterId: "secondary", Pchannels: []string{"p1"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "primary", TargetClusterId: "secondary"}},
	}
	checkpoint := &utility.ReplicateCheckpoint{
		ClusterID: "primary", PChannel: "source-p1", MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1,
	}
	snapshot := env.buildParam.InitialRecoverSnapshot
	snapshot.PChannelControl = &streamingpb.PChannelRecoveryControlMeta{
		ReplicateConfig: config, ReplicateCheckpoint: checkpoint.IntoProto(),
	}
	manager, err := replicates.RecoverReplicateManager(&replicates.ReplicateManagerRecoverParam{
		ChannelInfo: env.buildParam.ChannelInfo, CurrentClusterID: "secondary", InitialRecoverSnapshot: snapshot,
	})
	require.NoError(t, err)
	env.buildParam.ReplicateManager = manager
	env.chain = interceptors.NewChainedInterceptor(replicate.NewInterceptorBuilder().Build(env.buildParam), env.chain)
	return env, manager
}

func cdcReplayBegin() message.MutableMessage {
	return message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}).
		WithReplicateHeader(newChainTestReplicateHeader(10, 100))
}

func cdcReplayBodies(t *testing.T) []message.MutableMessage {
	ctx := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	return []message.MutableMessage{
		newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(5, 999)).
			WithTxnContext(ctx).WithReplicateHeader(newChainTestReplicateHeader(11, 100)),
		newGuardTestDelete(20).WithTxnContext(ctx).WithReplicateHeader(newChainTestReplicateHeader(12, 100)),
	}
}

func cdcReplayCommit(t *testing.T) message.MutableMessage {
	return newCASCommitTxnMessage(t, "v1", 1, 0).WithReplicateHeader(newChainTestReplicateHeader(13, 100))
}
