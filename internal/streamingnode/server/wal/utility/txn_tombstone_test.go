package utility

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func testTombstone(id int64, milliseconds int64) *TxnTombstone {
	return &TxnTombstone{VChannel: "v1", MessageID: walimplstest.NewTestMessageID(id), Result: ExtraAppendResult{
		TimeTick: tsoutil.ComposeTS(milliseconds, 0), LastConfirmedMessageID: walimplstest.NewTestMessageID(0),
		TxnCtx: &message.TxnContext{TxnID: message.TxnID(id), Keepalive: time.Second},
	}}
}

func TestTxnTombstoneRetention(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.StreamingCfg.TxnTombstoneMaxCount.Key, "2"))
	require.NoError(t, params.Save(params.StreamingCfg.TxnTombstoneMaxLifetime.Key, "1s"))
	defer params.Reset(params.StreamingCfg.TxnTombstoneMaxCount.Key)
	defer params.Reset(params.StreamingCfg.TxnTombstoneMaxLifetime.Key)
	results := NewTxnTombstones()
	first := testTombstone(1, 1000)
	first.Result.Extra = &messagespb.CommitTxnMessageHeader{BodyCnt: 5}
	results.Add(first)
	first.Result.TxnCtx.TxnID = 99
	first.Result.Extra.(*messagespb.CommitTxnMessageHeader).BodyCnt = 99
	hit := results.Get(1, "v1")
	require.Equal(t, message.TxnID(1), hit.Result.TxnCtx.TxnID)
	require.EqualValues(t, 5, hit.Result.Extra.(*messagespb.CommitTxnMessageHeader).BodyCnt)
	hit.Result.TxnCtx.TxnID = 100
	require.Equal(t, message.TxnID(1), results.Get(1, "v1").Result.TxnCtx.TxnID)
	require.Nil(t, results.Get(1, "another-vchannel"))
	// Completion order can differ from allocation order; evict by original TT.
	results.Add(testTombstone(3, 1200))
	results.Add(testTombstone(2, 1100))
	require.Nil(t, results.Get(1, "v1"))
	require.NotNil(t, results.Get(2, "v1"))
	results.Add(testTombstone(2, 1900)) // Must not renew the original result.
	require.Equal(t, tsoutil.ComposeTS(1100, 0), results.Get(2, "v1").Result.TimeTick)
	results.Advance(tsoutil.ComposeTS(2100, 0))
	require.Nil(t, results.Get(2, "v1"))
	require.NotNil(t, results.Get(3, "v1"))
	results.Advance(tsoutil.ComposeTS(2200, 0))
	require.Nil(t, results.Get(3, "v1"))
	// Old replay must not reset a result's age, or move the watermark backwards.
	results.Add(testTombstone(4, 1000))
	require.Nil(t, results.Get(4, "v1"))
	require.NoError(t, params.Save(params.StreamingCfg.TxnTombstoneMaxCount.Key, "0"))
	results.Add(testTombstone(5, 2300))
	require.Nil(t, results.Get(5, "v1"))
	require.NoError(t, params.Save(params.StreamingCfg.TxnTombstoneMaxCount.Key, "2"))
	require.NoError(t, params.Save(params.StreamingCfg.TxnTombstoneMaxLifetime.Key, "0s"))
	results.Add(testTombstone(6, 2400))
	require.Nil(t, results.Get(6, "v1"))
}

func TestTxnTombstoneReplay(t *testing.T) {
	paramtable.Init()
	for _, replicated := range []bool{false, true} {
		txnCtx := message.TxnContext{TxnID: 1, Keepalive: time.Second}
		begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().WithTimeTick(1).WithLastConfirmedUseMessageID().WithTxnContext(txnCtx).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable().WithTimeTick(3).WithLastConfirmedUseMessageID().WithTxnContext(txnCtx)
		if replicated {
			commit = commit.WithReplicateHeader(&message.ReplicateHeader{
				ClusterID: "source", VChannel: "v1", TimeTick: 3,
				MessageID: walimplstest.NewTestMessageID(3), LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
			})
		}
		transaction, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).
			Build(message.MustAsImmutableCommitTxnMessageV2(commit.IntoImmutableMessage(walimplstest.NewTestMessageID(3))))
		require.NoError(t, err)
		results := NewTxnTombstones()
		results.Observe(begin)
		require.Nil(t, results.Get(1, "v1"))
		results.Observe(transaction)
		if replicated {
			require.Nil(t, results.Get(1, "v1"))
			continue
		}
		result := results.Get(1, "v1")
		require.NotNil(t, result)
		require.True(t, result.MessageID.EQ(transaction.MessageID()))
		require.Equal(t, transaction.TimeTick(), result.Result.TimeTick)
		require.True(t, result.Result.LastConfirmedMessageID.EQ(transaction.LastConfirmedMessageID()))
	}
}
