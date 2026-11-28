package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestRetiredTransactionPreservesDefiniteExpiration(t *testing.T) {
	for _, terminal := range []string{"expire", "abort", "rollback", "reject commit"} {
		t.Run(terminal, func(t *testing.T) {
			resource.InitForTest(t)
			m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
			ctx := context.Background()
			s, err := m.BeginNewTxn(ctx, newBeginTxnMessage(1, time.Second))
			require.NoError(t, err)
			cleaned := 0
			s.RegisterCleanup(func() { cleaned++ }, 1)
			switch terminal {
			case "expire":
				m.CleanupTxnUntil(s.expiredTimeTick())
				require.True(t, s.IsExpiredOrDone(1), "terminal expiration releases the LCMID pin")
				// A request holding the old pointer and an older timestamp cannot
				// commit after cleanup published the terminal result.
				require.True(t, status.AsStreamingError(s.RequestCommitAndWait(ctx, 1, 0)).IsTxnExpired())
				require.True(t, status.AsStreamingError(s.AddNewMessage(ctx, 1)).IsTxnExpired())
			case "abort":
				m.FailTxnAtVChannel("v1", 1)
			case "rollback":
				require.NoError(t, s.RequestRollback(ctx, 1))
				s.RollbackDone()
				m.CleanupTxnUntil(1)
			case "reject commit":
				require.NoError(t, s.AddNewMessage(ctx, 1))
				s.AddNewMessageDoneAndKeepalive(1)
				require.NoError(t, s.RequestCommitAndWait(ctx, 1, 1))
				s.RejectCommit()
				m.CleanupTxnUntil(1)
			}
			require.Equal(t, 1, cleaned)
			require.Empty(t, m.sessions)
			_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
			require.True(t, status.AsStreamingError(err).IsTxnExpired(), "%v", err)
			require.NoError(t, m.GracefulClose(ctx), "terminal records must not block shutdown")
		})
	}
}

func TestExpiredTxnRetentionBounds(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().StreamingCfg
	require.NoError(t, paramtable.Get().Save(cfg.TxnTombstoneMaxCount.Key, "2"))
	require.NoError(t, paramtable.Get().Save(cfg.TxnTombstoneMaxLifetime.Key, "1s"))
	t.Cleanup(func() {
		paramtable.Get().Reset(cfg.TxnTombstoneMaxCount.Key)
		paramtable.Get().Reset(cfg.TxnTombstoneMaxLifetime.Key)
	})
	var records expiredTxns
	start := tsoutil.ComposeTS(1000, 0)
	records.advance(start)
	records.add(1)
	records.advance(tsoutil.AddPhysicalDurationOnTs(start, 500*time.Millisecond))
	records.add(1) // Duplicate retirement must not renew the original age.
	records.add(2)
	records.add(3)
	require.False(t, records.contains(1), "capacity removes the oldest result")
	require.True(t, records.contains(2))
	records.remove(2) // A replicated Begin may reuse a source TxnID.
	records.add(2)
	records.advance(start) // A regressing tick must not move the clock back.
	records.advance(tsoutil.AddPhysicalDurationOnTs(start, 1500*time.Millisecond))
	require.False(t, records.contains(2))
	require.False(t, records.contains(3))
	require.Empty(t, records.entries)
	require.Zero(t, records.order.Len())

	var aging expiredTxns
	aging.advance(start)
	aging.add(1)
	aging.advance(tsoutil.AddPhysicalDurationOnTs(start, 500*time.Millisecond))
	aging.add(1)
	aging.advance(tsoutil.AddPhysicalDurationOnTs(start, time.Second))
	require.False(t, aging.contains(1), "duplicate retirement does not refresh TTL")
	for _, disabled := range []string{cfg.TxnTombstoneMaxCount.Key, cfg.TxnTombstoneMaxLifetime.Key} {
		require.NoError(t, paramtable.Get().Save(disabled, "0"))
		records.add(4)
		require.False(t, records.contains(4))
		paramtable.Get().Reset(disabled)
	}
}

func TestExpiredTxnEvictionAndReplicaReuse(t *testing.T) {
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
	ctx := context.Background()
	s, err := m.BeginNewTxn(ctx, newBeginTxnMessage(1, time.Second))
	require.NoError(t, err)
	m.FailTxnAtVChannel("v1", 1)
	_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	successor := NewTxnManager(types.PChannelInfo{Name: t.Name() + "-successor"}, nil)
	_, err = successor.GetSessionOfTxn(s.TxnContext().TxnID)
	require.ErrorContains(t, err, "not found", "handoff without evidence must not imply expiration")
	lifetime := paramtable.Get().StreamingCfg.TxnTombstoneMaxLifetime.GetAsDurationByParse()
	m.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(1, lifetime))
	_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
	require.ErrorContains(t, err, "not found")
	require.False(t, status.AsStreamingError(err).IsTxnExpired())

	// Reuse a recovered replica's source TxnID after an explicit abort. A later
	// committed attempt must not inherit the earlier attempt's Expired result.
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(1).WithTxnContext(s.TxnContext()).WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(0), TimeTick: 1, VChannel: "v1"})
	replica, err := m.BeginNewTxn(ctx, message.MustAsMutableBeginTxnMessageV2(begin))
	require.NoError(t, err)
	require.Equal(t, s.TxnContext().TxnID, replica.TxnContext().TxnID)
	m.FailTxnAtVChannel("v1", 1)
	replica, err = m.BeginNewTxn(ctx, message.MustAsMutableBeginTxnMessageV2(begin))
	require.NoError(t, err)
	require.False(t, m.expired.contains(replica.TxnContext().TxnID))
	require.NoError(t, replica.AddNewMessage(ctx, 1))
	replica.AddNewMessageDoneAndKeepalive(1)
	require.NoError(t, replica.RequestCommitAndWait(ctx, 1, 1))
	replica.CommitDone()
	m.CleanupTxnUntil(1)
	_, err = m.GetSessionOfTxn(replica.TxnContext().TxnID)
	require.ErrorContains(t, err, "not found")
}

func TestCleanupAndCommitAdmissionAgreeOnOutcome(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()
	for range 50 {
		m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
		s, err := m.BeginNewTxn(ctx, newBeginTxnMessage(1, time.Millisecond))
		require.NoError(t, err)
		require.NoError(t, s.AddNewMessage(ctx, 1))
		s.AddNewMessageDoneAndKeepalive(1)
		start := make(chan struct{})
		var admitted error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; admitted = s.RequestCommitAndWait(ctx, 1, 1) }()
		go func() { defer wg.Done(); <-start; m.CleanupTxnUntil(s.expiredTimeTick()) }()
		close(start)
		wg.Wait()
		_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
		if admitted == nil {
			require.ErrorContains(t, err, "not found", "an admitted commit is unresolved, not expired")
			require.False(t, status.AsStreamingError(err).IsTxnExpired())
		} else {
			require.True(t, status.AsStreamingError(admitted).IsTxnExpired())
			require.True(t, status.AsStreamingError(err).IsTxnExpired())
		}
	}
}

func TestScopeAbortDoesNotInventCommitFailure(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()
	for _, committed := range []bool{false, true} {
		m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
		s, err := m.BeginNewTxn(ctx, newBeginTxnMessage(1, time.Millisecond))
		require.NoError(t, err)
		require.NoError(t, s.AddNewMessage(ctx, 1))
		s.AddNewMessageDoneAndKeepalive(1)
		require.NoError(t, s.RequestCommitAndWait(ctx, 1, 1))
		if committed {
			s.CommitDone()
		}
		m.FailTxnAtVChannel("", 2)
		_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
		require.ErrorContains(t, err, "not found")
		require.False(t, status.AsStreamingError(err).IsTxnExpired())
	}
}
