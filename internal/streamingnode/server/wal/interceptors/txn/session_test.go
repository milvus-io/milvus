package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestSession(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()

	m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	<-m.RecoverDone()
	session, err := m.BeginNewTxn(ctx, newBeginTxnMessage(0, 10*time.Millisecond))
	assert.Equal(t, session.VChannel(), "v1")
	assert.Equal(t, session.State(), message.TxnStateInFlight)
	assert.NotNil(t, session)
	assert.NoError(t, err)

	// Test add new message
	expiredTs := tsoutil.AddPhysicalDurationOnTs(0, 10*time.Millisecond)
	err = session.AddNewMessage(ctx, expiredTs)
	assert.Error(t, err)
	serr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	// Test add new message after expire, should expired forever.
	err = session.AddNewMessage(ctx, 0)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	session, err = m.BeginNewTxn(ctx, newBeginTxnMessage(0, 10*time.Millisecond))
	assert.NoError(t, err)
	err = session.AddNewMessage(ctx, 0)
	assert.NoError(t, err)
	session.AddNewMessageDoneAndKeepalive(0)

	// Test Commit.
	err = session.RequestCommitAndWait(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, message.TxnStateOnCommit, session.state)
	session.CommitDone()
	assert.Equal(t, message.TxnStateCommitted, session.state)

	// Test Commit timeout.
	session, err = m.BeginNewTxn(ctx, newBeginTxnMessage(0, 10*time.Millisecond))
	assert.NoError(t, err)
	err = session.AddNewMessage(ctx, 0)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err = session.RequestCommitAndWait(ctx, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Test Commit Expired
	err = session.RequestCommitAndWait(ctx, expiredTs)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	// Test Rollback
	session, _ = m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Millisecond))
	// Rollback expired.
	err = session.RequestRollback(context.Background(), expiredTs)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	// Rollback success
	session, _ = m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Millisecond))
	err = session.RequestRollback(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, message.TxnStateOnRollback, session.state)
}

func TestManager(t *testing.T) {
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)

	wg := &sync.WaitGroup{}

	wg.Add(20)
	count := atomic.NewInt32(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			defer wg.Done()
			session, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, time.Duration(i+1)*time.Millisecond))
			assert.NoError(t, err)
			assert.NotNil(t, session)

			session, err = m.GetSessionOfTxn(session.TxnContext().TxnID)
			assert.NoError(t, err)
			assert.NotNil(t, session)

			session.RegisterCleanup(func() {
				count.Dec()
			}, 0)
			if i%3 == 0 {
				err := session.RequestCommitAndWait(context.Background(), 0)
				session.CommitDone()
				assert.NoError(t, err)
			} else if i%3 == 1 {
				err := session.RequestRollback(context.Background(), 0)
				assert.NoError(t, err)
				session.RollbackDone()
			}
		}(i)
	}
	wg.Wait()

	closed := make(chan struct{})
	go func() {
		m.GracefulClose(context.Background())
		close(closed)
	}()

	select {
	case <-closed:
		t.Errorf("manager should not be closed")
	case <-time.After(10 * time.Millisecond):
	}

	expiredTs := tsoutil.AddPhysicalDurationOnTs(0, 10*time.Millisecond)
	m.CleanupTxnUntil(expiredTs)
	select {
	case <-closed:
		t.Errorf("manager should not be closed")
	case <-time.After(10 * time.Millisecond):
	}

	m.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(0, 20*time.Millisecond))
	select {
	case <-closed:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("manager should be closed")
	}

	assert.Equal(t, int32(0), count.Load())
}

func TestManagerRecoverAndFailAll(t *testing.T) {
	resource.InitForTest(t)
	now := time.Now()
	beginMsg1 := newImmutableBeginTxnMessageWithVChannel("v1", 1, tsoutil.ComposeTSByTime(now, 0), 10*time.Millisecond)
	beginMsg2 := newImmutableBeginTxnMessageWithVChannel("v2", 2, tsoutil.ComposeTSByTime(now, 1), 10*time.Millisecond)
	beginMsg3 := newImmutableBeginTxnMessageWithVChannel("v1", 3, tsoutil.ComposeTSByTime(now, 2), 10*time.Millisecond)
	builders := map[message.TxnID]*message.ImmutableTxnMessageBuilder{
		message.TxnID(1): message.NewImmutableTxnMessageBuilder(beginMsg1),
		message.TxnID(2): message.NewImmutableTxnMessageBuilder(beginMsg2),
		message.TxnID(3): message.NewImmutableTxnMessageBuilder(beginMsg3),
	}

	builders[message.TxnID(1)].Add(message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().
		WithTimeTick(tsoutil.ComposeTSByTime(now, 3)).
		WithLastConfirmedUseMessageID().
		WithTxnContext(message.TxnContext{
			TxnID:     message.TxnID(1),
			Keepalive: 10 * time.Millisecond,
		}).
		IntoImmutableMessage(rmq.NewRmqID(1)))

	m := NewTxnManager(types.PChannelInfo{Name: "test"}, builders)
	select {
	case <-m.RecoverDone():
		t.Errorf("txn manager should not be recovered")
	case <-time.After(1 * time.Millisecond):
	}

	m.FailTxnAtVChannel("v1")
	select {
	case <-m.RecoverDone():
		t.Errorf("txn manager should not be recovered")
	case <-time.After(1 * time.Millisecond):
	}
	m.FailTxnAtVChannel("v2")
	<-m.RecoverDone()
}

func TestWithContext(t *testing.T) {
	session := &TxnSession{}
	ctx := WithTxnSession(context.Background(), session)

	session = GetTxnSessionFromContext(ctx)
	assert.NotNil(t, session)
}

func TestRollbackAllInFlightTransactions(t *testing.T) {
	resource.InitForTest(t)

	t.Run("RollbackWithNoSessions", func(t *testing.T) {
		m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-m.RecoverDone()

		// Should not panic and handle empty case
		m.RollbackAllInFlightTransactions()
	})

	t.Run("RollbackWithActiveSessions", func(t *testing.T) {
		m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-m.RecoverDone()

		// Create 3 active sessions
		cleanupCalled := atomic.NewInt32(0)
		for i := 0; i < 3; i++ {
			session, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Second))
			assert.NoError(t, err)
			assert.NotNil(t, session)

			session.RegisterCleanup(func() {
				cleanupCalled.Inc()
			}, 0)
		}

		// Verify we have 3 active sessions
		m.mu.Lock()
		assert.Equal(t, 3, len(m.sessions))
		m.mu.Unlock()

		// Rollback all in-flight transactions
		m.RollbackAllInFlightTransactions()

		// Verify all sessions are cleaned up
		m.mu.Lock()
		assert.Equal(t, 0, len(m.sessions))
		m.mu.Unlock()

		// Verify cleanup was called for all sessions
		assert.Equal(t, int32(3), cleanupCalled.Load())
	})

	t.Run("RollbackWithMixedSessionStates", func(t *testing.T) {
		m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-m.RecoverDone()

		cleanupCalled := atomic.NewInt32(0)

		// Create 3 sessions with different states
		// Session 1: in-flight
		session1, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Second))
		assert.NoError(t, err)
		session1.RegisterCleanup(func() {
			cleanupCalled.Inc()
		}, 0)

		// Session 2: committed (already done, should be removed from manager)
		session2, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Second))
		assert.NoError(t, err)
		session2.RegisterCleanup(func() {
			cleanupCalled.Inc()
		}, 0)
		err = session2.RequestCommitAndWait(context.Background(), 0)
		assert.NoError(t, err)
		session2.CommitDone()

		// Session 3: in-flight with messages added
		session3, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Second))
		assert.NoError(t, err)
		session3.RegisterCleanup(func() {
			cleanupCalled.Inc()
		}, 0)
		err = session3.AddNewMessage(context.Background(), 0)
		assert.NoError(t, err)
		session3.AddNewMessageDoneAndKeepalive(0)

		// Rollback all in-flight transactions
		m.RollbackAllInFlightTransactions()

		// Verify all sessions are cleaned up
		m.mu.Lock()
		assert.Equal(t, 0, len(m.sessions))
		m.mu.Unlock()

		// All 3 sessions should have cleanup called
		assert.Equal(t, int32(3), cleanupCalled.Load())
	})

	t.Run("RollbackWithRecoveredSessions", func(t *testing.T) {
		// Create a manager with recovered sessions
		now := time.Now()
		beginMsg1 := newImmutableBeginTxnMessageWithVChannel("v1", 1, tsoutil.ComposeTSByTime(now, 0), 10*time.Minute)
		beginMsg2 := newImmutableBeginTxnMessageWithVChannel("v2", 2, tsoutil.ComposeTSByTime(now, 1), 10*time.Minute)

		builders := map[message.TxnID]*message.ImmutableTxnMessageBuilder{
			message.TxnID(1): message.NewImmutableTxnMessageBuilder(beginMsg1),
			message.TxnID(2): message.NewImmutableTxnMessageBuilder(beginMsg2),
		}

		m := NewTxnManager(types.PChannelInfo{Name: "test"}, builders)

		// RecoverDone should not be done yet
		select {
		case <-m.RecoverDone():
			t.Errorf("txn manager should not be recovered yet")
		case <-time.After(1 * time.Millisecond):
		}

		// Rollback all in-flight transactions
		m.RollbackAllInFlightTransactions()

		// Verify all sessions are cleaned up
		m.mu.Lock()
		assert.Equal(t, 0, len(m.sessions))
		m.mu.Unlock()

		// RecoverDone should be signaled because all recovered sessions are cleaned
		select {
		case <-m.RecoverDone():
			// expected
		case <-time.After(100 * time.Millisecond):
			t.Errorf("txn manager should be recovered after rollback")
		}
	})
}

func TestManagerFromReplcateMessage(t *testing.T) {
	resource.InitForTest(t)
	manager := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	immutableMsg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{
			KeepaliveMilliseconds: 10 * time.Millisecond.Milliseconds(),
		}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		WithTxnContext(message.TxnContext{
			TxnID:     18,
			Keepalive: 10 * time.Millisecond,
		}).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	replicateMsg := message.MustNewReplicateMessage("test2", immutableMsg.IntoImmutableMessageProto()).WithTimeTick(2)

	session, err := manager.BeginNewTxn(context.Background(), message.MustAsMutableBeginTxnMessageV2(replicateMsg))
	assert.NoError(t, err)
	assert.NotNil(t, session)
	assert.Equal(t, message.TxnID(18), session.TxnContext().TxnID)
	assert.Equal(t, message.TxnKeepaliveInfinite, session.TxnContext().Keepalive)
}

func TestBeginNewTxnManagerClosed(t *testing.T) {
	// Covers BeginNewTxn lines 92-94: manager closed returns error
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	<-m.RecoverDone()

	// Create an active session so GracefulClose doesn't complete immediately
	session, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Second))
	assert.NoError(t, err)
	assert.NotNil(t, session)

	// Start graceful close in background (won't complete because active session)
	closeCh := make(chan struct{})
	go func() {
		m.GracefulClose(context.Background())
		close(closeCh)
	}()
	time.Sleep(5 * time.Millisecond) // let GracefulClose set m.closed

	// Now BeginNewTxn should fail because manager is closed
	_, err = m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Millisecond))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manager closed")

	// Cleanup: expire the session so GracefulClose completes
	m.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(0, 20*time.Second))
	<-closeCh
}

func TestBuildTxnContextKeepaliveErrors(t *testing.T) {
	resource.InitForTest(t)

	t.Run("keepalive_zero_uses_default", func(t *testing.T) {
		// Covers buildTxnContext lines 112-115: keepalive == 0 uses default
		m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-m.RecoverDone()

		msg := newBeginTxnMessageWithKeepaliveMs("v1", 0, 0)
		session, err := m.BeginNewTxn(context.Background(), msg)
		assert.NoError(t, err)
		assert.NotNil(t, session)
		// Verify keepalive is set to default (not 0)
		assert.True(t, session.TxnContext().Keepalive > 0)
	})

	t.Run("keepalive_negative_returns_error", func(t *testing.T) {
		// Covers buildTxnContext lines 116-118: keepalive < 1ms returns error
		m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-m.RecoverDone()

		// Set keepalive to -1ms (negative value)
		msg := newBeginTxnMessageWithKeepaliveMs("v1", 0, -1)
		_, err := m.BeginNewTxn(context.Background(), msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "keepalive must be greater than 1ms")
	})
}

func TestGetSessionOfTxnNotFound(t *testing.T) {
	// Covers GetSessionOfTxn lines 185-187: txn not found
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	<-m.RecoverDone()

	_, err := m.GetSessionOfTxn(message.TxnID(99999))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGracefulCloseNoSessions(t *testing.T) {
	// Covers GracefulClose lines 225-227: sessions == 0 when closed initialized
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	<-m.RecoverDone()

	// GracefulClose with no active sessions should return immediately
	err := m.GracefulClose(context.Background())
	assert.NoError(t, err)
}

func TestGracefulCloseContextTimeout(t *testing.T) {
	// Covers GracefulClose lines 233-234: context done before close
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	<-m.RecoverDone()

	// Create an active session
	session, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, 10*time.Second))
	assert.NoError(t, err)
	assert.NotNil(t, session)

	// GracefulClose with short timeout should return DeadlineExceeded
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err = m.GracefulClose(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func newBeginTxnMessageWithKeepaliveMs(vchannel string, timetick uint64, keepaliveMs int64) message.MutableBeginTxnMessageV2 {
	msg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: keepaliveMs}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick)
	beginTxnMsg, _ := message.AsMutableBeginTxnMessageV2(msg)
	return beginTxnMsg
}

func newBeginTxnMessage(timetick uint64, keepalive time.Duration) message.MutableBeginTxnMessageV2 {
	return newBeginTxnMessageWithVChannel("v1", timetick, keepalive)
}

func newBeginTxnMessageWithVChannel(vchannel string, timetick uint64, keepalive time.Duration) message.MutableBeginTxnMessageV2 {
	msg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: keepalive.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick)

	beginTxnMsg, _ := message.AsMutableBeginTxnMessageV2(msg)
	return beginTxnMsg
}

func newImmutableBeginTxnMessageWithVChannel(vchannel string, txnID int64, timetick uint64, keepalive time.Duration) message.ImmutableBeginTxnMessageV2 {
	msg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{
			KeepaliveMilliseconds: keepalive.Milliseconds(),
		}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(1)).
		WithLastConfirmedUseMessageID().
		WithTxnContext(message.TxnContext{
			TxnID:     message.TxnID(txnID),
			Keepalive: keepalive,
		}).
		IntoImmutableMessage(rmq.NewRmqID(1))
	return message.MustAsImmutableBeginTxnMessageV2(msg)
}
