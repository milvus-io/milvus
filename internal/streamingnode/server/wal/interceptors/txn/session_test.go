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
