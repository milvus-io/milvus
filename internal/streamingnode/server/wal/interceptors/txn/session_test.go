package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestSession(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()

	m := NewTxnManager()
	session, err := m.BeginNewTxn(ctx, 0, 10*time.Millisecond)
	assert.NotNil(t, session)
	assert.NoError(t, err)

	// Test Begin
	assert.Equal(t, message.TxnStateBegin, session.state)
	assert.False(t, session.IsExpiredOrDone(0))
	expiredTs := tsoutil.AddPhysicalDurationOnTs(0, 10*time.Millisecond)
	assert.True(t, session.IsExpiredOrDone(expiredTs))
	session.BeginRollback()
	assert.Equal(t, message.TxnStateRollbacked, session.state)
	assert.True(t, session.IsExpiredOrDone(0))

	session, err = m.BeginNewTxn(ctx, 0, 10*time.Millisecond)
	assert.NoError(t, err)
	session.BeginDone()
	assert.Equal(t, message.TxnStateInFlight, session.state)
	assert.False(t, session.IsExpiredOrDone(0))

	// Test add new message
	err = session.AddNewMessage(ctx, expiredTs)
	assert.Error(t, err)
	serr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	// Test add new message after expire, should expired forever.
	err = session.AddNewMessage(ctx, 0)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	session, err = m.BeginNewTxn(ctx, 0, 10*time.Millisecond)
	assert.NoError(t, err)
	session.BeginDone()
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
	session, err = m.BeginNewTxn(ctx, 0, 10*time.Millisecond)
	assert.NoError(t, err)
	session.BeginDone()
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
	session, _ = m.BeginNewTxn(context.Background(), 0, 10*time.Millisecond)
	session.BeginDone()
	// Rollback expired.
	err = session.RequestRollback(context.Background(), expiredTs)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	// Rollback success
	session, _ = m.BeginNewTxn(context.Background(), 0, 10*time.Millisecond)
	session.BeginDone()
	err = session.RequestRollback(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, message.TxnStateOnRollback, session.state)
}

func TestManager(t *testing.T) {
	resource.InitForTest(t)
	m := NewTxnManager()

	wg := &sync.WaitGroup{}

	wg.Add(20)
	count := atomic.NewInt32(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			defer wg.Done()
			session, err := m.BeginNewTxn(context.Background(), 0, time.Duration(i+1)*time.Millisecond)
			assert.NoError(t, err)
			assert.NotNil(t, session)
			session.BeginDone()

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

func TestWithCo(t *testing.T) {
	session := &TxnSession{}
	ctx := WithTxnSession(context.Background(), session)

	session = GetTxnSessionFromContext(ctx)
	assert.NotNil(t, session)
}
