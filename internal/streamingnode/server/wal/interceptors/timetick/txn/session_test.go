package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/stretchr/testify/assert"
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

	assert.Equal(t, message.TxnStateBegin, session.state)
	assert.False(t, session.IsExpiredOrDone(0))
	expiredTs := tsoutil.AddPhysicalDurationOnTs(0, 10*time.Millisecond)
	assert.True(t, session.IsExpiredOrDone(expiredTs))
	session.BeginRollback()
	assert.Equal(t, message.TxnStateRollbacked, session.state)
	assert.True(t, session.IsExpiredOrDone(0))

	session, err = m.BeginNewTxn(ctx, 0, 10*time.Millisecond)
	session.BeginDone()
	assert.Equal(t, message.TxnStateInFlight, session.state)
	assert.False(t, session.IsExpiredOrDone(0))

	// Add a expired message
	err = session.AddNewMessage(ctx, expiredTs)
	assert.Error(t, err)
	serr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	// Add a not expired message
	err = session.AddNewMessage(ctx, 0)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	//  should be timeout, because there's a message not done.
	err = session.RequestCommitAndWait(ctx, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// should expired.
	err = session.RequestCommitAndWait(ctx, expiredTs)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	session.AddNewMessageDone()
	// session is on commited, so new message will be rejected.
	err = session.AddNewMessage(ctx, 0)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE, serr.Code)

	// test rollbacked
	session, _ = m.BeginNewTxn(context.Background(), 0, 10*time.Millisecond)
	session.BeginDone()
	err = session.RequestRollback(expiredTs)
	assert.Error(t, err)
	serr = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, serr.Code)

	err = session.RequestRollback(0)
	assert.NoError(t, err)
	assert.Equal(t, message.TxnStateRollbacked, session.state)
}

func TestManager(t *testing.T) {
	resource.InitForTest(t)
	m := NewTxnManager()

	wg := &sync.WaitGroup{}

	wg.Add(20)
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
			if i%3 == 0 {
				err := session.RequestCommitAndWait(context.Background(), 0)
				assert.NoError(t, err)
			} else if i%3 == 1 {
				err := session.RequestRollback(0)
				assert.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	closed := make(chan struct{}, 0)
	go func() {
		m.GracefulClose()
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
}
