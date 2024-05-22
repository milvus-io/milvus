package extends

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWalExtendImplReadFail(t *testing.T) {
	l := mock_wal.NewMockBasicWAL(t)
	expectedErr := errors.New("test")
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, ro wal.ReadOption) (wal.Scanner, error) {
			return nil, expectedErr
		})

	lExtend := newWALExtend(l)
	scanner, err := lExtend.Read(context.Background(), wal.ReadOption{})
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, scanner)
}

func TestWalExtendImpl(t *testing.T) {
	// Create a mock WAL implementation
	l := mock_wal.NewMockBasicWAL(t)
	l.EXPECT().Channel().Return(&logpb.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, mm message.MutableMessage) (mqwrapper.MessageID, error) {
			return nil, nil
		})
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ro wal.ReadOption) (wal.Scanner, error) {
		scanner := mock_wal.NewMockScanner(t)
		ch := make(chan message.ImmutableMessage, 1)
		scanner.EXPECT().Chan().Return(ch)
		scanner.EXPECT().Close().RunAndReturn(func() error {
			close(ch)
			return nil
		})
		return scanner, nil
	})
	l.EXPECT().GetLatestMessageID(mock.Anything).RunAndReturn(func(ctx context.Context) (mqwrapper.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Close().Return()

	lExtend := newWALExtend(l)
	assert.NotNil(t, lExtend.Channel())
	_, err := lExtend.Append(context.Background(), nil)
	assert.NoError(t, err)
	lExtend.AppendAsync(context.Background(), nil, func(mi message.MessageID, err error) {
		assert.Nil(t, err)
	})
	_, err = lExtend.GetLatestMessageID(context.Background())
	assert.NoError(t, err)

	// Test in concurrency env.
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			scanner, err := lExtend.Read(context.Background(), wal.ReadOption{})
			if err != nil {
				assertShutdownError(t, err)
				return
			}
			assert.NoError(t, err)
			<-scanner.Chan()
		}(i)
	}
	time.Sleep(time.Second * 1)
	lExtend.Close()

	// All wal should be closed with Opener.
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-time.After(time.Second * 3):
		t.Errorf("wal close should be fast")
	case <-ch:
	}

	_, err = lExtend.Append(context.Background(), nil)
	assertShutdownError(t, err)
	lExtend.AppendAsync(context.Background(), nil, func(mi message.MessageID, err error) {
		assertShutdownError(t, err)
	})
	_, err = lExtend.GetLatestMessageID(context.Background())
	assertShutdownError(t, err)
	_, err = lExtend.Read(context.Background(), wal.ReadOption{})
	assertShutdownError(t, err)
}

func assertShutdownError(t *testing.T, err error) {
	e := status.AsLogError(err)
	assert.Equal(t, e.Code, logpb.LogCode_LOG_CODE_ON_SHUTDOWN)
}
