package adaptor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestOpenerAdaptorFailure(t *testing.T) {
	basicOpener := mock_walimpls.NewMockOpenerImpls(t)
	errExpected := errors.New("test")
	basicOpener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, boo *walimpls.OpenOption) (walimpls.WALImpls, error) {
		return nil, errExpected
	})

	opener := adaptImplsToOpener(basicOpener, nil)
	l, err := opener.Open(context.Background(), &wal.OpenOption{})
	assert.ErrorIs(t, err, errExpected)
	assert.Nil(t, l)
}

func TestOpenerAdaptor(t *testing.T) {
	// Build basic opener.
	basicOpener := mock_walimpls.NewMockOpenerImpls(t)
	basicOpener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, boo *walimpls.OpenOption) (walimpls.WALImpls, error) {
			wal := mock_walimpls.NewMockWALImpls(t)

			wal.EXPECT().Channel().Return(boo.Channel)
			wal.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
					return nil, nil
				})
			wal.EXPECT().Close().Run(func() {})
			return wal, nil
		})

	basicOpener.EXPECT().Close().Run(func() {})

	// Create a opener with mock basic opener.
	opener := adaptImplsToOpener(basicOpener, nil)

	// Test in concurrency env.
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wal, err := opener.Open(context.Background(), &wal.OpenOption{
				Channel: types.PChannelInfo{
					Name: fmt.Sprintf("test_%d", i),
					Term: int64(i),
				},
			})
			if err != nil {
				assert.Nil(t, wal)
				assertShutdownError(t, err)
				return
			}
			assert.NotNil(t, wal)

			for {
				msg := mock_message.NewMockMutableMessage(t)
				msg.EXPECT().WithWALTerm(mock.Anything).Return(msg).Maybe()

				msgID, err := wal.Append(context.Background(), msg)
				time.Sleep(time.Millisecond * 10)
				if err != nil {
					assert.Nil(t, msgID)
					assertShutdownError(t, err)
					return
				}
			}
		}(i)
	}
	time.Sleep(time.Second * 1)
	opener.Close()

	// All wal should be closed with Opener.
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-time.After(time.Second * 3):
		t.Errorf("opener close should be fast")
	case <-ch:
	}

	// open a wal after opener closed should return shutdown error.
	_, err := opener.Open(context.Background(), &wal.OpenOption{
		Channel: types.PChannelInfo{
			Name: "test_after_close",
			Term: int64(1),
		},
	})
	assertShutdownError(t, err)
}
