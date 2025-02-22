package broadcast

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/client/mock_broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type mockBuilder struct {
	built func(ctx context.Context) (Watcher, error)
}

func (b *mockBuilder) Build(ctx context.Context) (Watcher, error) {
	return b.built(ctx)
}

func TestWatcherResuming(t *testing.T) {
	ctx := context.Background()
	b := newMockWatcherBuilder(t)
	rw := newResumingWatcher(b, &typeutil.BackoffTimerConfig{
		Default: 500 * time.Millisecond,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 10 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     500 * time.Millisecond,
		},
	})
	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := rand.Int31n(10)
			rk := message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey(fmt.Sprintf("c%d", id)))
			err := rw.ObserveResourceKeyEvent(ctx, rk)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	rw.Close()
	err := rw.ObserveResourceKeyEvent(ctx, message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c1")))
	assert.ErrorIs(t, err, errWatcherClosed)
}

func newMockWatcherBuilder(t *testing.T) WatcherBuilder {
	return &mockBuilder{built: func(ctx context.Context) (Watcher, error) {
		w := mock_broadcast.NewMockWatcher(t)
		n := rand.Int31n(10)
		if n < 3 {
			return nil, errors.New("err")
		}

		// ill watcher
		k := atomic.NewInt32(n)
		o := rand.Int31n(20) + n
		mu := sync.Mutex{}
		closed := false
		output := make(chan *message.BroadcastEvent, 500)
		w.EXPECT().ObserveResourceKeyEvent(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, be *messagespb.BroadcastEvent) error {
			k2 := k.Inc()
			if k2 >= o {
				return errors.New("err")
			}
			mu.Lock()
			if closed {
				return errors.New("closed")
			}
			go func() {
				defer mu.Unlock()
				time.Sleep(time.Duration(rand.Int31n(5)) * time.Millisecond)
				output <- be
			}()
			return nil
		}).Maybe()
		w.EXPECT().EventChan().RunAndReturn(func() <-chan *messagespb.BroadcastEvent {
			mu.Lock()
			defer mu.Unlock()
			if !closed && rand.Int31n(100) < 50 {
				close(output)
				closed = true
			}
			return output
		}).Maybe()
		w.EXPECT().Close().Return()
		return w, nil
	}}
}
