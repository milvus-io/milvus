package transformlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func TestRejectedConsumerReleasesOnlyItsRemoteReader(t *testing.T) {
	for _, entry := range []bool{false, true} {
		name := "sync_up"
		if entry {
			name = "entry"
		}
		t.Run(name, func(t *testing.T) {
			for _, failure := range []error{context.Canceled, context.DeadlineExceeded, status.NewUnknownError("consumer rejected")} {
				t.Run(failure.Error(), func(t *testing.T) {
					source, factory := fixture(t)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					stream := resumable.NewResumableStream(ctx, "p", factory)
					defer stream.Close()
					healthy := newConsumer()
					other, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: healthy})
					require.NoError(t, err)
					defer other.Close()
					through(t, healthy, 0)
					local := <-source.opened
					if entry {
						observe(source, "p_1v0", 10)
						through(t, healthy, 10)
					}
					rejected := newConsumer()
					rejected.fail = failure
					sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: rejected})
					if err != nil {
						require.ErrorIs(t, err, failure)
					}
					select {
					case <-rejected.done:
					case <-ctx.Done():
						t.Fatal("rejected consumer was not closed")
					}
					if sub != nil {
						require.ErrorIs(t, sub.Close(), failure)
					}
					require.Eventually(t, func() bool {
						source.mu.Lock()
						defer source.mu.Unlock()
						if len(source.subscribers) != 1 {
							return false
						}
						for sub := range source.subscribers {
							if sub.opt.VChannel != "p_2v0" {
								return false
							}
						}
						return true
					}, time.Second, time.Millisecond, "rejected consumer left a server reader running")
					observe(source, "p_2v0", 20)
					require.Equal(t, []uint64{20}, through(t, healthy, 20))
					require.NoError(t, local.ctx.Err())
					require.Empty(t, source.opened, "logical rejection must not reconnect the PChannel")
				})
			}
		})
	}
}
