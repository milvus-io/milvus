package transformlogbuffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWaitCatchupFailureWhileDrainAdvances(t *testing.T) {
	for _, cancelWait := range []bool{true, false} {
		name := "buffer failure"
		if cancelWait {
			name = "wait canceled"
		}
		t.Run(name, func(t *testing.T) {
			buf := newVChannelBuffer(nil, "p1", "v1", 50)
			segment := &fakeSegment{id: 10, vchannel: "v1", startAfter: 50}
			reg := newRegistration(buf, segment)
			defer reg.Unregister()
			buf.pending[segment.ID()] = reg
			buf.syncUp = true
			for tick := uint64(51); tick <= 2098; tick++ {
				buf.entries = append(buf.entries, &streamingpb.TransformLogEntry{TimeTick: tick})
			}

			started, proceed := make(chan struct{}), make(chan struct{})
			waitersFinished := make(chan struct{})
			var first, release, finishWaiters sync.Once
			defer release.Do(func() { close(proceed) })
			defer finishWaiters.Do(func() { close(waitersFinished) })
			patch := mockey.Mock((*fakeSegment).ApplyTransform).To(func(_ *fakeSegment, _ context.Context, entry *streamingpb.TransformLogEntry) error {
				first.Do(func() {
					close(started)
					<-proceed
				})
				if entry.GetTimeTick() == 2098 {
					// Keep successful completion from racing with the canceled
					// waiters' select; this test exercises progress reads only.
					<-waitersFinished
				}
				return nil
			}).Build()
			defer patch.UnPatch()
			drained := make(chan error, 1)
			go func() {
				err := buf.drainRegistration(reg.ctx, reg)
				reg.finish(err)
				drained <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("drain did not reach ApplyTransform")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := merr.WrapErrServiceUnavailableMsg("injected stream failure")
			expected := failure
			if cancelWait {
				cancel()
				expected = context.Canceled
			} else {
				buf.fail(failure)
			}
			const waiters = 64
			results := make(chan error, waiters)
			for range waiters {
				go func() {
					<-proceed
					results <- reg.WaitCatchup(ctx)
				}()
			}
			// The already running Apply and failed waiters resume together. Neither
			// cancellation nor external failure joins the drain's progress writes.
			release.Do(func() { close(proceed) })
			for range waiters {
				select {
				case err := <-results:
					require.ErrorIs(t, err, expected)
				case <-time.After(5 * time.Second):
					t.Fatal("WaitCatchup did not return")
				}
			}
			finishWaiters.Do(func() { close(waitersFinished) })
			select {
			case err := <-drained:
				if cancelWait {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, failure)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("drain did not finish")
			}
		})
	}
}
