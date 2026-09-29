package transformlogbuffer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type poisonObserverSegment struct{ qnview.TransformSegment }

func (*poisonObserverSegment) OnTransformFailed(uint64, error) { panic("mock required") }

func TestPoisonPublishedBeforeVisibilityAndStopsApply(t *testing.T) {
	for _, catchup := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "catchup"}[catchup], func(t *testing.T) {
			b := newVChannelBuffer(nil, "p", "v", 0)
			bad := &fakeSegment{id: 1}
			healthy := &fakeSegment{id: 2}
			var calls atomic.Int32
			patch := mockey.Mock((*fakeSegment).ApplyTransform).To(func(s *fakeSegment, _ context.Context, e *streamingpb.TransformLogEntry) error {
				if s == bad {
					calls.Add(1)
					return merr.WrapErrServiceUnavailableMsg("injected delete failure")
				}
				s.mu.Lock()
				s.applied = append(s.applied, e.GetTimeTick())
				s.mu.Unlock()
				return nil
			}).Build()
			defer patch.UnPatch()
			var poisoned atomic.Bool
			observer := mockey.Mock((*poisonObserverSegment).OnTransformFailed).To(func(_ *poisonObserverSegment, ts uint64, err error) {
				require.Equal(t, uint64(10), ts)
				require.Error(t, err)
				if !catchup {
					require.Less(t, b.visibleTimeTick, ts)
				}
				poisoned.Store(true)
			}).Build()
			defer observer.UnPatch()
			reg := newRegistration(b, &poisonObserverSegment{bad})
			goodReg := newRegistration(b, healthy)
			defer reg.Unregister()
			defer goodReg.Unregister()
			b.live[2] = goodReg
			if catchup {
				b.pending[1] = reg
			} else {
				b.live[1] = reg
			}
			require.NoError(t, b.onEntry(&streamingpb.TransformLogEntry{TimeTick: 10}))
			require.NoError(t, b.onSyncUp(10))
			if catchup {
				require.NoError(t, b.drainRegistration(context.Background(), reg))
			}
			require.True(t, poisoned.Load())
			require.NoError(t, b.onEntry(&streamingpb.TransformLogEntry{TimeTick: 20}))
			require.NoError(t, b.waitTransformVisible(context.Background(), 20))
			require.EqualValues(t, 1, calls.Load())
			require.Equal(t, []uint64{10, 20}, healthy.applied)
			require.Nil(t, b.err)
		})
	}
}

func TestOldRegistrationCannotRemoveReplacement(t *testing.T) {
	b := newVChannelBuffer(nil, "p", "v", 0)
	old := newRegistration(b, &fakeSegment{id: 1})
	current := newRegistration(b, &fakeSegment{id: 1})
	defer current.Unregister()
	b.pending[1] = current
	old.Unregister()
	b.removeRegistration(old) // late drain cleanup
	require.Same(t, current, b.pending[1])
	_, done, _, err := b.nextCatchupBatch(old)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, done)
}

func TestUnregisterWaitsForNativeApply(t *testing.T) {
	b := newVChannelBuffer(nil, "p", "v", 0)
	entered, finish := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*fakeSegment).ApplyTransform).To(func(*fakeSegment, context.Context, *streamingpb.TransformLogEntry) error {
		close(entered)
		<-finish
		return nil
	}).Build()
	defer patch.UnPatch()
	reg := newRegistration(b, &fakeSegment{id: 1})
	applied, unregistered := make(chan struct{}), make(chan struct{})
	go func() { _ = reg.applyEntry(&streamingpb.TransformLogEntry{TimeTick: 10}); close(applied) }()
	<-entered
	go func() { reg.Unregister(); close(unregistered) }()
	select {
	case <-unregistered:
		t.Fatal("native Apply still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	<-applied
	<-unregistered
}

func TestLegacyApplyFailureAndCanceledRegistrationFailClosed(t *testing.T) {
	b := newVChannelBuffer(nil, "p", "v", 0)
	failure := merr.WrapErrServiceUnavailableMsg("legacy consumer cannot publish Poison")
	patch := mockey.Mock((*fakeSegment).ApplyTransform).Return(failure).Build()
	defer patch.UnPatch()
	reg := newRegistration(b, &fakeSegment{id: 1})
	b.live[1] = reg
	require.ErrorIs(t, b.onEntry(&streamingpb.TransformLogEntry{TimeTick: 10}), failure)
	require.ErrorIs(t, b.onSyncUp(20), failure)
	require.ErrorIs(t, b.onEntry(&streamingpb.TransformLogEntry{TimeTick: 30}), failure)
	require.Zero(t, b.visibleTimeTick)
	require.ErrorIs(t, b.waitTransformVisible(context.Background(), 10), failure)
	reg.Unregister()
	require.ErrorIs(t, reg.applyEntry(&streamingpb.TransformLogEntry{TimeTick: 40}), context.Canceled)
}
