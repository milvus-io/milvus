package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLogicalStreamFailureReachesSharedBuffersAndLaterReferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	first, err := buffer.Acquire(ctx, newTestQueryView("p_1v0", 50))
	require.NoError(t, err)
	defer first.Release()
	second, err := buffer.Acquire(ctx, newTestQueryView("p_2v0", 50))
	require.NoError(t, err)
	defer second.Release()
	failure := merr.WrapErrServiceUnavailableMsg("logical stream stopped")
	streams.stream("p").failStream(failure)
	require.ErrorIs(t, first.WaitTransformVisible(ctx, 60), failure)
	require.ErrorIs(t, second.WaitTransformVisible(ctx, 60), failure)

	// Handler closure has notified the existing references.
	// A later VChannel must not keep retrying this already-closed logical stream.
	later, err := buffer.Bind(newTestQueryView("p_3v0", 50))
	require.NoError(t, err)
	defer later.Release()
	retryable, err := later.Prepare(ctx)
	require.False(t, retryable)
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, streams.callCount("p"))
}

func TestLogicalStreamFailureDuringSubscribeIsTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	failure := merr.WrapErrServiceUnavailableMsg("stream stopped during subscribe")
	patch := mockey.Mock((*fakeStream).Subscribe).To(func(s *fakeStream, _ context.Context, _ wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
		s.failStream(failure)
		return nil, failure
	}).Build()
	defer patch.UnPatch()
	buffer := New(newFakeStreamManager(), 1)
	guard, err := buffer.Bind(newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()
	retryable, err := guard.Prepare(ctx)
	require.False(t, retryable)
	require.ErrorIs(t, err, failure)
}

func TestNormalLogicalStreamCloseDoesNotSynthesizeFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	guard, err := buffer.Acquire(ctx, newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()
	require.NoError(t, streams.stream("v1").Close())
	require.NoError(t, streams.stream("v1").Error())
	buffer.channels["v1"].mu.Lock()
	require.NoError(t, buffer.channels["v1"].err)
	buffer.channels["v1"].mu.Unlock()
	cancel()
	require.ErrorIs(t, guard.WaitTransformVisible(ctx, 60), context.Canceled)
}
