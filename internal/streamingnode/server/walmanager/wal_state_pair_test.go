package walmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

func TestStatePair(t *testing.T) {
	statePair := newWALStatePair()
	currentState := statePair.GetCurrentState()
	expectedState := statePair.GetExpectedState()
	assert.Equal(t, initialCurrentWALState, currentState)
	assert.Equal(t, initialExpectedWALState, expectedState)
	assert.Nil(t, statePair.GetWAL())

	statePair.SetExpectedState(newAvailableExpectedState(context.Background(), types.PChannelInfo{
		Term: 1,
	}))
	assert.Equal(t, "(1,true)", toStateString(statePair.GetExpectedState()))

	statePair.SetExpectedState(newUnavailableExpectedState(1))
	assert.Equal(t, "(1,false)", toStateString(statePair.GetExpectedState()))

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Term: 1,
	}).Maybe()
	statePair.SetCurrentState(newAvailableCurrentState(l))
	assert.Equal(t, "(1,true)", toStateString(statePair.GetCurrentState()))

	statePair.SetCurrentState(newUnavailableCurrentState(1, nil))
	assert.Equal(t, "(1,false)", toStateString(statePair.GetCurrentState()))

	assert.NoError(t, statePair.WaitExpectedStateChanged(context.Background(), newAvailableExpectedState(context.Background(), types.PChannelInfo{
		Term: 1,
	})))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.ErrorIs(t, statePair.WaitExpectedStateChanged(ctx, newUnavailableExpectedState(1)), context.DeadlineExceeded)

	assert.NoError(t, statePair.WaitCurrentStateReachExpected(context.Background(), newUnavailableExpectedState(1)))
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.ErrorIs(t, statePair.WaitCurrentStateReachExpected(ctx, newUnavailableExpectedState(2)), context.DeadlineExceeded)

	ch := make(chan struct{})
	go func() {
		defer close(ch)

		err := statePair.WaitCurrentStateReachExpected(context.Background(), newUnavailableExpectedState(3))
		assertErrorTermExpired(t, err)
	}()

	statePair.SetCurrentState(newUnavailableCurrentState(2, nil))
	time.Sleep(100 * time.Millisecond)
	statePair.SetCurrentState(newUnavailableCurrentState(4, nil))

	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Error("WaitCurrentStateReachExpected should not block")
	}
}

func assertErrorOperationIgnored(t *testing.T, err error) {
	assert.Error(t, err)
	logErr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION, logErr.Code)
}

func assertErrorTermExpired(t *testing.T, err error) {
	assert.Error(t, err)
	logErr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM, logErr.Code)
}

func assertErrorChannelNotExist(t *testing.T, err error) {
	assert.Error(t, err)
	logErr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST, logErr.Code)
}
