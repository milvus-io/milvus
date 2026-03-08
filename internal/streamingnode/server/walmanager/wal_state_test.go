package walmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestInitialWALState(t *testing.T) {
	currentState := initialCurrentWALState

	assert.Equal(t, types.InitialTerm, currentState.Term())
	assert.False(t, currentState.Available())
	assert.Nil(t, currentState.GetWAL())
	assert.NoError(t, currentState.GetLastError())

	assert.Equal(t, toStateString(currentState), "(-1,false)")

	expectedState := initialExpectedWALState
	assert.Equal(t, types.InitialTerm, expectedState.Term())
	assert.False(t, expectedState.Available())
	assert.Zero(t, expectedState.GetPChannelInfo())
	assert.Equal(t, context.Background(), expectedState.Context())
	assert.Equal(t, toStateString(expectedState), "(-1,false)")
}

func TestAvailableCurrentWALState(t *testing.T) {
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Term: 1,
	})

	state := newAvailableCurrentState(l)
	assert.Equal(t, int64(1), state.Term())
	assert.True(t, state.Available())
	assert.Equal(t, l, state.GetWAL())
	assert.Nil(t, state.GetLastError())

	assert.Equal(t, toStateString(state), "(1,true)")
}

func TestUnavailableCurrentWALState(t *testing.T) {
	err := errors.New("test")
	state := newUnavailableCurrentState(1, err)

	assert.Equal(t, int64(1), state.Term())
	assert.False(t, state.Available())
	assert.Nil(t, state.GetWAL())
	assert.ErrorIs(t, state.GetLastError(), err)

	assert.Equal(t, toStateString(state), "(1,false)")
}

func TestAvailableExpectedWALState(t *testing.T) {
	channel := types.PChannelInfo{}
	state := newAvailableExpectedState(context.Background(), channel)

	assert.Equal(t, int64(0), state.Term())
	assert.True(t, state.Available())
	assert.Equal(t, context.Background(), state.Context())
	assert.Equal(t, channel, state.GetPChannelInfo())

	assert.Equal(t, toStateString(state), "(0,true)")
}

func TestUnavailableExpectedWALState(t *testing.T) {
	state := newUnavailableExpectedState(1)

	assert.Equal(t, int64(1), state.Term())
	assert.False(t, state.Available())
	assert.Zero(t, state.GetPChannelInfo())
	assert.Equal(t, context.Background(), state.Context())

	assert.Equal(t, toStateString(state), "(1,false)")
}

func TestIsStateBefore(t *testing.T) {
	// initial state comparison.
	assert.False(t, isStateBefore(initialCurrentWALState, initialExpectedWALState))
	assert.False(t, isStateBefore(initialExpectedWALState, initialCurrentWALState))

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Term: 1,
	})

	cases := []walState{
		newAvailableCurrentState(l),
		newUnavailableCurrentState(1, nil),
		newAvailableExpectedState(context.Background(), types.PChannelInfo{
			Term: 3,
		}),
		newUnavailableExpectedState(5),
	}
	for _, s := range cases {
		assert.True(t, isStateBefore(initialCurrentWALState, s))
		assert.True(t, isStateBefore(initialExpectedWALState, s))
		assert.False(t, isStateBefore(s, initialCurrentWALState))
		assert.False(t, isStateBefore(s, initialExpectedWALState))
	}
	for i, s1 := range cases {
		for _, s2 := range cases[:i] {
			assert.True(t, isStateBefore(s2, s1))
			assert.False(t, isStateBefore(s1, s2))
		}
	}
}

func TestStateWithCond(t *testing.T) {
	stateCond := newWALStateWithCond(initialCurrentWALState)
	assert.Equal(t, initialCurrentWALState, stateCond.GetState())

	// test notification.
	wg := sync.WaitGroup{}
	targetState := newUnavailableCurrentState(10, nil)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			oldState := stateCond.GetState()
			for {
				if !isStateBefore(oldState, targetState) {
					break
				}

				err := stateCond.WatchChanged(context.Background(), oldState)
				assert.NoError(t, err)
				newState := stateCond.GetState()
				assert.True(t, isStateBefore(oldState, newState))
				oldState = newState
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()

			oldState := stateCond.GetState()
			for i := int64(0); i < 10; i++ {
				var newState currentWALState
				if i%2 == 0 {
					l := mock_wal.NewMockWAL(t)
					l.EXPECT().Channel().Return(types.PChannelInfo{
						Term: i % 2,
					}).Maybe()
					newState = newAvailableCurrentState(l)
				} else {
					newState = newUnavailableCurrentState(i%3, nil)
				}
				stateCond.SetStateAndNotify(newState)

				// updated state should never before old state.
				stateNow := stateCond.GetState()
				assert.False(t, isStateBefore(stateNow, oldState))
				oldState = stateNow
			}
			stateCond.SetStateAndNotify(targetState)
		}()
	}

	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-time.After(time.Second * 3):
		t.Errorf("test should never block")
	case <-ch:
	}

	// test cancel.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := stateCond.WatchChanged(ctx, targetState)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
