package resolver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	mock_discoverer "github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

func TestResolverWithDiscoverer(t *testing.T) {
	d := mock_discoverer.NewMockDiscoverer(t)
	ch := make(chan discoverer.VersionedState)
	d.EXPECT().Discover(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(discoverer.VersionedState) error) error {
		for {
			state, ok := <-ch
			if !ok {
				return nil
			}
			if err := cb(state); err != nil {
				return err
			}
		}
	})
	d.EXPECT().NewVersionedState().Return(discoverer.VersionedState{
		Version: util.NewVersionInt64(),
	})

	r := newResolverWithDiscoverer("test", d, time.Second)

	state := r.GetLatestState()
	assert.Equal(t, util.NewVersionInt64(), state.Version)
	time.Sleep(500 * time.Millisecond)

	state = r.GetLatestState()
	assert.Equal(t, util.NewVersionInt64(), state.Version)

	// should be non block after context canceled
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	err := r.Watch(ctx, func(s VersionedState) error {
		state = s
		return nil
	})
	assert.Equal(t, util.NewVersionInt64(), state.Version)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, errors.Is(err, ErrCanceled))

	// should be non block after state operation failure.
	testErr := errors.New("test error")
	err = r.Watch(context.Background(), func(s VersionedState) error {
		return testErr
	})
	assert.ErrorIs(t, err, testErr)
	assert.True(t, errors.Is(err, ErrInterrupted))

	outCh := make(chan VersionedState, 1)
	go func() {
		var state VersionedState
		err := r.Watch(context.Background(), func(s VersionedState) error {
			state = s
			if state.Version.GT(util.VersionInt64(2)) {
				return testErr
			}
			return nil
		})
		assert.ErrorIs(t, err, testErr)
		outCh <- state
	}()

	// should be block.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	select {
	case _ = <-outCh:
		t.Errorf("should be block")
	case <-ctx.Done():
	}

	ch <- discoverer.VersionedState{
		Version: util.VersionInt64(1),
		State: resolver.State{
			Addresses: []resolver.Address{},
		},
	}

	// version do not reach, should be block.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	select {
	case _ = <-outCh:
		t.Errorf("should be block")
	case <-ctx.Done():
	}

	ch <- discoverer.VersionedState{
		Version: util.VersionInt64(3),
		State: resolver.State{
			Addresses:  []resolver.Address{},
			Attributes: attributes.New("1", "1"),
		},
	}

	// version do reach, should not be block.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	select {
	case state = <-outCh:
		assert.Equal(t, util.VersionInt64(3), state.Version)
		assert.NotNil(t, state.State.Attributes)
		assert.NotNil(t, state.State.Addresses)
	case <-ctx.Done():
		t.Errorf("should not be block")
	}
}
