package resolver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/google.golang.org/grpc/mock_resolver"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestResolverWithDiscoverer(t *testing.T) {
	d := mock_discoverer.NewMockDiscoverer(t)
	ch := make(chan discoverer.VersionedState)
	d.EXPECT().Discover(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(discoverer.VersionedState) error) error {
		for {
			select {
			case state := <-ch:
				if err := cb(state); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	d.EXPECT().NewVersionedState().Return(discoverer.VersionedState{
		Version: typeutil.VersionInt64(-1),
	})

	r := newResolverWithDiscoverer("test", d, time.Second)

	var resultOfGRPCResolver resolver.State
	mockClientConn := mock_resolver.NewMockClientConn(t)
	mockClientConn.EXPECT().UpdateState(mock.Anything).RunAndReturn(func(args resolver.State) error {
		resultOfGRPCResolver = args
		return nil
	})
	w := newWatchBasedGRPCResolver(mockClientConn, log.With())
	w2 := newWatchBasedGRPCResolver(nil, log.With())
	w2.Close()

	// Test Register a grpc resolver watcher.
	err := r.RegisterNewWatcher(w)
	assert.NoError(t, err)
	err = r.RegisterNewWatcher(w2) // A closed resolver should be removed automatically by resolver.
	assert.NoError(t, err)

	state := r.GetLatestState()
	assert.Equal(t, typeutil.VersionInt64(-1), state.Version)
	time.Sleep(500 * time.Millisecond)

	state = r.GetLatestState()
	assert.Equal(t, typeutil.VersionInt64(-1), state.Version)

	// should be non block after context canceled
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	err = r.Watch(ctx, func(s VersionedState) error {
		state = s
		return nil
	})
	assert.Equal(t, typeutil.VersionInt64(-1), state.Version)
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
			if state.Version.GT(typeutil.VersionInt64(2)) {
				return testErr
			}
			return nil
		})
		assert.ErrorIs(t, err, testErr)
		outCh <- state
	}()

	// should be block.
	shouldbeBlock(t, outCh)

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(1),
		State: resolver.State{
			Addresses: []resolver.Address{},
		},
	}

	// version do not reach, should be block.
	shouldbeBlock(t, outCh)

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(3),
		State: resolver.State{
			Addresses:  []resolver.Address{{Addr: "1"}},
			Attributes: attributes.New("1", "1"),
		},
	}

	// version do reach, should not be block.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	select {
	case state = <-outCh:
		assert.Equal(t, typeutil.VersionInt64(3), state.Version)
		assert.NotNil(t, state.State.Attributes)
		assert.NotNil(t, state.State.Addresses)
	case <-ctx.Done():
		t.Errorf("should not be block")
	}
	// after block, should be see the last state by grpc watcher.
	assert.Len(t, resultOfGRPCResolver.Addresses, 1)

	// old version should be filtered.
	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(2),
		State: resolver.State{
			Addresses:  []resolver.Address{{Addr: "1"}},
			Attributes: attributes.New("1", "1"),
		},
	}
	shouldbeBlock(t, outCh)
	w.Close() // closed watcher should be removed in next update.

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(5),
		State: resolver.State{
			Addresses:  []resolver.Address{{Addr: "1"}},
			Attributes: attributes.New("1", "1"),
		},
	}
	r.Close()

	// after close, new register is not allowed.
	err = r.RegisterNewWatcher(nil)
	assert.True(t, errors.Is(err, ErrCanceled))
}

func shouldbeBlock(t *testing.T, ch <-chan VersionedState) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case <-ch:
		t.Errorf("should be block")
	case <-ctx.Done():
	}
}
