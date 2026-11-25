package assignment

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestWatcher(t *testing.T) {
	r := mock_resolver.NewMockResolver(t)

	ch := make(chan discoverer.VersionedState)
	r.EXPECT().Watch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(s discoverer.VersionedState) error) error {
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return nil
				}
				f(v)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	w := NewWatcher(r)
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	a := w.Get(ctx, "test_pchannel")
	assert.Nil(t, a)
	err := w.Watch(ctx, "test_pchannel", nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(1),
		State: resolver.State{
			Addresses: []resolver.Address{
				{
					Addr: "test_addr",
					BalancerAttributes: attributes.WithChannelAssignmentInfo(
						new(attributes.Attributes),
						&types.StreamingNodeAssignment{
							NodeInfo: types.StreamingNodeInfo{
								ServerID: 1,
								Address:  "test_addr",
							},
							Channels: map[string]types.PChannelInfo{
								"test_pchannel": {
									Name: "test_pchannel",
									Term: 1,
								},
								"test_pchannel_2": {
									Name: "test_pchannel_2",
									Term: 2,
								},
							},
						},
					),
				},
			},
		},
	}
	err = w.Watch(context.Background(), "test_pchannel", nil)
	assert.NoError(t, err)
	a = w.Get(ctx, "test_pchannel")
	assert.NotNil(t, a)
	assert.Equal(t, int64(1), a.Channel.Term)

	err = w.Watch(context.Background(), "test_pchannel_2", nil)
	assert.NoError(t, err)
	a = w.Get(ctx, "test_pchannel_2")
	assert.NotNil(t, a)
	assert.Equal(t, int64(2), a.Channel.Term)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = w.Watch(ctx, "test_pchannel", a)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReadableWALRoutes(t *testing.T) {
	r := &watcherImpl{cond: *syncutil.NewContextCond(&sync.Mutex{}), changed: make(chan struct{})}
	state := discoverer.VersionedState{}
	for id := int64(1); id <= 3; id++ {
		a := &types.StreamingNodeAssignment{NodeInfo: types.StreamingNodeInfo{ServerID: id}}
		ch := types.PChannelInfo{Name: "p", WALReplicaID: id}
		if id == 1 {
			ch.Term = 7
			a.Channels = map[string]types.PChannelInfo{"p": ch}
		} else {
			ch.AccessMode = types.AccessModeRO
			a.ReadOnlyChannels = map[string]types.PChannelInfo{"p": ch}
		}
		state.State.Addresses = append(state.State.Addresses, resolver.Address{BalancerAttributes: attributes.WithChannelAssignmentInfo(new(attributes.Attributes), a)})
	}
	r.updateAssignment(state)
	first, changed := r.Readable("p")
	second, _ := r.Readable("p")
	require.Len(t, first, 3)
	require.Equal(t, first[1], second[0])
	require.Equal(t, first[0], second[1])
	require.EqualValues(t, 1, first[2].Node.ServerID)
	r.updateAssignment(discoverer.VersionedState{})
	<-changed
	empty, _ := r.Readable("p")
	require.Empty(t, empty)
}
