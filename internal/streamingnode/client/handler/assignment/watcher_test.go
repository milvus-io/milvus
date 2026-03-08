package assignment

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
