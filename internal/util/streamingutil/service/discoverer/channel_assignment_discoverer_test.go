package discoverer

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestChannelAssignmentDiscoverer(t *testing.T) {
	w := mock_types.NewMockAssignmentDiscoverWatcher(t)
	ch := make(chan *types.VersionedStreamingNodeAssignments, 10)
	w.EXPECT().AssignmentDiscover(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case result, ok := <-ch:
					if ok {
						if err := cb(result); err != nil {
							return err
						}
					} else {
						return io.EOF
					}
				}
			}
		})

	d := NewChannelAssignmentDiscoverer(w)
	s := d.NewVersionedState()
	assert.True(t, s.Version.EQ(typeutil.VersionInt64Pair{Global: -1, Local: -1}))

	expected := []*types.VersionedStreamingNodeAssignments{
		{
			Version:     typeutil.VersionInt64Pair{Global: -1, Local: -1},
			Assignments: map[int64]types.StreamingNodeAssignment{},
		},
		{
			Version: typeutil.VersionInt64Pair{
				Global: 1,
				Local:  2,
			},
			Assignments: map[int64]types.StreamingNodeAssignment{
				1: {
					NodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
					Channels: map[string]types.PChannelInfo{
						"ch1": {Name: "ch1", Term: 1},
					},
				},
			},
		},
		{
			Version: typeutil.VersionInt64Pair{
				Global: 3,
				Local:  4,
			},
			Assignments: map[int64]types.StreamingNodeAssignment{},
		},
		{
			Version: typeutil.VersionInt64Pair{
				Global: 5,
				Local:  6,
			},
			Assignments: map[int64]types.StreamingNodeAssignment{
				1: {
					NodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
					Channels: map[string]types.PChannelInfo{
						"ch2": {Name: "ch2", Term: 1},
					},
				},
			},
		},
	}

	idx := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := d.Discover(ctx, func(state VersionedState) error {
		assert.True(t, expected[idx].Version.EQ(state.Version))
		assignment := state.ChannelAssignmentInfo()
		assert.Equal(t, expected[idx].Assignments, assignment)
		if idx < len(expected)-1 {
			ch <- expected[idx+1]
			idx++
			return nil
		}

		// resolver attributes
		for _, addr := range state.State.Addresses {
			serverID := attributes.GetServerID(addr.Attributes)
			assert.NotNil(t, serverID)
		}
		return io.EOF
	})
	assert.ErrorIs(t, err, io.EOF)
}
