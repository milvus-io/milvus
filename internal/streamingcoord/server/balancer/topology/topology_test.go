package topology

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestTopologyConvergence(t *testing.T) {
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {ResourceGroup: "primary"}, 2: {ResourceGroup: "other"}, 3: {ResourceGroup: "other"}}
	current := &streamingpb.WALReplicaTopology{Pchannel: "p"}
	var seq uint64
	allocate := func(context.Context) (uint64, error) { seq++; return seq, nil }
	ready := map[int64]bool{}
	step := func(writer int64, eligible map[int64]*types.StreamingNodeInfoWithResourceGroup, load map[int64]int) {
		t.Helper()
		next, err := Next(t.Context(), current, writer, eligible, nodes, load, ready, allocate)
		require.NoError(t, err)
		current = next
	}
	step(1, nodes, nil)
	require.EqualValues(t, 1, current.WriterTerm)
	require.EqualValues(t, 1, current.WriterReplicaId)
	require.Len(t, current.Members, 2)
	require.EqualValues(t, 2, current.Members[1].NodeId)
	before := current
	step(1, nodes, map[int64]int{2: 20})
	require.Same(t, before, current, "existing readers survive load changes")
	ready[1], ready[2] = true, true
	step(2, nodes, nil)
	require.EqualValues(t, 2, current.WriterReplicaId)
	require.EqualValues(t, 2, current.WriterTerm)
	require.Equal(t, before.Members, current.Members)
	step(3, map[int64]*types.StreamingNodeInfoWithResourceGroup{1: nodes[1], 3: nodes[3]}, nil)
	require.Len(t, current.Members, 3)
	require.EqualValues(t, 3, current.WriterTerm)
	require.NotEqual(t, int64(2), current.WriterReplicaId)
	newWriter := current.WriterReplicaId
	ready[newWriter] = true
	step(3, map[int64]*types.StreamingNodeInfoWithResourceGroup{1: nodes[1], 3: nodes[3]}, nil)
	require.Len(t, current.Members, 2)
	require.EqualValues(t, 3, current.Members[1].NodeId)
	require.EqualValues(t, 3, current.WriterTerm)
	step(0, nil, nil)
	require.Empty(t, current.Members)
	require.Zero(t, current.WriterReplicaId)
	require.EqualValues(t, 3, current.WriterTerm)
	step(1, nodes, map[int64]int{2: 4, 3: 1})
	require.EqualValues(t, 4, current.WriterTerm)
	require.EqualValues(t, 3, current.Members[1].NodeId)
	require.NotEqual(t, newWriter, current.Members[1].WalReplicaId)
}

func TestTopologyLostSessionAndAllocationFailure(t *testing.T) {
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {ResourceGroup: "default"}}
	current := &streamingpb.WALReplicaTopology{Pchannel: "p", WriterReplicaId: 2, WriterTerm: 4, Members: []*streamingpb.WALReplicaPlacement{{NodeId: 2, WalReplicaId: 2}}}
	for failAt := 1; failAt <= 3; failAt++ {
		var calls int
		next, err := Next(t.Context(), current, 1, nodes, nodes, nil, nil, func(context.Context) (uint64, error) {
			calls++
			if calls == failAt {
				return 0, io.ErrUnexpectedEOF
			}
			return uint64(40 + calls), nil
		})
		if failAt < 3 {
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.Nil(t, next)
			continue
		}
		require.NoError(t, err)
		require.Len(t, next.Members, 1)
		require.EqualValues(t, 5, next.WriterTerm)
	}
}
