// Package topology computes the desired WAL replica placement for a PChannel.
package topology

import (
	"cmp"
	"context"
	"slices"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// Next computes one step towards a writer placement. The existing balance policy
// selects writerNode. Other resource groups retain their current placement or
// choose the least loaded eligible node. Retiring members stay in the topology
// until their replacements have acknowledged the requested role.
func Next(ctx context.Context, current *streamingpb.WALReplicaTopology, writerNode int64,
	eligible map[int64]*types.StreamingNodeInfoWithResourceGroup, live map[int64]*types.StreamingNodeInfoWithResourceGroup,
	load map[int64]int, ready map[int64]bool, allocate func(context.Context) (uint64, error),
) (*streamingpb.WALReplicaTopology, error) {
	previous := make(map[int64]*streamingpb.WALReplicaPlacement, len(current.Members))
	for _, member := range current.Members {
		previous[member.NodeId] = member
	}
	selected := make(map[string]int64)
	for id, node := range eligible {
		old, ok := selected[node.ResourceGroup]
		if !ok || preferNode(id, old, previous, load) {
			selected[node.ResourceGroup] = id
		}
	}
	if writerNode != 0 {
		selected[eligible[writerNode].ResourceGroup] = writerNode
	} else {
		// An explicit close retains the last allocated writer term.
		clear(selected)
	}
	ids := make([]int64, 0, len(selected))
	for _, id := range selected {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	next := &streamingpb.WALReplicaTopology{Pchannel: current.Pchannel, WriterTerm: current.WriterTerm}
	readyToRetire := true
	for _, id := range ids {
		member := previous[id]
		if member == nil {
			allocated, err := allocate(ctx)
			if err != nil {
				return nil, err
			}
			member = &streamingpb.WALReplicaPlacement{NodeId: id, WalReplicaId: int64(allocated)}
		}
		if id == writerNode {
			next.WriterReplicaId = member.WalReplicaId
		}
		readyToRetire = readyToRetire && ready[member.WalReplicaId]
		next.Members = append(next.Members, member)
	}
	writerChanged := next.WriterReplicaId != current.WriterReplicaId
	if writerChanged && writerNode != 0 {
		next.WriterTerm++
	}
	if writerNode != 0 && (!readyToRetire || writerChanged) {
		for _, member := range current.Members {
			if _, exists := live[member.NodeId]; !exists {
				continue
			}
			if _, selected := slices.BinarySearch(ids, member.NodeId); !selected {
				next.Members = append(next.Members, member)
			}
		}
	}
	slices.SortFunc(next.Members, func(a, b *streamingpb.WALReplicaPlacement) int { return cmp.Compare(a.NodeId, b.NodeId) })
	if !writerChanged && slices.EqualFunc(next.Members, current.Members, func(a, b *streamingpb.WALReplicaPlacement) bool {
		return a.WalReplicaId == b.WalReplicaId && a.NodeId == b.NodeId
	}) {
		return current, nil
	}
	revision, err := allocate(ctx)
	if err != nil {
		return nil, err
	}
	next.Revision = int64(revision)
	return next, nil
}

func preferNode(candidate, incumbent int64, previous map[int64]*streamingpb.WALReplicaPlacement, load map[int64]int) bool {
	hasCandidate, hasIncumbent := previous[candidate] != nil, previous[incumbent] != nil
	if hasCandidate != hasIncumbent {
		return hasCandidate
	}
	if load[candidate] != load[incumbent] {
		return load[candidate] < load[incumbent]
	}
	return candidate < incumbent
}
