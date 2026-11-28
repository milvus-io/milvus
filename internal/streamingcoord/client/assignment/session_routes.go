package assignment

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// sessionAssignments projects one complete etcd revision. A channel with conflicting
// writer advertisements has no writable route until the sessions converge.
func sessionAssignments(state resolver.VersionedState) *types.VersionedStreamingNodeAssignments {
	result := &types.VersionedStreamingNodeAssignments{
		Version:     typeutil.VersionInt64Pair{Global: int64(state.Version.(typeutil.VersionInt64))},
		Assignments: make(map[int64]types.StreamingNodeAssignment),
	}
	writers := make(map[string]int64)
	var controls []string
	for id, s := range state.Sessions() {
		if s.Stopping {
			continue
		}
		node := types.StreamingNodeAssignment{
			NodeInfo: types.StreamingNodeInfo{ServerID: id, Address: s.Address},
			Channels: make(map[string]types.PChannelInfo),
		}
		result.Assignments[id] = node
		for _, ad := range s.WALReplicas {
			if ad.GetPchannel() == "" || ad.GetWalReplicaId() <= 0 || ad.GetWriterTerm() <= 0 {
				continue
			}
			ch := types.PChannelInfo{Name: ad.Pchannel, WALReplicaID: ad.WalReplicaId, AccessMode: types.AccessModeRW, Term: ad.GetWriterTerm()}
			if ad.IsCchannel {
				controls = append(controls, ch.Name)
			}
			if previous, exists := writers[ch.Name]; exists {
				if previous != 0 {
					delete(result.Assignments[previous].Channels, ch.Name)
				}
				writers[ch.Name] = 0
				continue
			}
			node.Channels[ch.Name] = ch
			writers[ch.Name] = id
		}
	}
	if len(controls) == 1 && writers[controls[0]] != 0 {
		result.CChannel = &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: controls[0]}}
	}
	return result
}
