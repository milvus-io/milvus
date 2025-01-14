package discover

import (
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// discoverGrpcServerHelper is a wrapped discover server of log messages.
type discoverGrpcServerHelper struct {
	streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer
}

// SendFullAssignment sends the full assignment to client.
func (h *discoverGrpcServerHelper) SendFullAssignment(v typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error {
	assignmentsMap := make(map[int64]*streamingpb.StreamingNodeAssignment)
	for _, relation := range relations {
		if assignmentsMap[relation.Node.ServerID] == nil {
			assignmentsMap[relation.Node.ServerID] = &streamingpb.StreamingNodeAssignment{
				Node:     types.NewProtoFromStreamingNodeInfo(relation.Node),
				Channels: make([]*streamingpb.PChannelInfo, 0),
			}
		}
		assignmentsMap[relation.Node.ServerID].Channels = append(
			assignmentsMap[relation.Node.ServerID].Channels, types.NewProtoFromPChannelInfo(relation.Channel))
	}

	assignments := make([]*streamingpb.StreamingNodeAssignment, 0, len(assignmentsMap))
	for _, node := range assignmentsMap {
		assignments = append(assignments, node)
	}
	return h.Send(&streamingpb.AssignmentDiscoverResponse{
		Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
			FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{
				Version: &streamingpb.VersionPair{
					Global: v.Global,
					Local:  v.Local,
				},
				Assignments: assignments,
			},
		},
	})
}

// SendCloseResponse sends the close response to client.
func (h *discoverGrpcServerHelper) SendCloseResponse() error {
	return h.Send(&streamingpb.AssignmentDiscoverResponse{
		Response: &streamingpb.AssignmentDiscoverResponse_Close{},
	})
}
