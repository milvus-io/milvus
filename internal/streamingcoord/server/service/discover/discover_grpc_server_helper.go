package discover

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// discoverGrpcServerHelper is a wrapped discover server of log messages.
type discoverGrpcServerHelper struct {
	streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer
}

// SendFullAssignment sends the full assignment to client.
func (h *discoverGrpcServerHelper) SendFullAssignment(param balancer.WatchChannelAssignmentsCallbackParam) error {
	assignmentsMap := make(map[int64]*streamingpb.StreamingNodeAssignment)
	for _, relation := range param.Relations {
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
					Global: param.Version.Global,
					Local:  param.Version.Local,
				},
				Assignments:            assignments,
				Cchannel:               param.CChannelAssignment,
				ReplicateConfiguration: param.ReplicateConfiguration,
			},
		},
	})
}

// SendCloseResponse sends the close response to client.
func (h *discoverGrpcServerHelper) SendCloseResponse() error {
	return h.Send(&streamingpb.AssignmentDiscoverResponse{
		Response: &streamingpb.AssignmentDiscoverResponse_Close{
			Close: &streamingpb.CloseAssignmentDiscoverResponse{},
		},
	})
}
