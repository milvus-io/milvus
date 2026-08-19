package discover

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// discoverGrpcServerHelper is a wrapped discover server of log messages.
type discoverGrpcServerHelper struct {
	streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer
}

// SendFullAssignment sends the full assignment to client.
func (h *discoverGrpcServerHelper) SendFullAssignment(param balancer.WatchChannelAssignmentsCallbackParam) error {
	// current streaming node is not included in the assignments.
	nodes, err := resource.Resource().StreamingNodeManagerClient().GetAllStreamingNodes(h.Context())
	if err != nil {
		return err
	}
	assignmentsMap := make(map[int64]*streamingpb.StreamingNodeAssignment)
	for _, relation := range param.Relations {
		if _, ok := nodes[relation.Node.ServerID]; !ok {
			continue
		}
		if assignmentsMap[relation.Node.ServerID] == nil {
			assignmentsMap[relation.Node.ServerID] = newStreamingNodeAssignment(relation.Node)
		}
		assignmentsMap[relation.Node.ServerID].Channels = append(
			assignmentsMap[relation.Node.ServerID].Channels, types.NewProtoFromPChannelInfo(relation.Channel))
	}
	for _, relation := range param.WALReplicaRelations {
		node, ok := nodes[relation.Node.ServerID]
		if !ok {
			continue
		}
		if assignmentsMap[relation.Node.ServerID] == nil {
			assignmentsMap[relation.Node.ServerID] = newStreamingNodeAssignment(relation.Node)
		}
		replica := relation.Replica
		replica.ResourceGroup = node.ResourceGroup
		assignmentsMap[relation.Node.ServerID].WalReplicas = append(
			assignmentsMap[relation.Node.ServerID].WalReplicas,
			types.NewProtoFromWALReplicaInfo(replica),
		)
	}
	for _, node := range nodes {
		if assignmentsMap[node.ServerID] == nil {
			// if current streaming node is not assigned to any channel, add it to the assignments with empty assignments.
			assignmentsMap[node.ServerID] = newStreamingNodeAssignment(node.StreamingNodeInfo)
		}
	}
	assignments := make([]*streamingpb.StreamingNodeAssignment, 0, len(assignmentsMap))
	for _, node := range assignmentsMap {
		assignments = append(assignments, node)
	}
	return h.Send(&streamingpb.AssignmentDiscoverResponse{
		Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
			FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{
				StreamingVersion: param.StreamingVersion,
				Version: &streamingpb.VersionPair{
					// we are using the node id as the global version at previous implementation.
					// however, the server id of mixcoord didn't promise monotonic increasing,
					// so we are using the revision of session to promise it, Version is a deprecated field to keep compatibility,
					// TODO: may be removed in future.
					Global: paramtable.GetNodeID(),
					Local:  param.Version.Local,
				},
				VersionByRevision: &streamingpb.VersionPair{
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

func newStreamingNodeAssignment(node types.StreamingNodeInfo) *streamingpb.StreamingNodeAssignment {
	return &streamingpb.StreamingNodeAssignment{
		Node:        types.NewProtoFromStreamingNodeInfo(node),
		Channels:    make([]*streamingpb.PChannelInfo, 0),
		WalReplicas: make([]*streamingpb.WALReplicaInfo, 0),
	}
}

// SendCloseResponse sends the close response to client.
func (h *discoverGrpcServerHelper) SendCloseResponse() error {
	return h.Send(&streamingpb.AssignmentDiscoverResponse{
		Response: &streamingpb.AssignmentDiscoverResponse_Close{
			Close: &streamingpb.CloseAssignmentDiscoverResponse{},
		},
	})
}
