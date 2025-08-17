package replicatemanager

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	// replicatestream "github.com/milvus-io/milvus/internal/cdc/replicatestream"
)

// ReplicateManagerClient is the client that manages the replicate configuration.
type ReplicateManagerClient interface {
	// BroadcastReplicateConfiguration broadcasts the replicate configuration to the given clusters.
	BroadcastReplicateConfiguration(config *milvuspb.ReplicateConfiguration) error

	// // CreateReplicateStream creates a new replicate stream for the given cluster.
	// CreateReplicateStream(targetCluster string) (replicatestream.ReplicateStreamClient, error)

	// StartReplications starts the replications for the given clusters.
	StartReplications(config *milvuspb.ReplicateConfiguration) error

	// StopReplications stops the replications for the given clusters.
	StopReplications(config *milvuspb.ReplicateConfiguration) error

	// Close closes the replicate manager client.
	Close()
}
