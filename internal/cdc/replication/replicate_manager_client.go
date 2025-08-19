package replication

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// ReplicateManagerClient is the client that manages the replicate configuration.
type ReplicateManagerClient interface {
	// BroadcastReplicateConfiguration broadcasts the replicate configuration to the given clusters.
	BroadcastReplicateConfiguration(config *milvuspb.ReplicateConfiguration) error

	// StartReplications starts the replications for the given configuration.
	StartReplications(config *milvuspb.ReplicateConfiguration)

	// StopReplications stops the replications for the given configuration.
	StopReplications(config *milvuspb.ReplicateConfiguration)

	// Close stops all replications.
	Close()
}
