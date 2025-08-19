package replicatestream

import "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"

// ReplicateStreamClient is the client that replicates the message to the given cluster.
type ReplicateStreamClient interface {
	// Replicate replicates the message to the target cluster.
	// Replicate opeartion doesn't promise the message is delivered to the target cluster.
	// It will cache the message in memory and retry until the message is delivered to the target cluster or the client is closed.
	// Once the error is returned, the replicate operation will be unrecoverable.
	Replicate(msg message.ImmutableMessage) error

	// Stop stops the replicate operation.
	Close()
}
