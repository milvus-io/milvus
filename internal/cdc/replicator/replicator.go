package replicator

type Replicator interface {
	// StartReplication starts the replication for the target cluster.
	StartReplication() error

	// StopReplication stops the replication for the target cluster.
	StopReplication() error
}
