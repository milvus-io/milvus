package queryclient

import (
	"context"
	"math/rand"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// ReplicaPicker selects a target replica for shard-level requests.
// Modeled after grpc/balancer.Picker: Pick returns a result containing
// the selected replica and a Done callback for adaptive strategies
// to track per-replica metrics (latency, errors, etc.).
type ReplicaPicker interface {
	// Pick selects a replica from the candidates.
	// The returned ReplicaPickResult.Done must be called after the request completes.
	Pick(ctx context.Context, info ReplicaPickInfo) (ReplicaPickResult, error)
}

// ReplicaPickInfo contains the input for replica selection.
type ReplicaPickInfo struct {
	ShardReplicas *resolver.ShardReplicas
}

// ReplicaPickResult contains the selected replica and a completion callback.
type ReplicaPickResult struct {
	// ShardID is the selected target replica.
	ShardID qviews.ShardID
	// Done is called when the request to the selected replica completes.
	// Adaptive selectors (e.g., least-latency) use this to track per-replica metrics.
	// May be nil for non-adaptive selectors; callers must nil-check before calling.
	Done func(ReplicaDoneInfo)
}

// ReplicaDoneInfo contains the result of a request to the selected replica.
type ReplicaDoneInfo struct {
	Err error
}

// NewRandomReplicaPicker returns a picker that selects replicas uniformly at random.
func NewRandomReplicaPicker() ReplicaPicker {
	return randomReplicaPicker{}
}

type randomReplicaPicker struct{}

func (randomReplicaPicker) Pick(_ context.Context, info ReplicaPickInfo) (ReplicaPickResult, error) {
	replicas := info.ShardReplicas.ShardIDs
	return ReplicaPickResult{
		ShardID: replicas[rand.Intn(len(replicas))],
	}, nil
}
