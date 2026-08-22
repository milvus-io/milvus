package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReplicaPicker selects the replica used for one shard attempt.
type ReplicaPicker interface {
	Pick(ctx context.Context, replicas *resolver.ShardReplicas) (qviews.ShardID, error)
}

// NewPrimaryReplicaPicker returns the only picker supported by the initial
// milestone. Primary-only is intentional: secondary replica selection and its
// freshness policy are deferred until a later QueryView client phase.
func NewPrimaryReplicaPicker() ReplicaPicker {
	return primaryReplicaPicker{}
}

type primaryReplicaPicker struct{}

func (primaryReplicaPicker) Pick(_ context.Context, replicas *resolver.ShardReplicas) (qviews.ShardID, error) {
	if replicas == nil {
		return qviews.ShardID{}, merr.WrapErrServiceNotReadyMsg("query view shard discovery returned nil replicas")
	}
	primary := replicas.PrimaryShardID
	if replicas.VChannel == "" || primary.VChannel == "" || primary.ReplicaID == 0 {
		return qviews.ShardID{}, merr.WrapErrServiceNotReadyMsg(
			"query view primary is unavailable for vchannel %q", replicas.VChannel)
	}
	if primary.VChannel != replicas.VChannel {
		return qviews.ShardID{}, merr.WrapErrServiceInternalMsg(
			"query view primary vchannel mismatch: snapshot=%q, primary=%q",
			replicas.VChannel, primary.VChannel)
	}
	return primary, nil
}
