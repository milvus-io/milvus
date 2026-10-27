package replicadiscovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Snapshot is a replacement routing hint. Valid distinguishes an empty candidate
// set from disconnected/uninitialized state. Replicas are owned by the receiver.
type Snapshot struct {
	Replicas []Replica
	Valid    bool
}

// Replica describes a shard in the subscribed VChannel. Its maximum Up version
// is a routing-change hint, not the version pinned by a particular query.
// Value fields keep snapshots independent without retaining mutable protobufs.
type Replica struct {
	ReplicaID        int64
	MaxUpViewVersion qviews.QueryViewVersion
}

func ReplicasFromProto(entries []*viewpb.VChannelReplicas_Replica) []Replica {
	replicas := make([]Replica, len(entries))
	for i, entry := range entries {
		replicas[i] = Replica{ReplicaID: entry.GetReplicaId(), MaxUpViewVersion: qviews.FromProtoQueryViewVersion(entry.GetMaxUpViewVersion())}
	}
	return replicas
}

// Handler consumes an owned full snapshot. Physical streams invoke it from
// their delivery loop: it must not block or synchronously close the stream or
// subscription. The resuming client isolates application callbacks separately.
type Handler func(Snapshot)

// Stream is one owner-bound attempt. It never reconnects itself.
// Handlers must not block or synchronously close this stream/subscription.
// Local WAL and RPC adapters implement this same domain interface.
type Stream interface {
	Subscribe(context.Context, string, Handler) (Subscription, error)
	Done() <-chan struct{}
	Error() error
	Close() error
}

type Subscription interface {
	Snapshot() Snapshot
	Close() error
}

// Provider exposes direct local subscription, without wire-protocol emulation.
type Provider interface {
	AcquireReplicaWatchStream(context.Context) (Stream, error)
}
