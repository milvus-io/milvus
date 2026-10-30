package qviews

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

// ReplicaSnapshot is a replacement routing hint. Valid distinguishes an empty candidate
// set from disconnected/uninitialized state. Replicas are owned by the receiver.
type ReplicaSnapshot struct {
	Replicas []Replica
	Valid    bool
}

// Replica describes a shard in the subscribed VChannel. Its maximum Up version
// is a routing-change hint, not the version pinned by a particular query.
// Value fields keep snapshots independent without retaining mutable protobufs.
type Replica struct {
	ReplicaID        int64
	MaxUpViewVersion QueryViewVersion
}

func ReplicasFromProto(entries []*viewpb.VChannelReplicas_Replica) []Replica {
	replicas := make([]Replica, len(entries))
	for i, entry := range entries {
		replicas[i] = Replica{ReplicaID: entry.GetReplicaId(), MaxUpViewVersion: FromProtoQueryViewVersion(entry.GetMaxUpViewVersion())}
	}
	return replicas
}
