package extension

// ShardLeaderReadiness answers one question about one resource group: can that
// resource group serve queries on that collection right now.
//
// A distribution that loads a collection into several resource groups
// independently cannot use the collection-wide shard-leader view for this. A
// leader serving resource group A says nothing about resource group B, so a
// query routed to B can be admitted while B has no leader at all. This type is
// the per-resource-group answer, and Ready is scoped to the replicas that live
// in the requested resource group and to nothing else.
//
// It deliberately does NOT carry the leaders themselves - no node ids, no node
// addresses. Routing a query to a leader is the proxy's job and it has
// GetShardLeaders for that; a control plane deciding whether to admit a query
// needs a verdict, not a routing table, and handing it one would invite
// routing decisions made from a stale, out-of-band copy. What it does carry is
// enough to say WHY the answer is no: which shards are missing a leader, out
// of how many, and a reason for the outcomes that are not about a specific
// shard at all.
// The reason strings below mirror internal/querycoordv2/utils, which computes
// them; the two sets must stay identically valued, or callers comparing
// against these constants go blind. The DTO is duplicated rather than shared
// because the dependency points outward: querycoord's computation layer does
// not import this package.
type ShardLeaderReadiness struct {
	// Ready is true only when every shard of the collection has a serviceable
	// leader inside a replica that lives in the requested resource group. It is
	// false in every other case, including the cases that are not failures -
	// the collection still loading, the resource group holding no replica yet.
	Ready bool

	// Reason explains a false Ready in one short phrase. It is one of the
	// ShardLeadersReason constants below, and is empty when Ready is true.
	// Callers may compare it against those constants, but the values exist
	// mainly so that an operator reading a log line can tell "this resource
	// group is not on the collection" apart from "this resource group is on it
	// and still catching up".
	Reason string

	// TotalShards is how many shards (dm channels) the collection's current
	// target has. It is 0 when the answer was decided before the target was
	// consulted.
	TotalShards int

	// UnreadyShards names the shards that have no serviceable leader in the
	// requested resource group, sorted, so the same state always prints the
	// same line. It is empty when Ready is true, and also when the answer was
	// decided before any shard was examined - Reason covers that case.
	UnreadyShards []string
}

// The reasons a resource group is not ready to serve. They are ordinary
// strings rather than a named type so that they cost a caller nothing to log
// or ignore.
const (
	// ShardLeadersReasonCoordinatorNotReady means the coordinator's query meta
	// is not initialized yet, so no answer can be given at all.
	ShardLeadersReasonCoordinatorNotReady = "coordinator query meta is not ready"

	// ShardLeadersReasonNoReplicaInResourceGroup means no replica of the
	// collection lives in this resource group. Nothing is loading here; the
	// caller has to load the collection into the resource group first. This is
	// the shard-leader counterpart of the -1 that GetReplicaLoadPercentByRG
	// reports, and is distinct from a replica that exists and carries nothing.
	ShardLeadersReasonNoReplicaInResourceGroup = "no replica of the collection lives in this resource group"

	// ShardLeadersReasonCollectionNotLoaded means a replica record exists but
	// the collection is not currently registered as loaded, for example because
	// the load failed. When a failure was recorded it is returned as the error
	// alongside this reason.
	ShardLeadersReasonCollectionNotLoaded = "the collection is not registered as loaded"

	// ShardLeadersReasonNoChannelTarget means the collection has no shard in
	// the current target, which is what a collection under recovery looks like.
	ShardLeadersReasonNoChannelTarget = "the collection has no shard in the current target, it may be recovering"

	// ShardLeadersReasonShardsWithoutLeader means the resource group holds a
	// replica of the collection, but at least one shard has no serviceable
	// leader in it yet. UnreadyShards names them.
	ShardLeadersReasonShardsWithoutLeader = "some shards have no serviceable leader in this resource group"
)
