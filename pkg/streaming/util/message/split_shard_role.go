package message

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// SplitShardRole is what one replica of a SplitShard broadcast means to the
// vchannel it was appended to.
type SplitShardRole int

const (
	// SplitShardRoleUnknown: the vchannel is neither a source, a target nor the
	// control channel. A replica landing here is misrouted and must be refused.
	SplitShardRoleUnknown SplitShardRole = iota
	// SplitShardRoleSource: the fence. The replica's time tick is T_switch.
	SplitShardRoleSource
	// SplitShardRoleTarget: the genesis of a target vchannel.
	SplitShardRoleTarget
	// SplitShardRoleControl: the control channel replica, orders the ack callback.
	SplitShardRoleControl
	// SplitShardRoleBystander: a vchannel of the SAME collection that this
	// split neither fences nor creates. The broadcast now covers every
	// vchannel of the collection (not just the ones the split acts on), so
	// this replica must land and pass through every consumer without effect,
	// exactly as a bystander watches an event that does not concern it.
	SplitShardRoleBystander
)

// SplitShardRoleOf decides the role of a replica from the header it carries
// and the vchannel it sits on.
func SplitShardRoleOf(header *messagespb.SplitShardMessageHeader, vchannel string) SplitShardRole {
	if funcutil.IsControlChannel(vchannel) {
		return SplitShardRoleControl
	}
	for _, source := range header.GetSourceVchannels() {
		if source == vchannel {
			return SplitShardRoleSource
		}
	}
	if SplitShardTargetOf(header, vchannel) != nil {
		return SplitShardRoleTarget
	}
	// Neither a source, a target nor the control channel: a replica on
	// another vchannel of the SAME collection is a bystander, exactly what
	// the broadcast-to-the-whole-collection redesign puts there on purpose. A
	// vchannel of a different collection is a genuine misroute.
	if funcutil.GetCollectionIDFromVChannel(vchannel) == header.GetCollectionId() {
		return SplitShardRoleBystander
	}
	return SplitShardRoleUnknown
}

// SplitShardTargetOf returns the target entry of the given vchannel, nil when
// the vchannel is not a target of this split.
func SplitShardTargetOf(header *messagespb.SplitShardMessageHeader, vchannel string) *messagespb.SplitShardTarget {
	for _, target := range header.GetTargets() {
		if target.GetVchannel() == vchannel {
			return target
		}
	}
	return nil
}
