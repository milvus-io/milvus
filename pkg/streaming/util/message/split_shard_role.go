package message

import (
	"slices"

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
)

// SplitShardRoleOf decides the role of a replica from the header it carries
// and the vchannel it sits on.
//
// The broadcast reaches exactly the source, the targets and the control
// channel, so every replica has one of those roles. Any other vchannel --
// including another shard of the same collection -- was never a destination
// of the broadcast: it is a misroute (SplitShardRoleUnknown), refused on the
// append path and reported on the consume path.
func SplitShardRoleOf(header *messagespb.SplitShardMessageHeader, vchannel string) SplitShardRole {
	if funcutil.IsControlChannel(vchannel) {
		return SplitShardRoleControl
	}
	if vchannel != "" && header.GetSourceVchannel() == vchannel {
		return SplitShardRoleSource
	}
	if vchannel != "" && slices.Contains(header.GetTargetVchannels(), vchannel) {
		return SplitShardRoleTarget
	}
	return SplitShardRoleUnknown
}
