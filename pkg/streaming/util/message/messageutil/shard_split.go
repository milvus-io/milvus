package messageutil

import (
	"slices"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// RetiresVChannel reports whether an AlterCollection replica retires the
// vchannel it was appended to: the commit rewrites the shard routing and the
// new vchannel list no longer names this vchannel. A shard split's source is
// retired this way at adoption; the streamingnode treats the commit as that
// vchannel's drop and collects its recovery meta once the vchannel's own L0 and
// L1 work has finished. There is no separate message for it.
func RetiresVChannel(header *message.AlterCollectionMessageHeader, updates *message.AlterCollectionMessageUpdates, vchannel string) bool {
	if funcutil.IsControlChannel(vchannel) {
		return false
	}
	if !slices.Contains(header.GetUpdateMask().GetPaths(), message.FieldMaskCollectionShardSplitRouting) {
		return false
	}
	if len(updates.GetVirtualChannelNames()) == 0 {
		return false
	}
	return !slices.Contains(updates.GetVirtualChannelNames(), vchannel)
}

// IsShardSplitRouting reports whether an AlterCollection carries a shard split
// routing commit, from the HEADER alone.
//
// It exists so a caller can decide whether to decode the body at all:
// RetiresVChannel needs the post-image's vchannel list, and decoding every
// AlterCollection body to find out that it is not a routing commit would put a
// decode (and a possible decrypt) on a path that never needed one.
func IsShardSplitRouting(header *message.AlterCollectionMessageHeader) bool {
	return slices.Contains(header.GetUpdateMask().GetPaths(), message.FieldMaskCollectionShardSplitRouting)
}
