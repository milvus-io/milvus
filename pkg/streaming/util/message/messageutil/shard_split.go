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
