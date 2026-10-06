package vchannel

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// pendingDrop is a runtime close, never a catalog state. before fences metadata
// publication while later schema/partition observations continue in meta.
type pendingDrop struct {
	partitionID int64 // zero closes the whole VChannel
	timeTick    uint64
	before      *streamingpb.VChannelMeta
}

func (info *VChannelView) beginDropLocked(partitionID int64, timeTick uint64) {
	info.pendingDrops = append(info.pendingDrops, pendingDrop{
		partitionID: partitionID, timeTick: timeTick,
		before: proto.Clone(info.meta).(*streamingpb.VChannelMeta),
	})
}

func (info *VChannelView) closingLocked(partitionID int64) bool {
	for _, drop := range info.pendingDrops {
		if drop.partitionID == partitionID {
			return true
		}
	}
	return false
}

func (info *VChannelView) IsClosing() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.closingLocked(0)
}

func (info *VChannelView) stableMetaLocked() *streamingpb.VChannelMeta {
	if len(info.pendingDrops) > 0 {
		return info.pendingDrops[0].before
	}
	return info.meta
}

// CompleteDrop installs the oldest close only after its owner has joined all
// local dependencies. Later publication fences must include the same tombstone.
func (info *VChannelView) CompleteDrop(timeTick uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	drop := info.pendingDrops[0]
	if drop.timeTick != timeTick {
		panic("out-of-order drop completion")
	}
	info.pendingDrops[0] = pendingDrop{}
	info.pendingDrops = info.pendingDrops[1:]
	install := func(meta *streamingpb.VChannelMeta) {
		if drop.partitionID == 0 {
			meta.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
		} else {
			for _, partition := range meta.GetCollectionInfo().GetPartitions() {
				if partition.GetPartitionId() == drop.partitionID {
					partition.State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
					partition.CheckpointTimeTick = timeTick
				}
			}
		}
		meta.CheckpointTimeTick = max(meta.GetCheckpointTimeTick(), timeTick)
	}
	install(info.meta)
	for _, pending := range info.pendingDrops {
		install(pending.before)
	}
	info.dirty = true
}
