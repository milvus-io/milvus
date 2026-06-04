package recovery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func minCheckpointByTimeTick(left, right *WALCheckpoint) *WALCheckpoint {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if right.TimeTick < left.TimeTick {
		return right
	}
	return left
}

// clampCheckpointPositionByTimeTick rolls base's consume position (MessageID +
// TimeTick) back to bound when bound is older, while preserving base's
// control-plane metadata (Magic, ReplicateConfig, ReplicateCheckpoint,
// AlterWalState).
//
// The persisted consume checkpoint must not advance past a durability bound
// (idempotency window source or flusher) so that on restart the replayed range
// can rebuild that state. Clamping the position alone is enough for that; the
// replication / alter-WAL fields belong to the consume checkpoint and would be
// silently lost if we swapped in the bound checkpoint wholesale (bound carries
// only a position, not metadata).
func clampCheckpointPositionByTimeTick(base, bound *WALCheckpoint) *WALCheckpoint {
	if base == nil {
		return cloneWALCheckpoint(bound)
	}
	if bound == nil || base.TimeTick <= bound.TimeTick {
		return base.Clone()
	}
	clamped := base.Clone()
	clamped.MessageID = bound.MessageID
	clamped.TimeTick = bound.TimeTick
	return clamped
}

func minCheckpointByMessageID(left, right *WALCheckpoint) *WALCheckpoint {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if right.MessageID != nil && left.MessageID != nil && right.MessageID.LTE(left.MessageID) {
		return right
	}
	if right.TimeTick < left.TimeTick {
		return right
	}
	return left
}

func cloneWALCheckpoint(checkpoint *WALCheckpoint) *WALCheckpoint {
	if checkpoint == nil {
		return nil
	}
	return checkpoint.Clone()
}

func checkpointCovers(covered *WALCheckpoint, target *WALCheckpoint) bool {
	if covered == nil || target == nil {
		return false
	}
	if covered.MessageID != nil && target.MessageID != nil {
		return target.MessageID.LTE(covered.MessageID)
	}
	return target.TimeTick <= covered.TimeTick
}

func sameWALCheckpoint(left, right *WALCheckpoint) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.TimeTick != right.TimeTick {
		return false
	}
	if left.MessageID == nil || right.MessageID == nil {
		return left.MessageID == nil && right.MessageID == nil
	}
	return left.MessageID.EQ(right.MessageID)
}

func checkpointMessageIDString(checkpoint *WALCheckpoint) string {
	if checkpoint == nil || checkpoint.MessageID == nil {
		return ""
	}
	return checkpoint.MessageID.String()
}

func checkpointTimeTick(checkpoint *WALCheckpoint) uint64 {
	if checkpoint == nil {
		return 0
	}
	return checkpoint.TimeTick
}

func safeMessageIDProto(id message.MessageID) *commonpb.MessageID {
	if id == nil {
		return nil
	}
	return id.IntoProto()
}

func cloneMessageIDProto(id *commonpb.MessageID) *commonpb.MessageID {
	if id == nil {
		return nil
	}
	return proto.Clone(id).(*commonpb.MessageID)
}
