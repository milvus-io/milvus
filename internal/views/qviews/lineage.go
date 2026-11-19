package qviews

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// SealedReadTimestamp is the engine read timestamp of sealed segments for a
// plan MVCC. A plan of a view with lineage reads at the common read point of
// its lineage group; every other plan reads at the transform frontier.
func SealedReadTimestamp(mvcc *viewpb.QueryPlanMVCC) uint64 {
	if read := mvcc.GetReadTimetick(); read != 0 {
		return read
	}
	return mvcc.GetTransformingTimetick()
}

// GrowingReadTimestamp is the engine read timestamp of growing segments for a
// plan MVCC. The residual of a split source read in a lineage group is read at
// the common read point, so that a delete a target applied to it at or below
// that point hides its row; every other plan reads at the growing frontier.
func GrowingReadTimestamp(mvcc *viewpb.QueryPlanMVCC) uint64 {
	if read := mvcc.GetReadTimetick(); read != 0 {
		return read
	}
	return mvcc.GetGrowingTimetick()
}

// PlanSnapshotTimestamp is the newest timestamp a plan MVCC reads at.
func PlanSnapshotTimestamp(mvcc *viewpb.QueryPlanMVCC) uint64 {
	return max(mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick(), mvcc.GetReadTimetick())
}

// LineageMVCCMismatch reports why a plan MVCC cannot be executed on a view
// with the given lineage, or an empty string when both agree. A view with
// lineage needs the read point and exactly one transform tick per lineage
// vchannel; a view without lineage accepts neither.
func LineageMVCCMismatch(lineage *viewpb.QueryViewLineage, mvcc *viewpb.QueryPlanMVCC) string {
	ticks := mvcc.GetLineageTransformingTimeticks()
	if len(lineage.GetVchannels()) == 0 {
		if mvcc.GetReadTimetick() != 0 || len(ticks) != 0 {
			return "plan carries a lineage read point but the view has no lineage"
		}
		return ""
	}
	if mvcc.GetReadTimetick() == 0 {
		return "view has lineage but the plan carries no read point"
	}
	if len(ticks) != len(lineage.GetVchannels()) {
		return "plan lineage vchannels do not match the view lineage"
	}
	for _, vchannel := range lineage.GetVchannels() {
		if _, ok := ticks[vchannel]; !ok {
			return "plan misses lineage vchannel " + vchannel
		}
	}
	return ""
}

// ValidLineage reports whether lineage is well formed for a view of vchannel:
// a nonzero switch tick and distinct, nonempty vchannels other than its own.
// Absent lineage is valid.
func ValidLineage(vchannel string, lineage *viewpb.QueryViewLineage) bool {
	if lineage == nil {
		return true
	}
	if len(lineage.GetVchannels()) == 0 || lineage.GetSwitchTimetick() == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(lineage.GetVchannels()))
	for _, channel := range lineage.GetVchannels() {
		if _, dup := seen[channel]; dup || channel == "" || channel == vchannel {
			return false
		}
		seen[channel] = struct{}{}
	}
	return true
}
