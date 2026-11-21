package dataview

import (
	"context"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadAdmissionSampler returns both executable coordinates from one complete
// prefix on the destination VChannel's writable WAL owner.
type ReadAdmissionSampler func(context.Context, string) (*viewpb.QueryPlanMVCC, error)

func WithReadAdmissionSampler(sample ReadAdmissionSampler) Option {
	return func(m *dataViewManager) { m.readAdmissionSample = sample }
}

func hasReadAdmission(view *viewpb.DataViewOfCollection) bool {
	for _, shard := range view.GetShards() {
		if shard.GetReadAdmissionFloor() == nil {
			return false
		}
	}
	return true
}

// A Flush can hand a row from the Growing G axis to the Sealed T axis;
// a replacement can irreversibly materialize mutations even with no output.
// Keep a certificate only when the selected physical descriptors are unchanged.
func invalidateChangedReadAdmission(previous, candidate *viewpb.DataViewOfCollection) {
	before := make(map[string]*viewpb.DataViewOfShard, len(previous.GetShards()))
	for _, shard := range previous.GetShards() {
		before[shard.GetVchannel()] = shard
	}
	for _, shard := range candidate.GetShards() {
		old := before[shard.GetVchannel()]
		if old == nil || len(old.GetPartitions()) != len(shard.GetPartitions()) ||
			old.GetGrowingTransformStartAfterTimetick() != shard.GetGrowingTransformStartAfterTimetick() ||
			old.GetCompactedTransformStartAfterTimetick() != shard.GetCompactedTransformStartAfterTimetick() {
			shard.ReadAdmissionFloor = nil
			continue
		}
		for i, partition := range old.GetPartitions() {
			next := shard.GetPartitions()[i]
			// Partition statistics may be rebuilt without changing its rows.
			if partition.GetPartitionId() != next.GetPartitionId() ||
				!slices.Equal(partition.GetSegmentIds(), next.GetSegmentIds()) ||
				!slices.Equal(partition.GetSegmentManifestVersions(), next.GetSegmentManifestVersions()) ||
				!slices.Equal(partition.GetSegmentTransformStartAfterTimeticks(), next.GetSegmentTransformStartAfterTimeticks()) {
				shard.ReadAdmissionFloor = nil
				break
			}
		}
	}
}

// certifyReadAdmission is run only by the background reconciliation worker.
// Initial metadata recovery and atomic physical publication never wait for the
// WAL owner, which itself can call DataCoord when recovering legacy metadata.
func (m *dataViewManager) certifyReadAdmission(ctx context.Context, collectionID int64) error {
	if m.readAdmissionSample == nil {
		return nil
	}
	state := m.getState(collectionID)
	if state == nil {
		return nil
	}
	state.mu.Lock()
	frozen := state.latest
	if frozen == nil || hasReadAdmission(frozen.view) {
		state.mu.Unlock()
		return nil
	}
	physical := frozen.view
	candidate := canonicalDataViewClone(physical)
	ref := acquireRefLocked(state, frozen)
	state.mu.Unlock()
	defer ref.Deref()
	for _, shard := range candidate.GetShards() {
		if shard.GetReadAdmissionFloor() != nil {
			continue
		}
		pair, err := m.readAdmissionSample(ctx, shard.GetVchannel())
		if err != nil {
			return err
		}
		if pair.GetGrowingTimetick() == 0 || pair.GetTransformingTimetick() == 0 {
			return merr.WrapErrServiceNotReadyMsg("no complete read prefix for vchannel %s", shard.GetVchannel())
		}
		shard.ReadAdmissionFloor = proto.Clone(pair).(*viewpb.QueryPlanMVCC)
	}
	current, unlock := m.lockStateForMutation(collectionID)
	if current == nil {
		return nil
	}
	defer unlock()
	if current != state || current.latest != frozen || frozen.view != physical {
		// Even a same-version legacy metadata repair can replace the frozen object.
		if current.latest != nil && !hasReadAdmission(current.latest.view) {
			m.queue.requireCertification(collectionID)
		}
		return nil
	}
	// A replacement owner must first cover the last certified coordinates.
	// Never manufacture an executable pair by independently clamping its axes.
	if previous := current.native; previous != nil {
		for _, shard := range candidate.GetShards() {
			old := previous.Shard(shard.GetVchannel())
			if old == nil {
				continue
			}
			floor := shard.GetReadAdmissionFloor()
			if floor.GetGrowingTimetick() < old.ReadAdmissionFloor.GetGrowingTimetick() ||
				floor.GetTransformingTimetick() < old.ReadAdmissionFloor.GetTransformingTimetick() {
				return merr.WrapErrServiceNotReadyMsg("read prefix for %s precedes its published floor", shard.GetVchannel())
			}
		}
	}
	candidate.DataVersion = nextDataVersion(frozen.view, dataViewAdvanceCompact)
	return m.persistLockedWithStats(ctx, current, candidate, maps.Clone(frozen.stats))
}
