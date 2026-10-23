package partitionstats

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const EmptyPartitionStatsVersion = int64(0)

type PartitionStatsMeta struct {
	sync.RWMutex
	ctx                 context.Context
	catalog             metastore.DataCoordCatalog
	PartitionStatsInfos map[string]map[int64]*PartitionStatsInfo // channel -> partition -> PartitionStatsInfo
}

type PartitionStatsInfo struct {
	CurrentVersion int64
	Infos          map[int64]*datapb.PartitionStatsInfo
}

func NewPartitionStatsMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*PartitionStatsMeta, error) {
	psm := &PartitionStatsMeta{
		RWMutex:             sync.RWMutex{},
		ctx:                 ctx,
		catalog:             catalog,
		PartitionStatsInfos: make(map[string]map[int64]*PartitionStatsInfo),
	}
	if err := psm.reloadFromKV(); err != nil {
		return nil, err
	}
	return psm, nil
}

func (psm *PartitionStatsMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("partitionStatsMeta-reloadFromKV")

	partitionStatsInfos, err := psm.catalog.ListPartitionStatsInfos(psm.ctx)
	if err != nil {
		return err
	}
	for _, info := range partitionStatsInfos {
		if _, ok := psm.PartitionStatsInfos[info.GetVChannel()]; !ok {
			psm.PartitionStatsInfos[info.GetVChannel()] = make(map[int64]*PartitionStatsInfo)
		}
		if _, ok := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
			currentPartitionStatsVersion, err := psm.catalog.GetCurrentPartitionStatsVersion(psm.ctx, info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel())
			if err != nil {
				return err
			}
			psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()] = &PartitionStatsInfo{
				CurrentVersion: currentPartitionStatsVersion,
				Infos:          make(map[int64]*datapb.PartitionStatsInfo),
			}
		}
		psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos[info.GetVersion()] = info
	}
	mlog.Info(psm.ctx, "DataCoord partitionStatsMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (psm *PartitionStatsMeta) ListAllPartitionStatsInfos() []*datapb.PartitionStatsInfo {
	psm.RLock()
	defer psm.RUnlock()
	res := make([]*datapb.PartitionStatsInfo, 0)
	for _, partitionStats := range psm.PartitionStatsInfos {
		for _, infos := range partitionStats {
			for _, info := range infos.Infos {
				res = append(res, info)
			}
		}
	}
	return res
}

func (psm *PartitionStatsMeta) ListPartitionStatsInfos(collectionID int64, partitionID int64, vchannel string, filters ...func([]*datapb.PartitionStatsInfo) []*datapb.PartitionStatsInfo) []*datapb.PartitionStatsInfo {
	psm.RLock()
	defer psm.RUnlock()
	res := make([]*datapb.PartitionStatsInfo, 0)
	partitionStats, ok := psm.PartitionStatsInfos[vchannel]
	if !ok {
		return res
	}
	infos, ok := partitionStats[partitionID]
	if !ok {
		return res
	}
	for _, info := range infos.Infos {
		res = append(res, info)
	}

	for _, filter := range filters {
		res = filter(res)
	}
	return res
}

// SavePartitionStatsAndVersion persists a newly-computed partition-stats info
// together with the current-version pointer bump as a single composite catalog
// write, then applies the in-memory bookkeeping of both. It replaces the two
// sequential catalog writes a clustering-compaction completion used to do
// (SavePartitionStatsInfo followed by SaveCurrentPartitionStatsVersion), which
// left the current-version pointer disagreeing with the persisted stats set on
// a crash between them.
//
// The info is written before the version pointer, which is the logical commit
// marker: a persisted current version always references a persisted stats
// info. It is always exactly two ops, so it always fits a single atomic txn
// (the chunked fallback never triggers). In-memory state is applied only after
// the write succeeds, so a failed write never desyncs memory from disk.
func (psm *PartitionStatsMeta) SavePartitionStatsAndVersion(info *datapb.PartitionStatsInfo, currentVersion int64) error {
	psm.Lock()
	defer psm.Unlock()

	if err := psm.catalog.Update(psm.ctx,
		metastore.AddPartitionStats(info),
		metastore.SavePartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel(), currentVersion),
	); err != nil {
		mlog.Error(psm.ctx, "meta update: save PartitionStatsInfo and current version fail",
			mlog.FieldCollectionID(info.GetCollectionID()),
			mlog.FieldPartitionID(info.GetPartitionID()),
			mlog.FieldVChannel(info.GetVChannel()),
			mlog.Int64("version", info.GetVersion()),
			mlog.Err(err))
		return err
	}

	// Apply SavePartitionStatsInfo's in-memory bookkeeping (insert info),
	// then innerSaveCurrentPartitionStatsVersion's (set currentVersion). The
	// insert runs first so the (vchannel, partition) entry exists when the
	// version is set - mirroring the original two-call ordering.
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()]; !ok {
		psm.PartitionStatsInfos[info.GetVChannel()] = make(map[int64]*PartitionStatsInfo)
	}
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()] = &PartitionStatsInfo{
			Infos: make(map[int64]*datapb.PartitionStatsInfo),
		}
	}
	psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos[info.GetVersion()] = info
	psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].CurrentVersion = currentVersion
	return nil
}

// getRollbackVersionLocked returns the current-partition-stats-version
// rollback target for dropping info: the max known version below info's
// version, if info's version is the current one for its (collection,
// partition, vchannel); nil if dropping info requires no rollback (info is
// not the current version, or there is no current version at all).
//
// The caller must hold psm's write lock. Callers that persist a
// partition-stats drop via a composite catalog.Update (rather than through
// DropPartitionStatsInfo) compute the rollback under the SAME lock hold that
// spans the catalog write and applyDropLocked, so a concurrent
// SaveCurrentPartitionStatsVersion (e.g. a clustering-compaction completion
// for the same coll/part/vchannel) cannot interleave between compute and
// apply and get clobbered.
func (psm *PartitionStatsMeta) getRollbackVersionLocked(info *datapb.PartitionStatsInfo) *int64 {
	currentVersion := psm.innerGetCurrentPartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel())
	if currentVersion != info.GetVersion() || currentVersion == EmptyPartitionStatsVersion {
		return nil
	}
	infos := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos
	if len(infos) == 0 {
		return nil
	}
	var maxVersion int64
	for version := range infos {
		if version > maxVersion && version < currentVersion {
			maxVersion = version
		}
	}
	return &maxVersion
}

// applyDropLocked applies the in-memory bookkeeping for a partition-stats
// drop - the current-version rollback (when rollbackVersion is non-nil, as
// computed by getRollbackVersionLocked earlier in the same lock hold) and the
// removal of info from local state.
//
// The caller must hold psm's write lock across the whole
// getRollbackVersionLocked -> catalog.Update -> applyDropLocked sequence, and
// must only reach applyDropLocked after the catalog write has succeeded, so
// memory is never updated on a failed write and no concurrent version bump
// can interleave between the rollback compute and its application.
func (psm *PartitionStatsMeta) applyDropLocked(info *datapb.PartitionStatsInfo, rollbackVersion *int64) {
	if rollbackVersion != nil {
		if _, ok := psm.PartitionStatsInfos[info.GetVChannel()]; ok {
			if _, ok := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; ok {
				psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].CurrentVersion = *rollbackVersion
			}
		}
	}

	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()]; !ok {
		return
	}
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		return
	}
	delete(psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos, info.GetVersion())
	if len(psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos) == 0 {
		delete(psm.PartitionStatsInfos[info.GetVChannel()], info.GetPartitionID())
	}
	if len(psm.PartitionStatsInfos[info.GetVChannel()]) == 0 {
		delete(psm.PartitionStatsInfos, info.GetVChannel())
	}
}

func (psm *PartitionStatsMeta) GetCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string) int64 {
	psm.RLock()
	defer psm.RUnlock()
	return psm.innerGetCurrentPartitionStatsVersion(collectionID, partitionID, vChannel)
}

func (psm *PartitionStatsMeta) innerGetCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string) int64 {
	if _, ok := psm.PartitionStatsInfos[vChannel]; !ok {
		return EmptyPartitionStatsVersion
	}
	if _, ok := psm.PartitionStatsInfos[vChannel][partitionID]; !ok {
		return EmptyPartitionStatsVersion
	}
	return psm.PartitionStatsInfos[vChannel][partitionID].CurrentVersion
}

func (psm *PartitionStatsMeta) GetPartitionStats(collectionID, partitionID int64, vChannel string, version int64) *datapb.PartitionStatsInfo {
	psm.RLock()
	defer psm.RUnlock()

	if _, ok := psm.PartitionStatsInfos[vChannel]; !ok {
		return nil
	}
	if _, ok := psm.PartitionStatsInfos[vChannel][partitionID]; !ok {
		return nil
	}
	return psm.PartitionStatsInfos[vChannel][partitionID].Infos[version]
}

func (psm *PartitionStatsMeta) GetChannelPartitionsStatsVersion(collectionID int64, vChannel string) map[int64]int64 {
	psm.RLock()
	defer psm.RUnlock()

	result := make(map[int64]int64)
	partitionsStats := psm.PartitionStatsInfos[vChannel]
	for partitionID, info := range partitionsStats {
		result[partitionID] = info.CurrentVersion
	}

	return result
}
