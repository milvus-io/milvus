package datacoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const emptyPartitionStatsVersion = int64(0)

type partitionStatsMeta struct {
	sync.RWMutex
	ctx                 context.Context
	catalog             metastore.DataCoordCatalog
	partitionStatsInfos map[string]map[int64]*partitionStatsInfo // channel -> partition -> PartitionStatsInfo
}

type partitionStatsInfo struct {
	currentVersion int64
	infos          map[int64]*datapb.PartitionStatsInfo
}

func newPartitionStatsMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*partitionStatsMeta, error) {
	psm := &partitionStatsMeta{
		RWMutex:             sync.RWMutex{},
		ctx:                 ctx,
		catalog:             catalog,
		partitionStatsInfos: make(map[string]map[int64]*partitionStatsInfo),
	}
	if err := psm.reloadFromKV(); err != nil {
		return nil, err
	}
	return psm, nil
}

func (psm *partitionStatsMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("partitionStatsMeta-reloadFromKV")

	partitionStatsInfos, err := psm.catalog.ListPartitionStatsInfos(psm.ctx)
	if err != nil {
		return err
	}
	for _, info := range partitionStatsInfos {
		if _, ok := psm.partitionStatsInfos[info.GetVChannel()]; !ok {
			psm.partitionStatsInfos[info.GetVChannel()] = make(map[int64]*partitionStatsInfo)
		}
		if _, ok := psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
			currentPartitionStatsVersion, err := psm.catalog.GetCurrentPartitionStatsVersion(psm.ctx, info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel())
			if err != nil {
				return err
			}
			psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()] = &partitionStatsInfo{
				currentVersion: currentPartitionStatsVersion,
				infos:          make(map[int64]*datapb.PartitionStatsInfo),
			}
		}
		psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos[info.GetVersion()] = info
	}
	mlog.Info(psm.ctx, "DataCoord partitionStatsMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (psm *partitionStatsMeta) ListAllPartitionStatsInfos() []*datapb.PartitionStatsInfo {
	psm.RLock()
	defer psm.RUnlock()
	res := make([]*datapb.PartitionStatsInfo, 0)
	for _, partitionStats := range psm.partitionStatsInfos {
		for _, infos := range partitionStats {
			for _, info := range infos.infos {
				res = append(res, info)
			}
		}
	}
	return res
}

func (psm *partitionStatsMeta) ListPartitionStatsInfos(collectionID int64, partitionID int64, vchannel string, filters ...func([]*datapb.PartitionStatsInfo) []*datapb.PartitionStatsInfo) []*datapb.PartitionStatsInfo {
	psm.RLock()
	defer psm.RUnlock()
	res := make([]*datapb.PartitionStatsInfo, 0)
	partitionStats, ok := psm.partitionStatsInfos[vchannel]
	if !ok {
		return res
	}
	infos, ok := partitionStats[partitionID]
	if !ok {
		return res
	}
	for _, info := range infos.infos {
		res = append(res, info)
	}

	for _, filter := range filters {
		res = filter(res)
	}
	return res
}

func (psm *partitionStatsMeta) SavePartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	psm.Lock()
	defer psm.Unlock()
	if err := psm.catalog.SavePartitionStatsInfo(context.TODO(), info); err != nil {
		mlog.Error(psm.ctx, "meta update: update PartitionStatsInfo info fail", mlog.Err(err))
		return err
	}
	if _, ok := psm.partitionStatsInfos[info.GetVChannel()]; !ok {
		psm.partitionStatsInfos[info.GetVChannel()] = make(map[int64]*partitionStatsInfo)
	}
	if _, ok := psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()] = &partitionStatsInfo{
			infos: make(map[int64]*datapb.PartitionStatsInfo),
		}
	}

	psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos[info.GetVersion()] = info
	return nil
}

func (psm *partitionStatsMeta) DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	psm.Lock()
	defer psm.Unlock()
	// if the dropping partitionStats is the current version, should update currentPartitionStats
	if rollbackVersion := psm.getRollbackVersionLocked(info); rollbackVersion != nil {
		err := psm.innerSaveCurrentPartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel(), *rollbackVersion)
		if err != nil {
			return err
		}
	}

	if err := psm.catalog.DropPartitionStatsInfo(ctx, info); err != nil {
		mlog.Error(ctx, "meta update: drop PartitionStatsInfo info fail",
			mlog.FieldCollectionID(info.GetCollectionID()),
			mlog.FieldPartitionID(info.GetPartitionID()),
			mlog.FieldVChannel(info.GetVChannel()),
			mlog.Int64("version", info.GetVersion()),
			mlog.Err(err))
		return err
	}
	// the rollback (if any) was already applied in memory by
	// innerSaveCurrentPartitionStatsVersion above, so only the drop is applied here
	psm.applyDropLocked(info, nil)
	return nil
}

// getRollbackVersionLocked computes the current-version rollback required when
// dropping the given partition-stats version: if the dropped version is the
// (non-empty) current version and other versions exist, it returns the largest
// remaining version below it; otherwise nil (no rollback needed). Callers must
// hold at least the read lock.
func (psm *partitionStatsMeta) getRollbackVersionLocked(info *datapb.PartitionStatsInfo) *int64 {
	currentVersion := psm.innerGetCurrentPartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel())
	if currentVersion != info.GetVersion() || currentVersion == emptyPartitionStatsVersion {
		return nil
	}
	// a non-empty current version implies the vChannel/partition entry exists
	infos := psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos
	if len(infos) == 0 {
		return nil
	}
	var maxVersion int64 = 0
	for version := range infos {
		if version > maxVersion && version < currentVersion {
			maxVersion = version
		}
	}
	return &maxVersion
}

// applyDropLocked applies the in-memory effects of dropping a partition-stats
// version after the catalog write succeeded: it rolls the current version back
// when rollbackVersion is non-nil and removes the info from the three-level
// map. Callers must hold the write lock.
func (psm *partitionStatsMeta) applyDropLocked(info *datapb.PartitionStatsInfo, rollbackVersion *int64) {
	if rollbackVersion != nil {
		// getRollbackVersionLocked returns non-nil only when the dropped version
		// is the non-empty current version, which implies the vChannel/partition
		// entry exists — the "no partition info" error branch of
		// innerSaveCurrentPartitionStatsVersion is unreachable here, so this
		// in-memory update cannot fail.
		mlog.Info(psm.ctx, "update current partition stats version", mlog.FieldCollectionID(info.GetCollectionID()),
			mlog.FieldPartitionID(info.GetPartitionID()),
			mlog.String("vChannel", info.GetVChannel()), mlog.Int64("currentPartitionStatsVersion", *rollbackVersion))
		psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].currentVersion = *rollbackVersion
	}
	if _, ok := psm.partitionStatsInfos[info.GetVChannel()]; !ok {
		return
	}
	if _, ok := psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		return
	}
	delete(psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos, info.GetVersion())
	if len(psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos) == 0 {
		delete(psm.partitionStatsInfos[info.GetVChannel()], info.GetPartitionID())
	}
	if len(psm.partitionStatsInfos[info.GetVChannel()]) == 0 {
		delete(psm.partitionStatsInfos, info.GetVChannel())
	}
}

func (psm *partitionStatsMeta) SaveCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string, currentPartitionStatsVersion int64) error {
	psm.Lock()
	defer psm.Unlock()
	return psm.innerSaveCurrentPartitionStatsVersion(collectionID, partitionID, vChannel, currentPartitionStatsVersion)
}

func (psm *partitionStatsMeta) innerSaveCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string, currentPartitionStatsVersion int64) error {
	mlog.Info(psm.ctx, "update current partition stats version", mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(partitionID),
		mlog.String("vChannel", vChannel), mlog.Int64("currentPartitionStatsVersion", currentPartitionStatsVersion))

	if _, ok := psm.partitionStatsInfos[vChannel]; !ok {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion",
			merr.WrapErrServiceInternalMsg("update current partition stats version failed, there is no partition info exists with collID: %d, partID: %d, vChannel: %s", collectionID, partitionID, vChannel))
	}
	if _, ok := psm.partitionStatsInfos[vChannel][partitionID]; !ok {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion",
			merr.WrapErrServiceInternalMsg("update current partition stats version failed, there is no partition info exists with collID: %d, partID: %d, vChannel: %s", collectionID, partitionID, vChannel))
	}

	if err := psm.catalog.SaveCurrentPartitionStatsVersion(psm.ctx, collectionID, partitionID, vChannel, currentPartitionStatsVersion); err != nil {
		return err
	}

	psm.partitionStatsInfos[vChannel][partitionID].currentVersion = currentPartitionStatsVersion
	return nil
}

func (psm *partitionStatsMeta) GetCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string) int64 {
	psm.RLock()
	defer psm.RUnlock()
	return psm.innerGetCurrentPartitionStatsVersion(collectionID, partitionID, vChannel)
}

func (psm *partitionStatsMeta) innerGetCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string) int64 {
	if _, ok := psm.partitionStatsInfos[vChannel]; !ok {
		return emptyPartitionStatsVersion
	}
	if _, ok := psm.partitionStatsInfos[vChannel][partitionID]; !ok {
		return emptyPartitionStatsVersion
	}
	return psm.partitionStatsInfos[vChannel][partitionID].currentVersion
}

func (psm *partitionStatsMeta) GetPartitionStats(collectionID, partitionID int64, vChannel string, version int64) *datapb.PartitionStatsInfo {
	psm.RLock()
	defer psm.RUnlock()

	if _, ok := psm.partitionStatsInfos[vChannel]; !ok {
		return nil
	}
	if _, ok := psm.partitionStatsInfos[vChannel][partitionID]; !ok {
		return nil
	}
	return psm.partitionStatsInfos[vChannel][partitionID].infos[version]
}

func (psm *partitionStatsMeta) GetChannelPartitionsStatsVersion(collectionID int64, vChannel string) map[int64]int64 {
	psm.RLock()
	defer psm.RUnlock()

	result := make(map[int64]int64)
	partitionsStats := psm.partitionStatsInfos[vChannel]
	for partitionID, info := range partitionsStats {
		result[partitionID] = info.currentVersion
	}

	return result
}
