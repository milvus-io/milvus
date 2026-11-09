package partitionstats

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	if err := psm.ReloadFromKV(); err != nil {
		return nil, err
	}
	return psm, nil
}

func (psm *PartitionStatsMeta) ReloadFromKV() error {
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

func (psm *PartitionStatsMeta) SavePartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	psm.Lock()
	defer psm.Unlock()
	if err := psm.catalog.SavePartitionStatsInfo(context.TODO(), info); err != nil {
		mlog.Error(psm.ctx, "meta update: update PartitionStatsInfo info fail", mlog.Err(err))
		return err
	}
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()]; !ok {
		psm.PartitionStatsInfos[info.GetVChannel()] = make(map[int64]*PartitionStatsInfo)
	}
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()] = &PartitionStatsInfo{
			Infos: make(map[int64]*datapb.PartitionStatsInfo),
		}
	}

	psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos[info.GetVersion()] = info
	return nil
}

func (psm *PartitionStatsMeta) DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	psm.Lock()
	defer psm.Unlock()
	// if the dropping partitionStats is the current version, should update currentPartitionStats
	currentVersion := psm.innerGetCurrentPartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel())
	if currentVersion == info.GetVersion() && currentVersion != EmptyPartitionStatsVersion {
		infos := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos
		if len(infos) > 0 {
			var maxVersion int64 = 0
			for version := range infos {
				if version > maxVersion && version < currentVersion {
					maxVersion = version
				}
			}
			err := psm.innerSaveCurrentPartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel(), maxVersion)
			if err != nil {
				return err
			}
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
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()]; !ok {
		return nil
	}
	if _, ok := psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		return nil
	}
	delete(psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos, info.GetVersion())
	if len(psm.PartitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].Infos) == 0 {
		delete(psm.PartitionStatsInfos[info.GetVChannel()], info.GetPartitionID())
	}
	if len(psm.PartitionStatsInfos[info.GetVChannel()]) == 0 {
		delete(psm.PartitionStatsInfos, info.GetVChannel())
	}
	return nil
}

func (psm *PartitionStatsMeta) SaveCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string, currentPartitionStatsVersion int64) error {
	psm.Lock()
	defer psm.Unlock()
	return psm.innerSaveCurrentPartitionStatsVersion(collectionID, partitionID, vChannel, currentPartitionStatsVersion)
}

func (psm *PartitionStatsMeta) innerSaveCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string, currentPartitionStatsVersion int64) error {
	mlog.Info(psm.ctx, "update current partition stats version", mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(partitionID),
		mlog.String("vChannel", vChannel), mlog.Int64("currentPartitionStatsVersion", currentPartitionStatsVersion))

	if _, ok := psm.PartitionStatsInfos[vChannel]; !ok {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion",
			merr.WrapErrServiceInternalMsg("update current partition stats version failed, there is no partition info exists with collID: %d, partID: %d, vChannel: %s", collectionID, partitionID, vChannel))
	}
	if _, ok := psm.PartitionStatsInfos[vChannel][partitionID]; !ok {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion",
			merr.WrapErrServiceInternalMsg("update current partition stats version failed, there is no partition info exists with collID: %d, partID: %d, vChannel: %s", collectionID, partitionID, vChannel))
	}

	if err := psm.catalog.SaveCurrentPartitionStatsVersion(psm.ctx, collectionID, partitionID, vChannel, currentPartitionStatsVersion); err != nil {
		return err
	}

	psm.PartitionStatsInfos[vChannel][partitionID].CurrentVersion = currentPartitionStatsVersion
	return nil
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
