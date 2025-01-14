package datacoord

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
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
	log.Info("DataCoord partitionStatsMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
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
		log.Ctx(context.TODO()).Error("meta update: update PartitionStatsInfo info fail", zap.Error(err))
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

func (psm *partitionStatsMeta) DropPartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	psm.Lock()
	defer psm.Unlock()
	// if the dropping partitionStats is the current version, should update currentPartitionStats
	currentVersion := psm.innerGetCurrentPartitionStatsVersion(info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel())
	if currentVersion == info.GetVersion() && currentVersion != emptyPartitionStatsVersion {
		infos := psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos
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

	if err := psm.catalog.DropPartitionStatsInfo(psm.ctx, info); err != nil {
		log.Error("meta update: drop PartitionStatsInfo info fail",
			zap.Int64("collectionID", info.GetCollectionID()),
			zap.Int64("partitionID", info.GetPartitionID()),
			zap.String("vchannel", info.GetVChannel()),
			zap.Int64("version", info.GetVersion()),
			zap.Error(err))
		return err
	}
	if _, ok := psm.partitionStatsInfos[info.GetVChannel()]; !ok {
		return nil
	}
	if _, ok := psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()]; !ok {
		return nil
	}
	delete(psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos, info.GetVersion())
	if len(psm.partitionStatsInfos[info.GetVChannel()][info.GetPartitionID()].infos) == 0 {
		delete(psm.partitionStatsInfos[info.GetVChannel()], info.GetPartitionID())
	}
	if len(psm.partitionStatsInfos[info.GetVChannel()]) == 0 {
		delete(psm.partitionStatsInfos, info.GetVChannel())
	}
	return nil
}

func (psm *partitionStatsMeta) SaveCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string, currentPartitionStatsVersion int64) error {
	psm.Lock()
	defer psm.Unlock()
	return psm.innerSaveCurrentPartitionStatsVersion(collectionID, partitionID, vChannel, currentPartitionStatsVersion)
}

func (psm *partitionStatsMeta) innerSaveCurrentPartitionStatsVersion(collectionID, partitionID int64, vChannel string, currentPartitionStatsVersion int64) error {
	log.Ctx(context.TODO()).Info("update current partition stats version", zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.String("vChannel", vChannel), zap.Int64("currentPartitionStatsVersion", currentPartitionStatsVersion))

	if _, ok := psm.partitionStatsInfos[vChannel]; !ok {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion",
			fmt.Errorf("update current partition stats version failed, there is no partition info exists with collID: %d, partID: %d, vChannel: %s", collectionID, partitionID, vChannel))
	}
	if _, ok := psm.partitionStatsInfos[vChannel][partitionID]; !ok {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion",
			fmt.Errorf("update current partition stats version failed, there is no partition info exists with collID: %d, partID: %d, vChannel: %s", collectionID, partitionID, vChannel))
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
