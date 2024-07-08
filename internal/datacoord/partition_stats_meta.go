// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type partitionStatsMeta struct {
	sync.RWMutex
	ctx                 context.Context
	chunkManager        storage.ChunkManager
	catalog             metastore.DataCoordCatalog
	partitionStatsInfos map[string]map[int64]*partitionStatsInfo // channel -> partition -> PartitionStatsInfo
}

type partitionStatsInfo struct {
	currentVersion int64
	infos          map[int64]*datapb.PartitionStatsInfo
}

func newPartitionStatsMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager) (*partitionStatsMeta, error) {
	psm := &partitionStatsMeta{
		RWMutex:             sync.RWMutex{},
		ctx:                 ctx,
		catalog:             catalog,
		chunkManager:        chunkManager,
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

func (psm *partitionStatsMeta) SavePartitionStatsInfo(info *datapb.PartitionStatsInfo, removeSegmentIds []int64) error {
	psm.Lock()
	defer psm.Unlock()

	var toUpdateInfo *datapb.PartitionStatsInfo
	currentPartitionStats := psm.innerGetCurrentPartitionStats(info.CollectionID, info.PartitionID, info.GetVChannel())
	if currentPartitionStats != nil {
		existPartitionStats, err := psm.readPartitionStats(info.CollectionID, info.PartitionID, info.GetVChannel(), currentPartitionStats.currentVersion)
		if err != nil {
			return err
		}
		newPartitionStats, err := psm.readPartitionStats(info.CollectionID, info.PartitionID, info.GetVChannel(), info.Version)
		if err != nil {
			return err
		}
		mergedPartitionStats, err := storage.MergePartitionsStatsSnapshot(existPartitionStats, newPartitionStats, removeSegmentIds, info.Version)
		if err != nil {
			return err
		}
		err = psm.writePartitionStats(info.CollectionID, info.PartitionID, info.GetVChannel(), info.Version, mergedPartitionStats)
		if err != nil {
			return err
		}
		segmentIDs := lo.MapToSlice(mergedPartitionStats.SegmentStats, func(id int64, x storage.SegmentStats) int64 {
			return id
		})
		toUpdateInfo = &datapb.PartitionStatsInfo{
			CollectionID:  info.CollectionID,
			PartitionID:   info.PartitionID,
			VChannel:      info.VChannel,
			Version:       info.Version,
			SegmentIDs:    segmentIDs,
			AnalyzeTaskID: info.AnalyzeTaskID,
		}
	} else {
		toUpdateInfo = info
	}

	if err := psm.catalog.SavePartitionStatsInfo(psm.ctx, toUpdateInfo); err != nil {
		log.Error("meta update: update PartitionStatsInfo info fail", zap.Error(err))
		return err
	}
	if _, ok := psm.partitionStatsInfos[toUpdateInfo.GetVChannel()]; !ok {
		psm.partitionStatsInfos[toUpdateInfo.GetVChannel()] = make(map[int64]*partitionStatsInfo)
	}
	if _, ok := psm.partitionStatsInfos[toUpdateInfo.GetVChannel()][info.GetPartitionID()]; !ok {
		psm.partitionStatsInfos[toUpdateInfo.GetVChannel()][toUpdateInfo.GetPartitionID()] = &partitionStatsInfo{
			infos: make(map[int64]*datapb.PartitionStatsInfo),
		}
	}

	psm.partitionStatsInfos[toUpdateInfo.GetVChannel()][toUpdateInfo.GetPartitionID()].infos[toUpdateInfo.GetVersion()] = toUpdateInfo
	return nil
}

func (psm *partitionStatsMeta) DropPartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	psm.Lock()
	defer psm.Unlock()
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

	log.Info("update current partition stats version", zap.Int64("collectionID", collectionID),
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
	currentPartitionStats := psm.innerGetCurrentPartitionStats(collectionID, partitionID, vChannel)
	if currentPartitionStats == nil {
		return 0
	}
	return psm.innerGetCurrentPartitionStats(collectionID, partitionID, vChannel).currentVersion
}

// only use inside partitionStatsMeta
func (psm *partitionStatsMeta) innerGetCurrentPartitionStats(collectionID, partitionID int64, vChannel string) *partitionStatsInfo {
	if _, ok := psm.partitionStatsInfos[vChannel]; !ok {
		return nil
	}
	if _, ok := psm.partitionStatsInfos[vChannel][partitionID]; !ok {
		return nil
	}
	return psm.partitionStatsInfos[vChannel][partitionID]
}

func (psm *partitionStatsMeta) readPartitionStats(colID, partID int64, vchannel string, version int64) (*storage.PartitionStatsSnapshot, error) {
	log := log.With(zap.Int64("collectionID", colID), zap.Int64("partitionID", partID), zap.String("vchannel", vchannel))
	idPath := metautil.JoinIDPath(colID, partID)
	idPath = path.Join(idPath, vchannel)
	statsFilePath := path.Join(psm.chunkManager.RootPath(), common.PartitionStatsPath, idPath, strconv.FormatInt(version, 10))
	statsBytes, err := psm.chunkManager.Read(context.Background(), statsFilePath)
	if err != nil {
		log.Error("failed to read stats file from object storage", zap.String("path", statsFilePath))
		return nil, err
	}
	partStats, err := storage.DeserializePartitionsStatsSnapshot(statsBytes)
	if err != nil {
		log.Error("failed to parse partition stats from bytes", zap.Int("bytes_length", len(statsBytes)), zap.Error(err))
		return nil, err
	}
	log.Info("read partitionStats to memory", zap.String("path", statsFilePath))
	return partStats, nil
}

func (psm *partitionStatsMeta) writePartitionStats(colID, partID int64, vchannel string, version int64, partitions *storage.PartitionStatsSnapshot) error {
	log := log.With(zap.Int64("collectionID", colID), zap.Int64("partitionID", partID), zap.String("vchannel", vchannel))
	idPath := metautil.JoinIDPath(colID, partID)
	idPath = path.Join(idPath, vchannel)
	statsFilePath := path.Join(psm.chunkManager.RootPath(), common.PartitionStatsPath, idPath, strconv.FormatInt(version, 10))
	bytes, err := storage.SerializePartitionStatsSnapshot(partitions)
	if err != nil {
		log.Error("failed to SerializePartitionStatsSnapshot", zap.String("path", statsFilePath))
		return err
	}
	log.Info("write partitionStats to storage", zap.String("path", statsFilePath))
	return psm.chunkManager.Write(context.Background(), statsFilePath, bytes)
}
