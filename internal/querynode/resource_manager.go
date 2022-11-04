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

package querynode

import (
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
)

// TODO we need extra memory estimation about the index
const (
	UsedDiskMemoryRatio = 6
)

type ResourceManager struct {
	totalMem        uint64
	memoryWaterMark uint64
	totalDisk       uint64
	diskWaterMark   uint64

	reservedMemory uint64
	reservedDisk   uint64
	concurrentTask uint64

	sync.Mutex
	condition *sync.Cond
}

func (manager *ResourceManager) Init() error {
	manager.totalMem = hardware.GetMemoryCount()
	if manager.totalMem == 0 {
		return fmt.Errorf("get total memory failed")
	}
	if Params.QueryNodeCfg.EnableDisk {
		manager.totalDisk = uint64(Params.QueryNodeCfg.DiskCapacityLimit)
	} else {
		manager.totalDisk = 0
	}
	manager.memoryWaterMark = uint64(float64(manager.totalMem) * Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage)
	log.Info("Resource manager init", zap.Uint64("total memory", toMB(manager.totalMem)),
		zap.Uint64("total disk", toMB(manager.totalDisk)),
		zap.Uint64("memory watermark", toMB(manager.memoryWaterMark)),
		zap.Uint64("disk watermark", toMB(manager.memoryWaterMark)))
	manager.diskWaterMark = uint64(float64(manager.totalDisk) * Params.QueryNodeCfg.MaxDiskUsagePercentage)
	manager.reservedMemory = 0
	manager.reservedDisk = 0
	manager.concurrentTask = 0
	manager.condition = sync.NewCond(manager)
	return nil
}

// estimate memory usage of a segment
// TODO the estimation has to improve, make it control load accuracy
func (manager *ResourceManager) estimateUsage(loadInfo *querypb.SegmentLoadInfo) (memory, disk uint64) {
	vecFieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
	for _, fieldIndexInfo := range loadInfo.IndexInfos {
		if fieldIndexInfo.EnableIndex {
			fieldID := fieldIndexInfo.FieldID
			vecFieldID2IndexInfo[fieldID] = fieldIndexInfo
		}
	}

	for _, fieldBinlog := range loadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		if fieldIndexInfo, ok := vecFieldID2IndexInfo[fieldID]; ok {
			neededMemSize, neededDiskSize, err := GetStorageSizeByIndexInfo(fieldIndexInfo)
			if err != nil {
				// this is dangerous, we don't have a estimate about the current index
				log.Error(err.Error(), zap.Int64("collectionID", loadInfo.CollectionID),
					zap.Int64("segmentID", loadInfo.SegmentID),
					zap.Int64("indexBuildID", fieldIndexInfo.BuildID))
				return 0, 0
			}
			memory += neededMemSize
			disk += neededDiskSize
		} else {
			memory += uint64(funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog))
		}
	}

	// get size of state data
	for _, fieldBinlog := range loadInfo.Statslogs {
		memory += uint64(funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog))
	}

	// get size of delete data
	for _, fieldBinlog := range loadInfo.Deltalogs {
		memory += uint64(funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog))
	}
	return memory, disk
}

// 1. Allow at least one task to excuate, unless the allocation is too large and directly return error
// 2. if multiple task reserve resources, check if there is enough resource, otherwise block the reserver
// 3. Be noticed, this call maybe blocked if not enough resource!!
func (manager *ResourceManager) reserve(collectionID UniqueID, segmentLoadInfo *querypb.SegmentLoadInfo) error {
	manager.Lock()
	defer manager.Unlock()

	for {
		usedMem := hardware.GetUsedMemoryCount()
		if usedMem == 0 {
			return fmt.Errorf("get used memory failed , collectionID = %d", collectionID)
		}

		localDiskUsedSize, err := GetLocalDiskUsedSize()
		if err != nil {
			log.Warn("failed to get local used disk size", zap.Error(err))
			return fmt.Errorf("get local used size failed, collectionID = %d", collectionID)
		}

		memory, disk := manager.estimateUsage(segmentLoadInfo)

		memAfterLoad := usedMem + memory
		memLoadingUsage := usedMem + uint64(float64(memory)*Params.QueryNodeCfg.LoadMemoryUsageFactor) + manager.reservedMemory
		diskAfterLoad := disk + uint64(localDiskUsedSize)
		diskLoadingUsage := diskAfterLoad + manager.reservedDisk
		log.Info("predict memory and disk usage while loading (in MiB)",
			zap.Int64("collectionID", collectionID),
			zap.Uint64("memUsageEstimation", toMB(memory)),
			zap.Uint64("memLoadingUsage", toMB(memLoadingUsage)),
			zap.Uint64("memUsageAfterLoad", toMB(memAfterLoad)),
			zap.Uint64("diskUsageAfterLoad", toMB(diskAfterLoad)),
			zap.Uint64("diskLoadingUsage", toMB(diskLoadingUsage)))

		// memory after load is too much, load can not successful anyway
		if memAfterLoad > manager.memoryWaterMark {
			return fmt.Errorf("load segment failed, OOM if load, collectionID = %d, memoryEstimation = %v MB, usedMemAfterLoad = %v MB, totalMem = %v MB, thresholdFactor = %f",
				collectionID,
				toMB(memory),
				toMB(memAfterLoad),
				toMB(manager.totalMem),
				Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage)
		}

		if diskAfterLoad > manager.diskWaterMark {
			return fmt.Errorf("load segment failed, disk space is not enough, collectionID = %d, diskEstimation = %v MB, usedDiskAfterLoad = %v MB, totalDisk = %v MB, thresholdFactor = %f",
				collectionID,
				toMB(disk),
				toMB(diskAfterLoad),
				toMB(manager.totalDisk),
				Params.QueryNodeCfg.MaxDiskUsagePercentage)
		}

		// resource is enough, reserve and execute
		if memLoadingUsage < manager.memoryWaterMark && diskLoadingUsage < manager.diskWaterMark {
			manager.reservedDisk += disk
			manager.reservedMemory += uint64(float64(memory) * Params.QueryNodeCfg.LoadMemoryUsageFactor)
			manager.concurrentTask++
			return nil
		}

		// if resource is not enough for concurrent load, but this is currently the only task on flight, try it anyway.
		if manager.concurrentTask == 0 {
			manager.reservedDisk += disk
			manager.reservedMemory += uint64(float64(memory) * Params.QueryNodeCfg.LoadMemoryUsageFactor)
			manager.concurrentTask++
			return nil
		}

		// wait for someone release some reserved resource
		log.Info("wait for resource release to do load ", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentLoadInfo.SegmentID))
		manager.condition.Wait()
	}
}

// return resources reserved.
func (manager *ResourceManager) release(collectionID UniqueID, segmentLoadInfo *querypb.SegmentLoadInfo) {
	// reserve memory
	manager.Lock()
	defer manager.Unlock()
	memory, disk := manager.estimateUsage(segmentLoadInfo)
	manager.reservedDisk -= disk
	manager.reservedMemory -= uint64(float64(memory) * Params.QueryNodeCfg.LoadMemoryUsageFactor)
	manager.concurrentTask--
	// notify the waiter in reserve
	manager.condition.Broadcast()
}

func toMB(mem uint64) uint64 {
	return mem / 1024 / 1024
}

func GetStorageSizeByIndexInfo(indexInfo *querypb.FieldIndexInfo) (uint64, uint64, error) {
	indexType, err := funcutil.GetAttrByKeyFromRepeatedKV("index_type", indexInfo.IndexParams)
	if err != nil {
		return 0, 0, fmt.Errorf("index type not exist in index params")
	}
	if indexType == indexparamcheck.IndexDISKANN {
		neededMemSize := indexInfo.IndexSize / UsedDiskMemoryRatio
		neededDiskSize := indexInfo.IndexSize
		return uint64(neededMemSize), uint64(neededDiskSize), nil
	}

	return uint64(indexInfo.IndexSize), 0, nil
}
