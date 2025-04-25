package manager

import (
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// buildNewPartitionManagers builds new partition managers.
func buildNewPartitionManagers(
	wal *syncutil.Future[wal.WAL],
	pchannel types.PChannelInfo,
	recoverInfos *recovery.RecoverySnapshot,
	metrics *metricsutil.SegmentAssignMetrics,
	allocSegmentWorker *allocSegmentWorker,
) (partitionManager *partitionSegmentManagers,
	growingBelongs []stats.SegmentBelongs,
	growingStats []*stats.SegmentStats,
	waitForSealed []*segmentAllocManager,
) {
	// create a map to check if the partition exists.
	partitionExist := make(map[int64]struct{}, len(recoverInfos.VChannels))
	// collectionMap is a map from collectionID to collectionInfo.
	collectionInfoMap := make(map[int64]*CollectionInfo, len(recoverInfos.VChannels))
	for _, vchannelInfo := range recoverInfos.VChannels {
		currentPartition := make(map[int64]struct{}, len(vchannelInfo.CollectionInfo.Partitions))
		for _, partition := range vchannelInfo.CollectionInfo.Partitions {
			partitionExist[partition.PartitionId] = struct{}{}
			currentPartition[partition.PartitionId] = struct{}{}
		}
		collectionInfoMap[vchannelInfo.CollectionInfo.CollectionId] = &CollectionInfo{
			VChannel:     vchannelInfo.Vchannel,
			PartitionIDs: currentPartition,
		}
	}

	// recover the segment infos from the streaming node segment assignment meta storage
	metaMaps := make(map[int64][]*segmentAllocManager)
	for _, rawMeta := range recoverInfos.SegmentAssignments {
		m, stat := newSegmentAllocManagerFromProto(pchannel, rawMeta, metrics)
		if _, ok := partitionExist[rawMeta.GetPartitionId()]; !ok {
			// related collection or partition is not exist.
			// should be sealed right now.
			panic("segment assignment meta is dirty, a critical bug in system")
		}

		// The stats of growing segment should be managed by global stats manager.
		growingBelongs = append(growingBelongs, stats.SegmentBelongs{
			PChannel:     pchannel.Name,
			VChannel:     m.GetVChannel(),
			CollectionID: rawMeta.GetCollectionId(),
			PartitionID:  rawMeta.GetPartitionId(),
			SegmentID:    m.GetSegmentID(),
		})
		growingStats = append(growingStats, stat)
		if _, ok := metaMaps[rawMeta.GetPartitionId()]; !ok {
			metaMaps[rawMeta.GetPartitionId()] = make([]*segmentAllocManager, 0, 2)
		}
		metaMaps[rawMeta.GetPartitionId()] = append(metaMaps[rawMeta.GetPartitionId()], m)
	}

	// create managers list.
	managers := typeutil.NewConcurrentMap[int64, *partitionSegmentManager]()
	for collectionID, collectionInfo := range collectionInfoMap {
		for partitionID := range collectionInfo.PartitionIDs {
			segmentManagers := make([]*segmentAllocManager, 0)
			// recovery meta is recovered , use it.
			if managers, ok := metaMaps[partitionID]; ok {
				segmentManagers = managers
			}
			// otherwise, just create a new manager.
			_, ok := managers.GetOrInsert(partitionID, newPartitionSegmentManager(
				wal,
				pchannel,
				collectionInfo.VChannel,
				collectionID,
				partitionID,
				segmentManagers,
				allocSegmentWorker,
				metrics,
			))
			if ok {
				panic("partition manager already exists when buildNewPartitionManagers in segment assignment service, there's a bug in system")
			}
		}
	}
	m := &partitionSegmentManagers{
		mu: sync.Mutex{},
		logger: resource.Resource().Logger().With(
			log.FieldComponent("segment-assigner"),
			zap.String("pchannel", pchannel.Name),
		),
		wal:                wal,
		pchannel:           pchannel,
		managers:           managers,
		collectionInfos:    collectionInfoMap,
		allocSegmentWorker: allocSegmentWorker,
		metrics:            metrics,
	}
	m.updateMetrics()
	return m, growingBelongs, growingStats, waitForSealed
}

// partitionSegmentManagers is a collection of partition managers.
type partitionSegmentManagers struct {
	mu sync.Mutex

	logger             *log.MLogger
	wal                *syncutil.Future[wal.WAL]
	pchannel           types.PChannelInfo
	managers           *typeutil.ConcurrentMap[int64, *partitionSegmentManager] // map partitionID to partition manager
	collectionInfos    map[int64]*CollectionInfo                                // map collectionID to collectionInfo
	allocSegmentWorker *allocSegmentWorker
	metrics            *metricsutil.SegmentAssignMetrics
}

type CollectionInfo struct {
	VChannel     string
	PartitionIDs map[int64]struct{}
}

// NewCollection creates a new partition manager.
func (m *partitionSegmentManagers) NewCollection(collectionID int64, vchannel string, partitionID []int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.collectionInfos[collectionID]; ok {
		m.logger.Warn("collection already exists when NewCollection in segment assignment service",
			zap.Int64("collectionID", collectionID),
		)
		return
	}

	m.collectionInfos[collectionID] = newCollectionInfo(collectionID, vchannel, partitionID)
	for _, partitionID := range partitionID {
		if _, loaded := m.managers.GetOrInsert(partitionID, newPartitionSegmentManager(
			m.wal,
			m.pchannel,
			vchannel,
			collectionID,
			partitionID,
			make([]*segmentAllocManager, 0),
			m.allocSegmentWorker,
			m.metrics,
		)); loaded {
			m.logger.Warn("partition already exists when NewCollection in segment assignment service, it's may be a bug in system",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
			)
		}
	}
	m.logger.Info("collection created in segment assignment service",
		zap.Int64("collectionID", collectionID),
		zap.String("vchannel", vchannel),
		zap.Int64s("partitionIDs", partitionID))
	m.updateMetrics()
}

// NewPartition creates a new partition manager.
func (m *partitionSegmentManagers) NewPartition(collectionID int64, partitionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.collectionInfos[collectionID]; !ok {
		m.logger.Warn("collection not exists when NewPartition in segment assignment service, it's may be a bug in system",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID),
		)
		return
	}
	if _, ok := m.collectionInfos[collectionID].PartitionIDs[partitionID]; ok {
		m.logger.Warn("partition already exists when NewPartition in segment assignment service, it's may be a bug in system",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID),
		)
		return
	}

	m.collectionInfos[collectionID].PartitionIDs[partitionID] = struct{}{}
	if _, loaded := m.managers.GetOrInsert(partitionID, newPartitionSegmentManager(
		m.wal,
		m.pchannel,
		m.collectionInfos[collectionID].VChannel,
		collectionID,
		partitionID,
		make([]*segmentAllocManager, 0),
		m.allocSegmentWorker,
		m.metrics,
	)); loaded {
		m.logger.Warn(
			"partition already exists when NewPartition in segment assignment service, it's may be a bug in system",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
	}
	m.logger.Info("partition created in segment assignment service",
		zap.Int64("collectionID", collectionID),
		zap.String("vchannel", m.collectionInfos[collectionID].VChannel),
		zap.Int64("partitionID", partitionID))
	m.updateMetrics()
}

// Get gets a partition manager from the partition managers.
func (m *partitionSegmentManagers) Get(collectionID int64, partitionID int64) (*partitionSegmentManager, error) {
	pm, ok := m.managers.Get(partitionID)
	if !ok {
		return nil, status.NewUnrecoverableError("partition %d in collection %d not found in segment assignment service", partitionID, collectionID)
	}
	return pm, nil
}

// RemoveCollection removes a collection manager from the partition managers.
// Return the segments that need to be sealed.
func (m *partitionSegmentManagers) RemoveCollection(collectionID int64) []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	collectionInfo, ok := m.collectionInfos[collectionID]
	if !ok {
		m.logger.Warn("collection not exists when RemoveCollection in segment assignment service", zap.Int64("collectionID", collectionID))
		return nil
	}
	delete(m.collectionInfos, collectionID)

	needSealed := make([]*segmentAllocManager, 0)
	partitionIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	segmentIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	for partitionID := range collectionInfo.PartitionIDs {
		pm, ok := m.managers.Get(partitionID)
		if ok {
			segments := pm.CollectAllAndClear(policy.PolicyCollectionRemoved())
			partitionIDs = append(partitionIDs, partitionID)
			for _, segment := range segments {
				segmentIDs = append(segmentIDs, segment.GetSegmentID())
			}
			needSealed = append(needSealed, segments...)
			m.managers.Remove(partitionID)
		}
	}
	m.logger.Info(
		"collection removed in segment assignment service",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64s("segmentIDs", segmentIDs),
	)
	m.updateMetrics()
	return needSealed
}

// RemovePartition removes a partition manager from the partition managers.
func (m *partitionSegmentManagers) RemovePartition(collectionID int64, partitionID int64) []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	collectionInfo, ok := m.collectionInfos[collectionID]
	if !ok {
		m.logger.Warn("collection not exists when RemovePartition in segment assignment service", zap.Int64("collectionID", collectionID))
		return nil
	}
	if _, ok := collectionInfo.PartitionIDs[partitionID]; !ok {
		m.logger.Warn("partition not exists when RemovePartition in segment assignment service",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
		return nil
	}
	delete(collectionInfo.PartitionIDs, partitionID)

	pm, loaded := m.managers.GetAndRemove(partitionID)
	if !loaded {
		m.logger.Warn("partition not exists when RemovePartition in segment assignment service",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
		return nil
	}
	segments := pm.CollectAllAndClear(policy.PolicyPartitionRemoved())
	segmentIDs := make([]int64, 0, len(segments))
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.GetSegmentID())
	}
	m.logger.Info(
		"partition removed in segment assignment service",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64s("segmentIDs", segmentIDs),
	)
	m.updateMetrics()
	return segments
}

// SealAndFenceSegmentUntil seal all segment that contains the message less than the incoming timetick.
func (m *partitionSegmentManagers) SealAndFenceSegmentUntil(collectionID int64, timetick uint64) ([]*segmentAllocManager, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	collectionInfo, ok := m.collectionInfos[collectionID]
	if !ok {
		m.logger.Warn("collection not exists when Flush in segment assignment service", zap.Int64("collectionID", collectionID))
		return nil, errors.New("collection not found")
	}

	sealedSegments := make([]*segmentAllocManager, 0)
	segmentIDs := make([]int64, 0)
	// collect all partitions
	for partitionID := range collectionInfo.PartitionIDs {
		// Seal all segments and fence assign to the partition manager.
		pm, ok := m.managers.Get(partitionID)
		if !ok {
			m.logger.Warn("partition not found when Flush in segment assignment service, it's may be a bug in system",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID))
			return nil, errors.New("partition not found")
		}
		newSealedSegments := pm.SealAndFenceSegmentUntil(timetick)
		for _, segment := range newSealedSegments {
			segmentIDs = append(segmentIDs, segment.GetSegmentID())
		}
		sealedSegments = append(sealedSegments, newSealedSegments...)
	}
	m.logger.Info(
		"all segments sealed and fence assign until timetick in segment assignment service",
		zap.Int64("collectionID", collectionID),
		zap.Uint64("timetick", timetick),
		zap.Int64s("segmentIDs", segmentIDs),
	)
	return sealedSegments, nil
}

// Range ranges the partition managers.
func (m *partitionSegmentManagers) Range(f func(pm *partitionSegmentManager)) {
	m.managers.Range(func(_ int64, pm *partitionSegmentManager) bool {
		f(pm)
		return true
	})
}

func (m *partitionSegmentManagers) updateMetrics() {
	m.metrics.UpdatePartitionCount(m.managers.Len())
	m.metrics.UpdateCollectionCount(len(m.collectionInfos))
}

// newCollectionInfo creates a new collection info.
func newCollectionInfo(collectionID int64, vchannel string, partitionIDs []int64) *CollectionInfo {
	info := &CollectionInfo{
		VChannel:     vchannel,
		PartitionIDs: make(map[int64]struct{}, len(partitionIDs)),
	}
	for _, partitionID := range partitionIDs {
		info.PartitionIDs[partitionID] = struct{}{}
	}
	return info
}
