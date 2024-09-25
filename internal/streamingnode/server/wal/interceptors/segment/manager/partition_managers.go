package manager

import (
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// buildNewPartitionManagers builds new partition managers.
func buildNewPartitionManagers(
	pchannel types.PChannelInfo,
	rawMetas []*streamingpb.SegmentAssignmentMeta,
	collectionInfos []*rootcoordpb.CollectionInfoOnPChannel,
) (*partitionSegmentManagers, []*segmentAllocManager) {
	// create a map to check if the partition exists.
	partitionExist := make(map[int64]struct{}, len(collectionInfos))
	// collectionMap is a map from collectionID to collectionInfo.
	collectionInfoMap := make(map[int64]*rootcoordpb.CollectionInfoOnPChannel, len(collectionInfos))
	for _, collectionInfo := range collectionInfos {
		for _, partition := range collectionInfo.GetPartitions() {
			partitionExist[partition.GetPartitionId()] = struct{}{}
		}
		collectionInfoMap[collectionInfo.GetCollectionId()] = collectionInfo
	}

	// recover the segment infos from the streaming node segment assignment meta storage
	waitForSealed := make([]*segmentAllocManager, 0)
	metaMaps := make(map[int64][]*segmentAllocManager)
	for _, rawMeta := range rawMetas {
		m := newSegmentAllocManagerFromProto(pchannel, rawMeta)
		if _, ok := partitionExist[rawMeta.GetPartitionId()]; !ok {
			// related collection or partition is not exist.
			// should be sealed right now.
			waitForSealed = append(waitForSealed, m)
			continue
		}
		if _, ok := metaMaps[rawMeta.GetPartitionId()]; !ok {
			metaMaps[rawMeta.GetPartitionId()] = make([]*segmentAllocManager, 0, 2)
		}
		metaMaps[rawMeta.GetPartitionId()] = append(metaMaps[rawMeta.GetPartitionId()], m)
	}

	// create managers list.
	managers := typeutil.NewConcurrentMap[int64, *partitionSegmentManager]()
	for collectionID, collectionInfo := range collectionInfoMap {
		for _, partition := range collectionInfo.GetPartitions() {
			segmentManagers := make([]*segmentAllocManager, 0)
			// recovery meta is recovered , use it.
			if managers, ok := metaMaps[partition.GetPartitionId()]; ok {
				segmentManagers = managers
			}
			// otherwise, just create a new manager.
			_, ok := managers.GetOrInsert(partition.GetPartitionId(), newPartitionSegmentManager(
				pchannel,
				collectionInfo.GetVchannel(),
				collectionID,
				partition.GetPartitionId(),
				segmentManagers,
			))
			if ok {
				panic("partition manager already exists when buildNewPartitionManagers in segment assignment service, there's a bug in system")
			}
		}
	}
	return &partitionSegmentManagers{
		mu:              sync.Mutex{},
		logger:          log.With(zap.Any("pchannel", pchannel)),
		pchannel:        pchannel,
		managers:        managers,
		collectionInfos: collectionInfoMap,
	}, waitForSealed
}

// partitionSegmentManagers is a collection of partition managers.
type partitionSegmentManagers struct {
	mu sync.Mutex

	logger          *log.MLogger
	pchannel        types.PChannelInfo
	managers        *typeutil.ConcurrentMap[int64, *partitionSegmentManager] // map partitionID to partition manager
	collectionInfos map[int64]*rootcoordpb.CollectionInfoOnPChannel          // map collectionID to collectionInfo
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
			m.pchannel,
			vchannel,
			collectionID,
			partitionID,
			make([]*segmentAllocManager, 0),
		)); loaded {
			m.logger.Warn("partition already exists when NewCollection in segment assignment service, it's may be a bug in system",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
			)
		}
	}
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
	m.collectionInfos[collectionID].Partitions = append(m.collectionInfos[collectionID].Partitions, &rootcoordpb.PartitionInfoOnPChannel{
		PartitionId: partitionID,
	})

	if _, loaded := m.managers.GetOrInsert(partitionID, newPartitionSegmentManager(
		m.pchannel,
		m.collectionInfos[collectionID].Vchannel,
		collectionID,
		partitionID,
		make([]*segmentAllocManager, 0),
	)); loaded {
		m.logger.Warn(
			"partition already exists when NewPartition in segment assignment service, it's may be a bug in system",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
	}
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
	for _, partition := range collectionInfo.Partitions {
		pm, ok := m.managers.Get(partition.PartitionId)
		if ok {
			needSealed = append(needSealed, pm.CollectAllCanBeSealedAndClear()...)
		}
		m.managers.Remove(partition.PartitionId)
	}
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
	partitions := make([]*rootcoordpb.PartitionInfoOnPChannel, 0, len(collectionInfo.Partitions)-1)
	for _, partition := range collectionInfo.Partitions {
		if partition.PartitionId != partitionID {
			partitions = append(partitions, partition)
		}
	}
	collectionInfo.Partitions = partitions

	pm, loaded := m.managers.GetAndRemove(partitionID)
	if !loaded {
		m.logger.Warn("partition not exists when RemovePartition in segment assignment service",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
		return nil
	}
	return pm.CollectAllCanBeSealedAndClear()
}

// SealAllSegmentsAndFenceUntil seals all segments and fence assign until timetick.
func (m *partitionSegmentManagers) SealAllSegmentsAndFenceUntil(collectionID int64, timetick uint64) ([]*segmentAllocManager, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	collectionInfo, ok := m.collectionInfos[collectionID]
	if !ok {
		m.logger.Warn("collection not exists when Flush in segment assignment service", zap.Int64("collectionID", collectionID))
		return nil, errors.New("collection not found")
	}

	sealedSegments := make([]*segmentAllocManager, 0)
	// collect all partitions
	for _, partition := range collectionInfo.Partitions {
		// Seal all segments and fence assign to the partition manager.
		pm, ok := m.managers.Get(partition.PartitionId)
		if !ok {
			m.logger.Warn("partition not found when Flush in segment assignment service, it's may be a bug in system",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partition.PartitionId))
			return nil, errors.New("partition not found")
		}
		newSealedSegments := pm.SealAllSegmentsAndFenceUntil(timetick)
		sealedSegments = append(sealedSegments, newSealedSegments...)
	}
	return sealedSegments, nil
}

// Range ranges the partition managers.
func (m *partitionSegmentManagers) Range(f func(pm *partitionSegmentManager)) {
	m.managers.Range(func(_ int64, pm *partitionSegmentManager) bool {
		f(pm)
		return true
	})
}

// newCollectionInfo creates a new collection info.
func newCollectionInfo(collectionID int64, vchannel string, partitionIDs []int64) *rootcoordpb.CollectionInfoOnPChannel {
	info := &rootcoordpb.CollectionInfoOnPChannel{
		CollectionId: collectionID,
		Vchannel:     vchannel,
		Partitions:   make([]*rootcoordpb.PartitionInfoOnPChannel, 0, len(partitionIDs)),
	}
	for _, partitionID := range partitionIDs {
		info.Partitions = append(info.Partitions, &rootcoordpb.PartitionInfoOnPChannel{
			PartitionId: partitionID,
		})
	}
	return info
}
