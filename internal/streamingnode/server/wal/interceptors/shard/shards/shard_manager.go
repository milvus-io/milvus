package shards

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var (
	ErrCollectionExists   = errors.New("collection exists")
	ErrCollectionNotFound = errors.New("collection not found")
	ErrPartitionExists    = errors.New("partition exists")
	ErrPartitionNotFound  = errors.New("partition not found")
	ErrSegmentExists      = errors.New("segment exists")
	ErrSegmentNotFound    = errors.New("segment not found")
	ErrSegmentOnGrowing   = errors.New("segment on growing")
	ErrFencedAssign       = errors.New("fenced assign")

	ErrTimeTickTooOld    = errors.New("time tick is too old")
	ErrWaitForNewSegment = errors.New("wait for new segment")
	ErrNotGrowing        = errors.New("segment is not growing")
	ErrNotEnoughSpace    = stats.ErrNotEnoughSpace
	ErrTooLargeInsert    = stats.ErrTooLargeInsert
)

// ShardManagerRecoverParam is the parameter for recovering the segment assignment manager.
type ShardManagerRecoverParam struct {
	ChannelInfo            types.PChannelInfo
	WAL                    *syncutil.Future[wal.WAL]
	InitialRecoverSnapshot *recovery.RecoverySnapshot
	TxnManager             TxnManager
}

// RecoverShardManager recovers the segment assignment manager from the recovery snapshot.
func RecoverShardManager(param *ShardManagerRecoverParam) *ShardManager {
	// recover the collection infos
	collections := newCollectionInfos(param.InitialRecoverSnapshot)
	// recover the segment assignment infos
	partitionToSegmentManagers, segmentBelongs := newSegmentAllocManagersFromRecovery(param.ChannelInfo, param.InitialRecoverSnapshot, collections)

	ctx, cancel := context.WithCancel(context.Background())
	logger := resource.Resource().Logger().With(log.FieldComponent("shard-manager")).With(zap.Stringer("pchannel", param.ChannelInfo))
	// create managers list.
	managers := make(map[int64]*partitionManager)
	segmentTotal := 0
	metrics := metricsutil.NewSegmentAssignMetrics(param.ChannelInfo.Name)
	for collectionID, collectionInfo := range collections {
		for partitionID := range collectionInfo.PartitionIDs {
			segmentManagers := make(map[int64]*segmentAllocManager, 0)
			// recovery meta is recovered , use it.
			if managers, ok := partitionToSegmentManagers[partitionID]; ok {
				segmentManagers = managers
			}
			if _, ok := managers[partitionID]; ok {
				panic("partition manager already exists when buildNewPartitionManagers in segment assignment service, there's a bug in system")
			}
			managers[partitionID] = newPartitionSegmentManager(
				ctx,
				logger,
				param.WAL,
				param.ChannelInfo,
				collectionInfo.VChannel,
				collectionID,
				partitionID,
				segmentManagers,
				param.TxnManager,
				param.InitialRecoverSnapshot.Checkpoint.TimeTick, // use the checkpoint time tick to fence directly.
				metrics,
			)
			segmentTotal += len(segmentManagers)
		}
	}
	m := &ShardManager{
		mu:                sync.Mutex{},
		ctx:               ctx,
		cancel:            cancel,
		wal:               param.WAL,
		pchannel:          param.ChannelInfo,
		partitionManagers: managers,
		collections:       collections,
		txnManager:        param.TxnManager,
		metrics:           metrics,
	}
	m.SetLogger(logger)
	m.updateMetrics()
	m.metrics.UpdateSegmentCount(segmentTotal)
	belongs := lo.Values(segmentBelongs)
	stats := make([]*stats.SegmentStats, 0, len(belongs))
	for _, belong := range belongs {
		stat := m.partitionManagers[belong.PartitionID].segments[belong.SegmentID].GetStatFromRecovery()
		stats = append(stats, stat)
	}
	resource.Resource().SegmentStatsManager().RegisterSealOperator(m, belongs, stats)
	return m
}

// newSegmentAllocManagersFromRecovery creates new segment alloc managers from the recovery snapshot.
func newSegmentAllocManagersFromRecovery(pchannel types.PChannelInfo, recoverInfos *recovery.RecoverySnapshot, collections map[int64]*CollectionInfo) (
	map[int64]map[int64]*segmentAllocManager,
	map[int64]stats.SegmentBelongs,
) {
	// recover the segment infos from the streaming node segment assignment meta storage
	partitionToSegmentManagers := make(map[int64]map[int64]*segmentAllocManager)
	growingBelongs := make(map[int64]stats.SegmentBelongs)
	for _, rawMeta := range recoverInfos.SegmentAssignments {
		m := newSegmentAllocManagerFromProto(pchannel, rawMeta)
		coll, ok := collections[rawMeta.GetCollectionId()]
		if !ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, collection not found, %d", rawMeta.GetCollectionId()))
		}
		if _, ok := coll.PartitionIDs[rawMeta.GetPartitionId()]; !ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, partition not found, partition not found, %d", rawMeta.GetPartitionId()))
		}
		if _, ok := growingBelongs[rawMeta.GetSegmentId()]; ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, segment repeated, %d", rawMeta.GetSegmentId()))
		}
		growingBelongs[m.GetSegmentID()] = stats.SegmentBelongs{
			PChannel:     pchannel.Name,
			VChannel:     m.GetVChannel(),
			CollectionID: rawMeta.GetCollectionId(),
			PartitionID:  rawMeta.GetPartitionId(),
			SegmentID:    m.GetSegmentID(),
		}
		if _, ok := partitionToSegmentManagers[rawMeta.GetPartitionId()]; !ok {
			partitionToSegmentManagers[rawMeta.GetPartitionId()] = make(map[int64]*segmentAllocManager, 2)
		}
		partitionToSegmentManagers[rawMeta.GetPartitionId()][rawMeta.GetSegmentId()] = m
	}
	return partitionToSegmentManagers, growingBelongs
}

// newCollectionInfos creates a new collection info map from the recovery snapshot.
func newCollectionInfos(recoverInfos *recovery.RecoverySnapshot) map[int64]*CollectionInfo {
	// collectionMap is a map from collectionID to collectionInfo.
	collectionInfoMap := make(map[int64]*CollectionInfo, len(recoverInfos.VChannels))
	for _, vchannelInfo := range recoverInfos.VChannels {
		currentPartition := make(map[int64]struct{}, len(vchannelInfo.CollectionInfo.Partitions))
		for _, partition := range vchannelInfo.CollectionInfo.Partitions {
			currentPartition[partition.PartitionId] = struct{}{}
		}
		collectionInfoMap[vchannelInfo.CollectionInfo.CollectionId] = &CollectionInfo{
			VChannel:     vchannelInfo.Vchannel,
			PartitionIDs: currentPartition,
		}
	}
	return collectionInfoMap
}

// ShardManager manages the all shard info of collection on current pchannel.
// It's a in-memory data structure, and will be recovered from recovery stroage of wal and wal itself.
// !!! Don't add any block operation (such as rpc or meta opration) in this module.
type ShardManager struct {
	log.Binder

	mu                sync.Mutex
	ctx               context.Context
	cancel            context.CancelFunc
	wal               *syncutil.Future[wal.WAL]
	pchannel          types.PChannelInfo
	partitionManagers map[int64]*partitionManager // map partitionID to partition manager
	collections       map[int64]*CollectionInfo   // map collectionID to collectionInfo
	metrics           *metricsutil.SegmentAssignMetrics
	txnManager        TxnManager
}

type CollectionInfo struct {
	VChannel     string
	PartitionIDs map[int64]struct{}
}

func (m *ShardManager) Channel() types.PChannelInfo {
	return m.pchannel
}

// Close try to persist all stats and invalid the manager.
func (m *ShardManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove the segment assignment manager from the global manager.
	resource.Resource().SegmentStatsManager().UnregisterSealOperator(m)
	m.cancel()
	m.metrics.Close()
}

func (m *ShardManager) updateMetrics() {
	m.metrics.UpdatePartitionCount(len(m.partitionManagers))
	m.metrics.UpdateCollectionCount(len(m.collections))
}

// newCollectionInfo creates a new collection info.
func newCollectionInfo(vchannel string, partitionIDs []int64) *CollectionInfo {
	info := &CollectionInfo{
		VChannel:     vchannel,
		PartitionIDs: make(map[int64]struct{}, len(partitionIDs)),
	}
	for _, partitionID := range partitionIDs {
		info.PartitionIDs[partitionID] = struct{}{}
	}
	return info
}
