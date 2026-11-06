package shards

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// latestCollectionSchemaVersion asks schema accessors to use the latest
// snapshot. Zero is a valid schema version and must stay explicit.
const latestCollectionSchemaVersion int32 = -1

var (
	ErrCollectionExists                = errors.New("collection exists")
	ErrCollectionNotFound              = errors.New("collection not found")
	ErrCollectionSchemaNotFound        = errors.New("collection schema not found")
	ErrCollectionSchemaVersionNotMatch = errors.New("collection schema version not match")
	ErrPartitionExists                 = errors.New("partition exists")
	ErrPartitionNotFound               = errors.New("partition not found")
	ErrSegmentExists                   = errors.New("segment exists")
	ErrSegmentNotFound                 = errors.New("segment not found")
	ErrSegmentOnGrowing                = errors.New("segment on growing")
	ErrFencedAssign                    = errors.New("fenced assign")

	ErrTimeTickTooOld    = errors.New("time tick is too old")
	ErrWaitForNewSegment = errors.New("wait for new segment")
	ErrNotGrowing        = errors.New("segment is not growing")
	ErrNotEnoughSpace    = stats.ErrNotEnoughSpace
)

// ShardManagerRecoverParam is the parameter for recovering the segment assignment manager.
type ShardManagerRecoverParam struct {
	ChannelInfo            types.PChannelInfo
	WAL                    *syncutil.Future[wal.WAL]
	InitialRecoverSnapshot *recovery.RecoverySnapshot
	TxnManager             TxnManager
}

// RecoverShardManager recovers the segment assignment manager from the recovery snapshot.
func RecoverShardManager(param *ShardManagerRecoverParam) ShardManager {
	// recover the collection infos
	collections := newCollectionInfos(param.InitialRecoverSnapshot)
	// recover the segment assignment infos
	partitionToSegmentManagers, segmentBelongs := newSegmentAllocManagersFromRecovery(param.ChannelInfo, param.InitialRecoverSnapshot, collections)

	ctx, cancel := context.WithCancel(context.Background())
	logger := resource.Resource().Logger().With(mlog.FieldComponent("shard-manager")).With(mlog.Stringer("pchannel", param.ChannelInfo))
	// create managers list.
	managers := make(map[PartitionUniqueKey]*partitionManager)
	segmentTotal := 0
	metrics := metricsutil.NewSegmentAssignMetrics(param.ChannelInfo.Name)
	for collectionID, collectionInfo := range collections {
		for partitionID := range collectionInfo.PartitionIDs {
			segmentManagers := make(map[int64]*segmentAllocManager, 0)
			// recovery meta is recovered , use it.
			uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
			if managers, ok := partitionToSegmentManagers[uniqueKey]; ok {
				segmentManagers = managers
			}
			if _, ok := managers[uniqueKey]; ok {
				panic("partition manager already exists when buildNewPartitionManagers in segment assignment service, there's a bug in system")
			}
			managers[uniqueKey] = newPartitionSegmentManager(
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
	m := &shardManagerImpl{
		mu:                sync.RWMutex{},
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
		stat := m.partitionManagers[belong.PartitionUniqueKey()].segments[belong.SegmentID].GetStatFromRecovery()
		if info := m.collections[belong.CollectionID]; info != nil {
			stat.RuntimeFlushSize = info.RuntimeFlushSize(stat.Modified)
		}
		stats = append(stats, stat)
	}
	resource.Resource().SegmentStatsManager().RegisterSealOperator(m, belongs, stats)
	return m
}

// newSegmentAllocManagersFromRecovery creates new segment alloc managers from the recovery snapshot.
func newSegmentAllocManagersFromRecovery(pchannel types.PChannelInfo, recoverInfos *recovery.RecoverySnapshot, collections map[int64]*CollectionInfo) (
	map[PartitionUniqueKey]map[int64]*segmentAllocManager,
	map[int64]stats.SegmentBelongs,
) {
	if recoverInfos == nil || recoverInfos.WritePathRecovery == nil {
		// An absent write-path snapshot means there is no write-path state to
		// restore; degrade to an empty allocation set instead of panicking on
		// a nil dereference at the recovery boundary.
		return make(map[PartitionUniqueKey]map[int64]*segmentAllocManager), make(map[int64]stats.SegmentBelongs)
	}
	return newSegmentAllocManagersFromWritePathRecovery(pchannel, recoverInfos.WritePathRecovery.GrowingSegments, collections)
}

func newSegmentAllocManagersFromWritePathRecovery(
	pchannel types.PChannelInfo,
	segments map[int64]moduleapi.SegmentWritePathRecoveryState,
	collections map[int64]*CollectionInfo,
) (map[PartitionUniqueKey]map[int64]*segmentAllocManager, map[int64]stats.SegmentBelongs) {
	partitionToSegmentManagers := make(map[PartitionUniqueKey]map[int64]*segmentAllocManager)
	growingBelongs := make(map[int64]stats.SegmentBelongs, len(segments))
	for segmentID, state := range segments {
		if _, ok := collections[state.CollectionID]; !ok {
			panic(fmt.Sprintf("write path recovery state is dirty, collection not found, %d", state.CollectionID))
		}
		if _, ok := collections[state.CollectionID].PartitionIDs[state.PartitionID]; !ok {
			panic(fmt.Sprintf("write path recovery state is dirty, partition not found, %d", state.PartitionID))
		}
		uniqueKey := PartitionUniqueKey{CollectionID: state.CollectionID, PartitionID: state.PartitionID}
		if partitionToSegmentManagers[uniqueKey] == nil {
			partitionToSegmentManagers[uniqueKey] = make(map[int64]*segmentAllocManager, 2)
		}
		manager := newSegmentAllocManagerFromRecovery(pchannel, state)
		partitionToSegmentManagers[uniqueKey][segmentID] = manager
		growingBelongs[segmentID] = stats.SegmentBelongs{
			PChannel:     pchannel.Name,
			VChannel:     state.VChannel,
			CollectionID: state.CollectionID,
			PartitionID:  state.PartitionID,
			SegmentID:    segmentID,
		}
	}
	return partitionToSegmentManagers, growingBelongs
}

// newCollectionInfos creates a new collection info map from the recovery snapshot.
func newCollectionInfos(recoverInfos *recovery.RecoverySnapshot) map[int64]*CollectionInfo {
	if recoverInfos == nil || recoverInfos.WritePathRecovery == nil {
		return make(map[int64]*CollectionInfo)
	}
	return newCollectionInfosFromWritePathRecovery(recoverInfos.WritePathRecovery.VChannels)
}

func newCollectionInfosFromWritePathRecovery(
	vchannels map[string]moduleapi.VChannelWritePathRecoveryState,
) map[int64]*CollectionInfo {
	collectionInfoMap := make(map[int64]*CollectionInfo, len(vchannels))
	for vchannel, state := range vchannels {
		partitionIDs := make(map[int64]struct{}, len(state.PartitionIDs)+1)
		for _, partitionID := range state.PartitionIDs {
			partitionIDs[partitionID] = struct{}{}
		}
		partitionIDs[common.AllPartitionsID] = struct{}{}
		collectionInfo := &CollectionInfo{
			VChannel:     vchannel,
			PartitionIDs: partitionIDs,
		}
		if state.Schema != nil {
			collectionInfo.setSchema(&streamingpb.CollectionSchemaOfVChannel{Schema: state.Schema, CheckpointTimeTick: state.SchemaTimeTick})
		}
		collectionInfoMap[state.CollectionID] = collectionInfo
	}
	return collectionInfoMap
}

// shardManagerImpl manages the all shard info of collection on current pchannel.
// It's a in-memory data structure, and will be recovered from recovery stroage of wal and wal itself.
// !!! Don't add any block operation (such as rpc or meta opration) in this module.
type shardManagerImpl struct {
	mlog.Binder

	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
	wal               *syncutil.Future[wal.WAL]
	pchannel          types.PChannelInfo
	partitionManagers map[PartitionUniqueKey]*partitionManager // map partitionID to partition manager
	collections       map[int64]*CollectionInfo                // map collectionID to collectionInfo
	metrics           *metricsutil.SegmentAssignMetrics
	txnManager        TxnManager
}

type CollectionInfo struct {
	VChannel     string
	PartitionIDs map[int64]struct{}
	Schema       *streamingpb.CollectionSchemaOfVChannel
	primaryKey   *PrimaryKeyDescriptor
}

// PrimaryKeyDescriptor is the immutable PK information needed by WAL write
// tracking without exposing or cloning a collection schema.
type PrimaryKeyDescriptor struct {
	FieldID  int64
	DataType schemapb.DataType
}

func (c *CollectionInfo) setSchema(schema *streamingpb.CollectionSchemaOfVChannel) {
	c.Schema = schema
	c.primaryKey = nil
	if schema == nil || schema.GetSchema() == nil {
		return
	}
	descriptor, err := primaryKeyDescriptorFromSchema(schema.GetSchema())
	if err == nil {
		c.primaryKey = &descriptor
	}
}

func primaryKeyDescriptorFromSchema(schema *schemapb.CollectionSchema) (PrimaryKeyDescriptor, error) {
	primaryField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return PrimaryKeyDescriptor{}, err
	}
	return PrimaryKeyDescriptor{
		FieldID:  primaryField.GetFieldID(),
		DataType: primaryField.GetDataType(),
	}, nil
}

// SchemaVersion returns the current collection schema version for the write path.
// It returns 0 if schema is not set (nil receiver, nil Schema, or nil inner CollectionSchema).
func (c *CollectionInfo) SchemaVersion() int32 {
	if c == nil || c.Schema == nil {
		return 0
	}
	s := c.Schema.GetSchema()
	if s == nil {
		return 0
	}
	return s.GetVersion()
}

func (c *CollectionInfo) RequiresStorageV3() bool {
	return c.HasTextField()
}

func (c *CollectionInfo) HasTextField() bool {
	if c == nil || c.Schema == nil || c.Schema.GetSchema() == nil {
		return false
	}
	return typeutil.HasTextField(c.Schema.GetSchema())
}

// RuntimeFlushSize estimates the in-memory footprint used by flush pressure decisions.
func (c *CollectionInfo) RuntimeFlushSize(modified stats.ModifiedMetrics) uint64 {
	return modified.BinarySize
}

func (m *shardManagerImpl) Channel() types.PChannelInfo {
	return m.pchannel
}

// Close try to persist all stats and invalid the manager.
func (m *shardManagerImpl) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove the segment assignment manager from the global manager.
	resource.Resource().SegmentStatsManager().UnregisterSealOperator(m)
	m.cancel()
	m.metrics.Close()
}

func (m *shardManagerImpl) updateMetrics() {
	// the partition managers contains the all partitions id, so we need to subtract the collections count.
	m.metrics.UpdatePartitionCount(len(m.partitionManagers) - len(m.collections))
	m.metrics.UpdateCollectionCount(len(m.collections))
}

// newCollectionInfo creates a new collection info.
func newCollectionInfo(vchannel string, partitionIDs []int64) *CollectionInfo {
	info := &CollectionInfo{
		VChannel:     vchannel,
		PartitionIDs: make(map[int64]struct{}, len(partitionIDs)),
		Schema:       nil, // Schema will be set when collection is created or altered
	}
	for _, partitionID := range partitionIDs {
		info.PartitionIDs[partitionID] = struct{}{}
	}
	// add all partitions id into the collection info.
	info.PartitionIDs[common.AllPartitionsID] = struct{}{}
	return info
}
