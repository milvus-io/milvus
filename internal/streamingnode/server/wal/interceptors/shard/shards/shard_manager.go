package shards

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	ErrTooLargeInsert    = stats.ErrTooLargeInsert
)

// ShardManagerRecoverParam is the parameter for recovering the segment assignment manager.
type ShardManagerRecoverParam struct {
	ChannelInfo        types.PChannelInfo
	WAL                *syncutil.Future[wal.WAL]
	Scheduler          nodescheduler.Scheduler
	WritePathRecovery  *moduleapi.WritePathRecoveryModuleSnapshot
	CheckpointTimeTick uint64
	// InitialRecoverSnapshot is kept for compatibility with existing callers.
	// Production recovery passes WritePathRecovery instead.
	InitialRecoverSnapshot *recovery.RecoverySnapshot
	TxnManager             TxnManager
}

// RecoverShardManager recovers the segment assignment manager from the recovery snapshot.
func RecoverShardManager(param *ShardManagerRecoverParam) ShardManager {
	writePathRecovery := param.WritePathRecovery
	checkpointTimeTick := param.CheckpointTimeTick
	if writePathRecovery == nil {
		writePathRecovery = writePathRecoveryFromLegacySnapshot(param.InitialRecoverSnapshot)
		if param.InitialRecoverSnapshot != nil && param.InitialRecoverSnapshot.Checkpoint != nil {
			checkpointTimeTick = param.InitialRecoverSnapshot.Checkpoint.TimeTick
		}
	}
	// recover the collection infos
	collections := newCollectionInfos(writePathRecovery)
	// recover the segment assignment infos
	partitionToSegmentManagers, segmentBelongs := newSegmentAllocManagersFromRecovery(param.ChannelInfo, writePathRecovery, collections)

	ctx, cancel := context.WithCancel(context.Background())
	logger := resource.Resource().Logger().With(mlog.FieldComponent("shard-manager")).With(mlog.Stringer("pchannel", param.ChannelInfo))
	segmentTotal := 0
	metrics := metricsutil.NewSegmentAssignMetrics(param.ChannelInfo.Name)
	for collectionID, collectionInfo := range collections {
		collectionInfo.FencedAssignTimeTick = checkpointTimeTick
		for partitionID := range collectionInfo.Partitions {
			var segmentManagers map[int64]*segmentAllocManager
			// recovery meta is recovered , use it.
			uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
			if recovered, ok := partitionToSegmentManagers[uniqueKey]; ok {
				segmentManagers = recovered
			}
			if partitionID == common.AllPartitionsID && len(segmentManagers) == 0 {
				continue
			}
			collectionInfo.Partitions[partitionID] = newPartitionSegmentManager(
				ctx,
				logger,
				param.WAL,
				param.Scheduler,
				param.ChannelInfo,
				collectionInfo.VChannel,
				collectionID,
				partitionID,
				segmentManagers,
				param.TxnManager,
				checkpointTimeTick, // use the checkpoint time tick to fence directly.
				metrics,
			)
			segmentTotal += len(segmentManagers)
		}
	}
	m := &shardManagerImpl{
		mu:          sync.RWMutex{},
		ctx:         ctx,
		cancel:      cancel,
		wal:         param.WAL,
		scheduler:   param.Scheduler,
		pchannel:    param.ChannelInfo,
		collections: collections,
		txnManager:  param.TxnManager,
		metrics:     metrics,
	}
	m.SetLogger(logger)
	m.updateMetrics()
	m.metrics.UpdateSegmentCount(segmentTotal)
	belongs := lo.Values(segmentBelongs)
	stats := make([]*stats.SegmentStats, 0, len(belongs))
	for _, belong := range belongs {
		stat := m.collections[belong.CollectionID].Partitions[belong.PartitionID].segments[belong.SegmentID].GetStatFromRecovery()
		if info := m.collections[belong.CollectionID]; info != nil {
			stat.RuntimeFlushSize = info.RuntimeFlushSize(stat.Modified)
		}
		stats = append(stats, stat)
	}
	resource.Resource().SegmentStatsManager().RegisterSealOperator(m, belongs, stats)
	return m
}

func writePathRecoveryFromLegacySnapshot(snapshot *recovery.RecoverySnapshot) *moduleapi.WritePathRecoveryModuleSnapshot {
	if snapshot == nil {
		return &moduleapi.WritePathRecoveryModuleSnapshot{}
	}
	if snapshot.WritePathRecovery != nil {
		return snapshot.WritePathRecovery
	}
	write := &moduleapi.WritePathRecoveryModuleSnapshot{
		VChannels:       make(map[string]moduleapi.VChannelWritePathRecoveryState),
		GrowingSegments: make(map[int64]moduleapi.SegmentWritePathRecoveryState),
	}
	for vchannel, meta := range snapshot.VChannels {
		if meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL || meta.GetCollectionInfo() == nil {
			continue
		}
		collection := meta.GetCollectionInfo()
		state := moduleapi.VChannelWritePathRecoveryState{
			VChannel:     vchannel,
			CollectionID: collection.GetCollectionId(),
			PartitionIDs: make([]int64, 0, len(collection.GetPartitions())),
		}
		for _, partition := range collection.GetPartitions() {
			state.PartitionIDs = append(state.PartitionIDs, partition.GetPartitionId())
		}
		if schemas := collection.GetSchemas(); len(schemas) > 0 {
			state.Schema = schemas[len(schemas)-1].GetSchema()
		}
		write.VChannels[vchannel] = state
	}
	for segmentID, meta := range snapshot.SegmentAssignments {
		if meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
			continue
		}
		write.GrowingSegments[segmentID] = moduleapi.SegmentWritePathRecoveryState{
			VChannel:     meta.GetVchannel(),
			CollectionID: meta.GetCollectionId(),
			PartitionID:  meta.GetPartitionId(),
			SegmentID:    meta.GetSegmentId(),
			Stat:         meta.GetStat(),
		}
	}
	return write
}

// newSegmentAllocManagersFromRecovery creates new segment alloc managers from the recovery snapshot.
func newSegmentAllocManagersFromRecovery(pchannel types.PChannelInfo, recoverInfos *moduleapi.WritePathRecoveryModuleSnapshot, collections map[int64]*CollectionInfo) (
	map[PartitionUniqueKey]map[int64]*segmentAllocManager,
	map[int64]stats.SegmentBelongs,
) {
	// recover the segment infos from the streaming node segment assignment meta storage
	partitionToSegmentManagers := make(map[PartitionUniqueKey]map[int64]*segmentAllocManager)
	growingBelongs := make(map[int64]stats.SegmentBelongs)
	seenSegments := make(map[int64]struct{}, len(recoverInfos.GrowingSegments))
	for _, state := range recoverInfos.GrowingSegments {
		coll, ok := collections[state.CollectionID]
		if !ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, collection not found, %d", state.CollectionID))
		}
		if _, ok := coll.Partitions[state.PartitionID]; !ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, partition not found, partition not found, %d", state.PartitionID))
		}
		if _, ok := seenSegments[state.SegmentID]; ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, segment repeated, %d", state.SegmentID))
		}
		seenSegments[state.SegmentID] = struct{}{}
		uniqueKey := PartitionUniqueKey{
			CollectionID: state.CollectionID,
			PartitionID:  state.PartitionID,
		}
		m := newSegmentAllocManagerFromRecovery(pchannel, state)
		growingBelongs[m.GetSegmentID()] = stats.SegmentBelongs{
			PChannel:     pchannel.Name,
			VChannel:     m.GetVChannel(),
			CollectionID: state.CollectionID,
			PartitionID:  state.PartitionID,
			SegmentID:    m.GetSegmentID(),
		}
		if _, ok := partitionToSegmentManagers[uniqueKey]; !ok {
			partitionToSegmentManagers[uniqueKey] = make(map[int64]*segmentAllocManager, 2)
		}
		partitionToSegmentManagers[uniqueKey][state.SegmentID] = m
	}
	return partitionToSegmentManagers, growingBelongs
}

// newCollectionInfos creates a new collection info map from the recovery snapshot.
func newCollectionInfos(recoverInfos *moduleapi.WritePathRecoveryModuleSnapshot) map[int64]*CollectionInfo {
	// collectionMap is a map from collectionID to collectionInfo.
	collectionInfoMap := make(map[int64]*CollectionInfo, len(recoverInfos.VChannels))
	for _, state := range recoverInfos.VChannels {
		partitions := make(map[int64]*partitionManager, len(state.PartitionIDs)+1)
		for _, partitionID := range state.PartitionIDs {
			partitions[partitionID] = nil
		}
		// add all partitions id into the collection info.
		partitions[common.AllPartitionsID] = nil
		var latestSchema *streamingpb.CollectionSchemaOfVChannel
		if state.Schema != nil {
			latestSchema = &streamingpb.CollectionSchemaOfVChannel{Schema: state.Schema}
		}
		collectionInfo := &CollectionInfo{
			VChannel:   state.VChannel,
			Partitions: partitions,
		}
		collectionInfo.setSchema(latestSchema)
		collectionInfoMap[state.CollectionID] = collectionInfo
	}
	return collectionInfoMap
}

// shardManagerImpl manages the all shard info of collection on current pchannel.
// It's a in-memory data structure, and will be recovered from recovery stroage of wal and wal itself.
// !!! Don't add any block operation (such as rpc or meta opration) in this module.
type shardManagerImpl struct {
	mlog.Binder

	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	wal         *syncutil.Future[wal.WAL]
	scheduler   nodescheduler.Scheduler
	pchannel    types.PChannelInfo
	collections map[int64]*CollectionInfo // map collectionID to collectionInfo
	metrics     *metricsutil.SegmentAssignMetrics
	txnManager  TxnManager
}

type CollectionInfo struct {
	VChannel             string
	Partitions           map[int64]*partitionManager
	Schema               *streamingpb.CollectionSchemaOfVChannel
	FencedAssignTimeTick uint64
	primaryKey           *PrimaryKeyDescriptor
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

func (m *shardManagerImpl) partitionManager(key PartitionUniqueKey) *partitionManager {
	collection := m.collections[key.CollectionID]
	if collection == nil {
		return nil
	}
	return collection.Partitions[key.PartitionID]
}

func (m *shardManagerImpl) ensurePartitionManager(key PartitionUniqueKey) *partitionManager {
	collection := m.collections[key.CollectionID]
	if collection == nil {
		return nil
	}
	manager, ok := collection.Partitions[key.PartitionID]
	if !ok {
		return nil
	}
	if manager == nil {
		manager = newPartitionSegmentManager(
			m.ctx,
			m.Logger(),
			m.wal,
			m.scheduler,
			m.pchannel,
			collection.VChannel,
			key.CollectionID,
			key.PartitionID,
			nil,
			m.txnManager,
			collection.FencedAssignTimeTick,
			m.metrics,
		)
		collection.Partitions[key.PartitionID] = manager
	}
	return manager
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

func (c *CollectionInfo) AllowGrowingSourceFlush() bool {
	if c == nil || c.Schema == nil {
		return false
	}
	return typeutil.AllowGrowingSourceFlush(c.Schema.GetSchema(),
		paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool(),
		paramtable.Get().CommonCfg.EnableGrowingSourceFlush.GetAsBool())
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
	if modified.Rows == 0 || modified.BinarySize == 0 {
		return modified.BinarySize
	}
	if !c.shouldEstimateInterimIndexExtra() {
		return modified.BinarySize
	}

	extra := estimateInterimIndexExtra(c.Schema.GetSchema(), modified.Rows)
	if extra == 0 {
		return modified.BinarySize
	}
	return utils.SaturatingAddUint64(modified.BinarySize, extra)
}

func (c *CollectionInfo) shouldEstimateInterimIndexExtra() bool {
	if c == nil || c.Schema == nil || c.Schema.GetSchema() == nil || !c.AllowGrowingSourceFlush() {
		return false
	}
	params := paramtable.Get()
	return params.QueryNodeCfg.EnableInterminSegmentIndex.GetAsBool() &&
		!params.QueryNodeCfg.GrowingMmapEnabled.GetAsBool()
}

func estimateInterimIndexExtra(schema *schemapb.CollectionSchema, rows uint64) uint64 {
	var extra uint64
	for _, field := range schema.GetFields() {
		switch field.GetDataType() {
		case schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			dim, err := typeutil.GetDim(field)
			if err != nil || dim <= 0 {
				continue
			}
			extra = utils.SaturatingAddUint64(extra, estimateDenseInterimIndexExtra(field.GetDataType(), uint64(dim), rows))
		case schemapb.DataType_SparseFloatVector:
			// Sparse interim indexes keep their own representation roughly at
			// raw sparse-vector size. Modified.BinarySize already accounts for
			// the raw insert payload, so add one more sparse estimate as index
			// overhead when chunks are retained for growing-source flush.
			extra = utils.SaturatingAddUint64(extra, utils.SaturatingMulUint64(rows, uint64(typeutil.GetSparseFloatVectorEstimateLength())))
		}
	}
	return extra
}

func estimateDenseInterimIndexExtra(dataType schemapb.DataType, dim uint64, rows uint64) uint64 {
	params := paramtable.Get()
	indexType := params.QueryNodeCfg.DenseVectorInterminIndexType.GetValue()
	switch {
	case strings.EqualFold(indexType, "IVF_FLAT_CC"):
		rawBytes := utils.SaturatingMulUint64(rows, denseVectorRawBytes(dataType, dim))
		expansionRate := params.QueryNodeCfg.InterimIndexMemExpandRate.GetAsFloat()
		if expansionRate <= 0 {
			expansionRate = 1
		}
		return ceilMulFloat(rawBytes, expansionRate)
	case strings.EqualFold(indexType, "SCANN_DVR"):
		return utils.SaturatingMulUint64(rows, scannDVRBytesPerRow(dim))
	default:
		return 0
	}
}

func denseVectorRawBytes(dataType schemapb.DataType, dim uint64) uint64 {
	switch dataType {
	case schemapb.DataType_FloatVector:
		return utils.SaturatingMulUint64(dim, 4)
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return utils.SaturatingMulUint64(dim, 2)
	default:
		return 0
	}
}

func scannDVRBytesPerRow(dim uint64) uint64 {
	params := paramtable.Get()
	subDim := uint64(params.QueryNodeCfg.InterimIndexSubDim.GetAsInt64())
	bytes := utils.SaturatingMulUint64(subDim/8, dim)
	switch strings.ToUpper(params.QueryNodeCfg.InterimIndexRefineQuantType.GetValue()) {
	case "UINT8":
		bytes = utils.SaturatingAddUint64(bytes, dim)
	case "FLOAT16", "BFLOAT16":
		bytes = utils.SaturatingAddUint64(bytes, utils.SaturatingMulUint64(dim, 2))
	}
	return bytes
}

func ceilMulFloat(value uint64, factor float64) uint64 {
	if value == 0 || factor <= 0 {
		return 0
	}
	result := math.Ceil(float64(value) * factor)
	if result >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(result)
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
	partitionCount := 0
	for _, collection := range m.collections {
		partitionCount += len(collection.Partitions) - 1
	}
	m.metrics.UpdatePartitionCount(partitionCount)
	m.metrics.UpdateCollectionCount(len(m.collections))
}

// newCollectionInfo creates a new collection info.
func newCollectionInfo(vchannel string, partitionIDs []int64) *CollectionInfo {
	info := &CollectionInfo{
		VChannel:   vchannel,
		Partitions: make(map[int64]*partitionManager, len(partitionIDs)+1),
		Schema:     nil, // Schema will be set when collection is created or altered
	}
	for _, partitionID := range partitionIDs {
		info.Partitions[partitionID] = nil
	}
	// add all partitions id into the collection info.
	info.Partitions[common.AllPartitionsID] = nil
	return info
}
