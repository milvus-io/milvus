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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// latestCollectionSchemaVersion asks GetCollectionSchema to use the latest
// schema snapshot. Zero is a valid schema version and must stay explicit.
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
	// recover the segment infos from the streaming node segment assignment meta storage
	partitionToSegmentManagers := make(map[PartitionUniqueKey]map[int64]*segmentAllocManager)
	growingBelongs := make(map[int64]stats.SegmentBelongs)
	seenSegments := make(map[int64]struct{}, len(recoverInfos.SegmentAssignments))
	for _, rawMeta := range recoverInfos.SegmentAssignments {
		coll, ok := collections[rawMeta.GetCollectionId()]
		if !ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, collection not found, %d", rawMeta.GetCollectionId()))
		}
		if _, ok := coll.PartitionIDs[rawMeta.GetPartitionId()]; !ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, partition not found, partition not found, %d", rawMeta.GetPartitionId()))
		}
		if _, ok := seenSegments[rawMeta.GetSegmentId()]; ok {
			panic(fmt.Sprintf("segment assignment meta is dirty, segment repeated, %d", rawMeta.GetSegmentId()))
		}
		seenSegments[rawMeta.GetSegmentId()] = struct{}{}
		uniqueKey := PartitionUniqueKey{
			CollectionID: rawMeta.GetCollectionId(),
			PartitionID:  rawMeta.GetPartitionId(),
		}
		switch rawMeta.GetState() {
		case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING:
			m := newSegmentAllocManagerFromProto(pchannel, rawMeta)
			growingBelongs[m.GetSegmentID()] = stats.SegmentBelongs{
				PChannel:     pchannel.Name,
				VChannel:     m.GetVChannel(),
				CollectionID: rawMeta.GetCollectionId(),
				PartitionID:  rawMeta.GetPartitionId(),
				SegmentID:    m.GetSegmentID(),
			}
			if _, ok := partitionToSegmentManagers[uniqueKey]; !ok {
				partitionToSegmentManagers[uniqueKey] = make(map[int64]*segmentAllocManager, 2)
			}
			partitionToSegmentManagers[uniqueKey][rawMeta.GetSegmentId()] = m
		case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED:
			continue
		default:
			panic(fmt.Sprintf("segment assignment meta has unknown state, segment %d state %s", rawMeta.GetSegmentId(), rawMeta.GetState()))
		}
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
		// add all partitions id into the collection info.
		currentPartition[common.AllPartitionsID] = struct{}{}
		// Only keep the latest schema, as shard_interceptor only needs the current write view
		var latestSchema *streamingpb.CollectionSchemaOfVChannel
		if len(vchannelInfo.CollectionInfo.Schemas) > 0 {
			latestSchema = vchannelInfo.CollectionInfo.Schemas[len(vchannelInfo.CollectionInfo.Schemas)-1]
		}
		collectionInfoMap[vchannelInfo.CollectionInfo.CollectionId] = &CollectionInfo{
			VChannel:     vchannelInfo.Vchannel,
			PartitionIDs: currentPartition,
			Schema:       latestSchema,
		}
	}
	return collectionInfoMap
}

// shardManagerImpl manages the all shard info of collection on current pchannel.
// It's a in-memory data structure, and will be recovered from recovery stroage of wal and wal itself.
// !!! Don't add any block operation (such as rpc or meta opration) in this module.
type shardManagerImpl struct {
	mlog.Binder

	mu                sync.Mutex
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

// acceptsSchemaVersion reports whether a write stamped with schemaVersion may be
// accepted against the current schema. A write is accepted iff it is at or behind
// the current version — an exact match, or any earlier version (compiled against an
// older schema while a change is still propagating across vchannels). segcore
// reconciles the gap: it backfills nullable/default columns the write lacks and
// skips columns the write still carries for a since-dropped field. A write AHEAD of
// this vchannel (a not-yet-applied newer schema) is rejected, so the proxy retries
// once this vchannel catches up (invariant: proxy never runs ahead of a vchannel).
//
// Accepting a behind write is loss-free ONLY because the rootcoord DDL gate (I1)
// admits pure add / pure drop and rejects every in-place semantic change, so the
// node never needs to distinguish additive from destructive here.
func (c *CollectionInfo) acceptsSchemaVersion(schemaVersion int32) bool {
	return schemaVersion <= c.SchemaVersion()
}

func (c *CollectionInfo) UseGrowingSourceFlush() bool {
	if c == nil || c.Schema == nil {
		return false
	}
	return typeutil.UseGrowingSourceFlush(c.Schema.GetSchema(),
		paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool(),
		paramtable.Get().CommonCfg.EnableGrowingSourceFlush.GetAsBool())
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
	if c == nil || c.Schema == nil || c.Schema.GetSchema() == nil || !c.UseGrowingSourceFlush() {
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
