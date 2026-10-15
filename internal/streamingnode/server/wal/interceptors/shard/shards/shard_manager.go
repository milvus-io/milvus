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
	ErrVChannelFenced                  = errors.New("vchannel is fenced by shard split")
	// ErrVChannelConflict is returned when a vchannel cannot be registered
	// because another vchannel of the same collection already holds this
	// pchannel's entry. See CheckIfVChannelCanBeCreated.
	ErrVChannelConflict = errors.New("another vchannel of the collection is registered on this pchannel")

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
	fenced := newFencedVChannels(param.InitialRecoverSnapshot)
	// recover the segment assignment infos
	partitionToSegmentManagers, segmentBelongs := newSegmentAllocManagersFromRecovery(param.ChannelInfo, param.InitialRecoverSnapshot, collections, fenced)

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
		fencedVChannels:   fenced,
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
func newSegmentAllocManagersFromRecovery(
	pchannel types.PChannelInfo,
	recoverInfos *recovery.RecoverySnapshot,
	collections map[int64]*CollectionInfo,
	fenced map[string]SplitFence,
) (
	map[PartitionUniqueKey]map[int64]*segmentAllocManager,
	map[int64]stats.SegmentBelongs,
) {
	if recoverInfos == nil || recoverInfos.WritePathRecovery == nil {
		// An absent write-path snapshot means there is no write-path state to
		// restore; degrade to an empty allocation set instead of panicking on
		// a nil dereference at the recovery boundary.
		return make(map[PartitionUniqueKey]map[int64]*segmentAllocManager), make(map[int64]stats.SegmentBelongs)
	}
	return newSegmentAllocManagersFromWritePathRecovery(pchannel, recoverInfos.WritePathRecovery.GrowingSegments, collections, fenced)
}

func newSegmentAllocManagersFromWritePathRecovery(
	pchannel types.PChannelInfo,
	segments map[int64]moduleapi.SegmentWritePathRecoveryState,
	collections map[int64]*CollectionInfo,
	fenced map[string]SplitFence,
) (map[PartitionUniqueKey]map[int64]*segmentAllocManager, map[int64]stats.SegmentBelongs) {
	partitionToSegmentManagers := make(map[PartitionUniqueKey]map[int64]*segmentAllocManager)
	growingBelongs := make(map[int64]stats.SegmentBelongs, len(segments))
	for segmentID, state := range segments {
		if _, isFenced := fenced[state.VChannel]; isFenced {
			// A fenced vchannel keeps no registration to attach a segment to.
			// The fence seals every growing segment of the source in the same
			// message, so one surviving here means the meta was persisted in
			// parts; the alternative -- attaching it to whatever entry sits
			// under its collection id -- would hand a successor vchannel a
			// segment that is not its own.
			mlog.Warn(context.TODO(), "growing segment of a fenced vchannel is skipped on recovery",
				mlog.FieldCollectionID(state.CollectionID),
				mlog.FieldVChannel(state.VChannel),
				mlog.Int64("segmentID", segmentID))
			continue
		}
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

// newFencedVChannels recovers the shard split fence tombstones from the
// recovery snapshot.
//
// A fenced source stays VCHANNEL_STATE_NORMAL -- it keeps observing DDL and
// draining its own data until adoption drops it -- so the write-path snapshot
// still reports it, carrying SplitFenceTimeTick. That field is the whole
// tombstone: it is remembered by NAME whether or not the vchannel also keeps
// this pchannel's single collection registration, because the one that loses a
// collision to a live successor is precisely the one a stale proxy route still
// points at.
func newFencedVChannels(recoverInfos *recovery.RecoverySnapshot) map[string]SplitFence {
	fenced := make(map[string]SplitFence)
	if recoverInfos == nil || recoverInfos.WritePathRecovery == nil {
		return fenced
	}
	for vchannel, state := range recoverInfos.WritePathRecovery.VChannels {
		if state.SplitFenceTimeTick == 0 {
			continue
		}
		fenced[vchannel] = SplitFence{
			TimeTick: state.SplitFenceTimeTick,
			TaskID:   state.SplitFenceTaskID,
		}
	}
	return fenced
}

// newCollectionInfos creates a new collection info map from the recovery snapshot.
//
// A fenced split source is skipped: the fence tears its registration down as it
// is placed (see SplitShard), and a restart has to land on the same state.
// Rebuilding one would take back a pchannel slot a live successor may already
// hold -- m.collections keeps a single entry per collection -- and hand that
// successor's writes to a shard that is fenced. newFencedVChannels seeds the
// tombstone for it, which is all that a fenced vchannel is remembered by.
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
		if state.SplitFenceTimeTick != 0 {
			continue
		}
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
			collectionInfo.setSchema(&streamingpb.CollectionSchemaOfVChannel{Schema: state.Schema})
		}
		if incumbent, ok := collectionInfoMap[state.CollectionID]; ok {
			// Two LIVE vchannels of one collection on one pchannel, which the
			// map cannot hold: it keeps a single entry per collection. A
			// split's own source no longer collides here (it is skipped above),
			// so this is a placement the coordinator should never have made.
			// Keep the entry deterministically -- by name, so every replay of
			// this snapshot answers the same -- and report it.
			mlog.Error(context.TODO(), "two live vchannels of one collection recovered on one pchannel",
				mlog.FieldCollectionID(state.CollectionID),
				mlog.String("registered", incumbent.VChannel),
				mlog.String("conflicting", collectionInfo.VChannel))
			if incumbent.VChannel <= collectionInfo.VChannel {
				continue
			}
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
	// fencedVChannels remembers, by name, every vchannel this pchannel has
	// fenced by shard split, together with its T_switch. It REPLACES the entry
	// in collections: the fence removes the registration as it is placed, so
	// this is the only record a fenced vchannel leaves, and it is what a write
	// still routing to it is answered from -- SHARD_FENCED, meant to make the
	// proxy refresh (not implemented on this branch); an unrecoverable error
	// instead fails a write that one refresh would have completed.
	//
	// Best effort across a restart: a vchannel already dropped before the
	// restart leaves nothing in the snapshot to seed from, and a write to it
	// falls back to the unknown-route answer.
	fencedVChannels map[string]SplitFence
	metrics         *metricsutil.SegmentAssignMetrics
	txnManager      TxnManager
}

// CollectionInfo is one collection's LIVE registration on this pchannel: the
// vchannel that holds the slot, its partitions and its current schema. A vchannel
// fenced by a shard split has no CollectionInfo -- the fence removes it and
// leaves a SplitFence tombstone in its place.
type CollectionInfo struct {
	VChannel     string
	PartitionIDs map[int64]struct{}
	Schema       *streamingpb.CollectionSchemaOfVChannel
	primaryKey   *PrimaryKeyDescriptor
}

// SplitFence is what a fenced vchannel is remembered by after its registration
// is gone: when it was fenced, and by which task.
type SplitFence struct {
	TimeTick uint64
	TaskID   int64
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
