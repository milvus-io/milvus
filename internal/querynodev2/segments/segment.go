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

package segments

/*
#cgo pkg-config: milvus_core

#include "futures/future_c.h"
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/segment_c.h"
#include "common/init_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SegmentType = commonpb.SegmentState

const (
	SegmentTypeGrowing = commonpb.SegmentState_Growing
	SegmentTypeSealed  = commonpb.SegmentState_Sealed
)

var ErrSegmentUnhealthy = errors.New("segment unhealthy")

// IndexedFieldInfo contains binlog info of vector field
type IndexedFieldInfo struct {
	FieldBinlog *datapb.FieldBinlog
	IndexInfo   *querypb.FieldIndexInfo
	IsLoaded    bool
}

type baseSegment struct {
	collection *Collection
	version    *atomic.Int64

	segmentType   SegmentType
	pkCandidate   pkoracle.Candidate // PK candidate: BloomFilterSet for regular collections, ExternalSegmentCandidate for external collections
	loadInfo      *atomic.Pointer[querypb.SegmentLoadInfo]
	skipGrowingBF bool // Skip generating or maintaining BF for growing segments; deletion checks will be handled in segcore.
	channel       metautil.Channel

	resourceUsageCache *atomic.Pointer[ResourceUsage]

	needUpdatedVersion *atomic.Int64 // only for lazy load mode update index
}

func newBaseSegment(collection *Collection, segmentType SegmentType, version int64, loadInfo *querypb.SegmentLoadInfo) (baseSegment, error) {
	channel, err := metautil.ParseChannel(loadInfo.GetInsertChannel(), channelMapper)
	if err != nil {
		return baseSegment{}, err
	}
	bs := baseSegment{
		collection:    collection,
		loadInfo:      atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo),
		version:       atomic.NewInt64(version),
		segmentType:   segmentType,
		pkCandidate:   pkoracle.NewBloomFilterSet(loadInfo.GetSegmentID(), loadInfo.GetPartitionID(), segmentType),
		channel:       channel,
		skipGrowingBF: segmentType == SegmentTypeGrowing && paramtable.Get().QueryNodeCfg.SkipGrowingSegmentBF.GetAsBool(),

		resourceUsageCache: atomic.NewPointer[ResourceUsage](nil),
		needUpdatedVersion: atomic.NewInt64(0),
	}
	return bs, nil
}

// ID returns the identity number.
func (s *baseSegment) ID() int64 {
	return s.loadInfo.Load().GetSegmentID()
}

func (s *baseSegment) Collection() int64 {
	return s.loadInfo.Load().GetCollectionID()
}

func (s *baseSegment) GetCollection() *Collection {
	return s.collection
}

func (s *baseSegment) Partition() int64 {
	return s.loadInfo.Load().GetPartitionID()
}

func (s *baseSegment) DatabaseName() string {
	return s.collection.GetDBName()
}

func (s *baseSegment) ResourceGroup() string {
	return s.collection.GetResourceGroup()
}

func (s *baseSegment) Shard() metautil.Channel {
	return s.channel
}

func (s *baseSegment) Type() SegmentType {
	return s.segmentType
}

func (s *baseSegment) Level() datapb.SegmentLevel {
	return s.loadInfo.Load().GetLevel()
}

func (s *baseSegment) IsSorted() bool {
	return s.loadInfo.Load().GetIsSorted()
}

func (s *baseSegment) StartPosition() *msgpb.MsgPosition {
	return s.loadInfo.Load().GetStartPosition()
}

func (s *baseSegment) Version() int64 {
	return s.version.Load()
}

func (s *baseSegment) CASVersion(old, newVersion int64) bool {
	return s.version.CompareAndSwap(old, newVersion)
}

func (s *baseSegment) LoadInfo() *querypb.SegmentLoadInfo {
	return s.loadInfo.Load()
}

func (s *baseSegment) SetPKCandidate(candidate pkoracle.Candidate) {
	s.pkCandidate = candidate
}

// PkCandidateExist implements pkoracle.Candidate — reports whether PK data has been loaded.
func (s *baseSegment) PkCandidateExist() bool {
	return s.pkCandidate != nil && s.pkCandidate.PkCandidateExist()
}

// UpdatePkCandidate feeds new primary keys into the PK candidate.
func (s *baseSegment) UpdatePkCandidate(pks []storage.PrimaryKey) {
	if s.skipGrowingBF {
		return
	}
	if s.pkCandidate != nil {
		s.pkCandidate.UpdatePkCandidate(pks)
	}
}

// Stats implements pkoracle.Candidate — returns PK statistics (min/max PK).
func (s *baseSegment) Stats() *storage.PkStatistics {
	if s.pkCandidate != nil {
		return s.pkCandidate.Stats()
	}
	return nil
}

// Charge implements pkoracle.Candidate — charges memory resources.
func (s *baseSegment) Charge() {
	if s.pkCandidate != nil {
		s.pkCandidate.Charge()
	}
}

// Refund implements pkoracle.Candidate — releases memory resources.
func (s *baseSegment) Refund() {
	if s.pkCandidate != nil {
		s.pkCandidate.Refund()
	}
}

type bm25StatsHolder struct {
	mu    sync.RWMutex
	stats map[int64]*storage.BM25Stats
}

func newBM25StatsHolder() *bm25StatsHolder {
	return &bm25StatsHolder{
		stats: make(map[int64]*storage.BM25Stats),
	}
}

func (h *bm25StatsHolder) UpdateBM25Stats(stats map[int64]*storage.BM25Stats) {
	if h == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for fieldID, new := range stats {
		if new == nil {
			continue
		}
		if current, ok := h.stats[fieldID]; ok {
			current.Merge(new)
		} else {
			h.stats[fieldID] = new.Clone()
		}
	}
}

func (h *bm25StatsHolder) GetBM25Stats() map[int64]*storage.BM25Stats {
	if h == nil {
		return map[int64]*storage.BM25Stats{}
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := cloneBM25StatsMap(h.stats)
	if stats == nil {
		return map[int64]*storage.BM25Stats{}
	}
	return stats
}

func cloneBM25StatsMap(stats map[int64]*storage.BM25Stats) map[int64]*storage.BM25Stats {
	if len(stats) == 0 {
		return nil
	}
	cloned := make(map[int64]*storage.BM25Stats, len(stats))
	for fieldID, stat := range stats {
		if stat != nil {
			cloned[fieldID] = stat.Clone()
		}
	}
	return cloned
}

// MayPkExist returns true if the given PK exists in the PK range and being positive through the bloom filter,
// false otherwise,
// may returns true even the PK doesn't exist actually
func (s *baseSegment) MayPkExist(pk *storage.LocationsCache) bool {
	if s.skipGrowingBF {
		return true
	}
	if s.pkCandidate == nil {
		return true // No candidate, assume PK might exist
	}
	return s.pkCandidate.MayPkExist(pk)
}

func (s *baseSegment) GetMinPk() *storage.PrimaryKey {
	if s.pkCandidate != nil {
		if stats := s.pkCandidate.Stats(); stats != nil {
			return &stats.MinPK
		}
	}
	return nil
}

func (s *baseSegment) GetMaxPk() *storage.PrimaryKey {
	if s.pkCandidate != nil {
		if stats := s.pkCandidate.Stats(); stats != nil {
			return &stats.MaxPK
		}
	}
	return nil
}

func (s *baseSegment) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	if s.skipGrowingBF {
		allPositive := make([]bool, lc.Size())
		for i := 0; i < lc.Size(); i++ {
			allPositive[i] = true
		}
		return allPositive
	}
	if s.pkCandidate == nil {
		allPositive := make([]bool, lc.Size())
		for i := 0; i < lc.Size(); i++ {
			allPositive[i] = true
		}
		return allPositive
	}
	return s.pkCandidate.BatchPkExist(lc)
}

// ResourceUsageEstimate returns the final estimated resource usage of the segment.
func (s *baseSegment) ResourceUsageEstimate() ResourceUsage {
	if s.segmentType == SegmentTypeGrowing {
		// Growing segment cannot do resource usage estimate.
		return ResourceUsage{}
	}
	cache := s.resourceUsageCache.Load()
	if cache != nil {
		return *cache
	}

	usage, err := estimateLogicalResourceUsageOfSegment(s.collection.Schema(), s.LoadInfo(), resourceEstimateFactor{
		deltaDataExpansionFactor:        paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
		TieredEvictionEnabled:           paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool(),
		TieredEvictableMemoryCacheRatio: paramtable.Get().QueryNodeCfg.TieredEvictableMemoryCacheRatio.GetAsFloat(),
		TieredEvictableDiskCacheRatio:   paramtable.Get().QueryNodeCfg.TieredEvictableDiskCacheRatio.GetAsFloat(),
	})
	if err != nil {
		// Should never failure, if failed, segment should never be loaded.
		mlog.Warn(context.TODO(), "unreachable: failed to get resource usage estimate of segment", mlog.Err(err), mlog.FieldCollectionID(s.Collection()), mlog.FieldSegmentID(s.ID()))
		return ResourceUsage{}
	}
	s.resourceUsageCache.Store(usage)
	return *usage
}

func (s *baseSegment) NeedUpdatedVersion() int64 {
	return s.needUpdatedVersion.Load()
}

func (s *baseSegment) SetLoadInfo(loadInfo *querypb.SegmentLoadInfo) {
	s.loadInfo.Store(loadInfo)
}

func (s *baseSegment) SetNeedUpdatedVersion(version int64) {
	s.needUpdatedVersion.Store(version)
}

type FieldInfo struct {
	*datapb.FieldBinlog
	RowCount int64
}

var _ Segment = (*LocalSegment)(nil)

// Segment is a wrapper of the underlying C-structure segment.
type LocalSegment struct {
	baseSegment
	*bm25StatsHolder

	manager SegmentManager
	ptrLock *state.LoadStateLock
	ptr     C.CSegmentInterface // TODO: Remove in future, after move load index into segcore package.
	// always keep same with csegment.RawPtr(), for eaiser to access,
	csegment segcore.CSegment

	// cached results, to avoid too many CGO calls
	memSize     *atomic.Int64
	binlogSize  *atomic.Int64
	rowNum      *atomic.Int64
	insertCount *atomic.Int64

	deltaMut           sync.Mutex
	lastDeltaTimestamp *atomic.Uint64
	fields             *typeutil.ConcurrentMap[int64, *FieldInfo]
	fieldIndexes       *typeutil.ConcurrentMap[int64, *IndexedFieldInfo] // indexID -> IndexedFieldInfo
	fieldJSONStats     map[int64]*querypb.JsonStatsInfo
	fieldJSONStatsMu   sync.RWMutex
}

func NewSegment(ctx context.Context,
	collection *Collection,
	manager SegmentManager,
	segmentType SegmentType,
	version int64,
	loadInfo *querypb.SegmentLoadInfo,
) (Segment, error) {
	/*
		CStatus
		NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type, CSegmentInterface* newSegment);
	*/
	if loadInfo.GetLevel() == datapb.SegmentLevel_L0 {
		return NewL0Segment(collection, segmentType, version, loadInfo)
	}

	base, err := newBaseSegment(collection, segmentType, version, loadInfo)
	if err != nil {
		return nil, err
	}

	var locker *state.LoadStateLock
	switch segmentType {
	case SegmentTypeSealed:
		locker = state.NewLoadStateLock(state.LoadStateOnlyMeta)
	case SegmentTypeGrowing:
		locker = state.NewLoadStateLock(state.LoadStateDataLoaded)
	default:
		return nil, merr.WrapErrServiceInternalMsg("illegal segment type %d when create segment %d", segmentType, loadInfo.GetSegmentID())
	}

	logger := mlog.With(
		mlog.FieldCollectionID(loadInfo.GetCollectionID()),
		mlog.FieldPartitionID(loadInfo.GetPartitionID()),
		mlog.FieldSegmentID(loadInfo.GetSegmentID()),
		mlog.String("segmentType", segmentType.String()),
		mlog.String("level", loadInfo.GetLevel().String()),
	)

	var csegment segcore.CSegment
	if _, err := GetDynamicPool().Submit(func() (any, error) {
		var err error
		csegment, err = segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
			Collection:  collection.ccollection,
			SegmentID:   loadInfo.GetSegmentID(),
			SegmentType: segmentType,
			IsSorted:    loadInfo.GetIsSorted(),
			LoadInfo:    loadInfo,
		})
		return nil, err
	}).Await(); err != nil {
		logger.Warn(ctx, "create segment failed", mlog.Err(err))
		return nil, err
	}
	logger.Info(ctx, "create segment done")

	segment := &LocalSegment{
		baseSegment:        base,
		bm25StatsHolder:    newBM25StatsHolder(),
		manager:            manager,
		ptrLock:            locker,
		ptr:                C.CSegmentInterface(csegment.RawPointer()),
		csegment:           csegment,
		lastDeltaTimestamp: atomic.NewUint64(0),
		fields:             typeutil.NewConcurrentMap[int64, *FieldInfo](),
		fieldIndexes:       typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),
		fieldJSONStats:     make(map[int64]*querypb.JsonStatsInfo),

		memSize:     atomic.NewInt64(-1),
		binlogSize:  atomic.NewInt64(0),
		rowNum:      atomic.NewInt64(-1),
		insertCount: atomic.NewInt64(0),
	}

	if err := segment.initializeSegment(); err != nil {
		csegment.Release()
		return nil, err
	}
	return segment, nil
}

func (s *LocalSegment) initializeSegment() error {
	loadInfo := s.loadInfo.Load()
	indexedFieldInfos, fieldBinlogs := separateIndexAndBinlog(loadInfo)
	schemaHelper, _ := typeutil.CreateSchemaHelper(s.collection.Schema())

	for _, info := range indexedFieldInfos {
		fieldID := info.IndexInfo.FieldID
		field, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			return err
		}
		indexInfo := info.IndexInfo
		s.fieldIndexes.Insert(indexInfo.GetIndexID(), &IndexedFieldInfo{
			FieldBinlog: &datapb.FieldBinlog{
				FieldID: indexInfo.GetFieldID(),
			},
			IndexInfo: indexInfo,
			IsLoaded:  false,
		})
		if !typeutil.IsVectorType(field.GetDataType()) && !s.HasRawData(fieldID) {
			s.fields.Insert(fieldID, &FieldInfo{
				FieldBinlog: info.FieldBinlog,
				RowCount:    loadInfo.GetNumOfRows(),
			})
		}
	}

	for _, binlogs := range fieldBinlogs {
		s.fields.Insert(binlogs.FieldID, &FieldInfo{
			FieldBinlog: binlogs,
			RowCount:    loadInfo.GetNumOfRows(),
		})
	}

	// Update the insert count when initialize the segment and update the metrics.
	s.insertCount.Store(loadInfo.GetNumOfRows())
	return nil
}

// PinIfNotReleased acquires the `ptrLock` and returns true if the pointer is valid
// Provide ONLY the read lock operations,
// don't make `ptrLock` public to avoid abusing of the mutex.
func (s *LocalSegment) PinIfNotReleased() error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	return nil
}

func (s *LocalSegment) Unpin() {
	s.ptrLock.Unpin()
}

func (s *LocalSegment) InsertCount() int64 {
	return s.insertCount.Load()
}

func (s *LocalSegment) RowNum() int64 {
	// if segment is not loaded, return 0 (maybe not loaded or release by lru)
	if !s.ptrLock.PinIf(state.IsDataLoaded) {
		return 0
	}
	defer s.ptrLock.Unpin()

	rowNum := s.rowNum.Load()
	if rowNum < 0 {
		GetDynamicPool().Submit(func() (any, error) {
			rowNum = s.csegment.RowNum()
			s.rowNum.Store(rowNum)
			return nil, nil
		}).Await()
	}
	return rowNum
}

func (s *LocalSegment) MemSize() int64 {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return 0
	}
	defer s.ptrLock.Unpin()

	memSize := s.memSize.Load()
	if memSize < 0 {
		GetDynamicPool().Submit(func() (any, error) {
			memSize = s.csegment.MemSize()
			s.memSize.Store(memSize)
			return nil, nil
		}).Await()
	}
	return memSize
}

func (s *LocalSegment) LastDeltaTimestamp() uint64 {
	return s.lastDeltaTimestamp.Load()
}

// advanceLastDeltaTimestamp moves lastDeltaTimestamp forward to max(current, max(tss)).
// Consumers (file-level skip in segment_loader.LoadDeltaLogs, dist_handler reporting to
// QueryCoord) treat this field as a high-water-mark. Using tss[last] on unsorted batches
// underestimates the watermark, so we scan for the true max.
func (s *LocalSegment) advanceLastDeltaTimestamp(tss []typeutil.Timestamp) {
	if len(tss) == 0 {
		return
	}
	maxTs := tss[0]
	for _, t := range tss[1:] {
		if t > maxTs {
			maxTs = t
		}
	}
	for {
		cur := s.lastDeltaTimestamp.Load()
		if maxTs <= cur {
			return
		}
		if s.lastDeltaTimestamp.CompareAndSwap(cur, maxTs) {
			return
		}
	}
}

// UpdatePkCandidate updates the PK candidate with provided pks and charges resource.
// Overrides baseSegment.UpdatePkCandidate to handle resource charging for growing segments.
func (s *LocalSegment) UpdatePkCandidate(pks []storage.PrimaryKey) {
	if s.skipGrowingBF {
		return
	}

	s.pkCandidate.UpdatePkCandidate(pks)

	// Charge resource (safe to call multiple times - only charges once)
	s.pkCandidate.Charge()
}

func (s *LocalSegment) GetIndexByID(indexID int64) *IndexedFieldInfo {
	info, _ := s.fieldIndexes.Get(indexID)
	return info
}

func (s *LocalSegment) GetIndex(fieldID int64) []*IndexedFieldInfo {
	var info []*IndexedFieldInfo
	s.fieldIndexes.Range(func(key int64, value *IndexedFieldInfo) bool {
		if value.IndexInfo.FieldID == fieldID {
			info = append(info, value)
		}
		return true
	})
	return info
}

func (s *LocalSegment) ExistIndex(fieldID int64) bool {
	contain := false
	s.fieldIndexes.Range(func(key int64, value *IndexedFieldInfo) bool {
		if value.IndexInfo.FieldID == fieldID {
			contain = true
		}
		return !contain
	})

	return contain
}

func (s *LocalSegment) HasRawData(fieldID int64) bool {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return false
	}
	defer s.ptrLock.Unpin()

	return s.csegment.HasRawData(fieldID)
}

func (s *LocalSegment) HasFieldData(fieldID int64) bool {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return false
	}
	defer s.ptrLock.Unpin()
	return s.csegment.HasFieldData(fieldID)
}

func (s *LocalSegment) DropIndex(ctx context.Context, indexID int64) error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	if indexInfo, ok := s.fieldIndexes.Get(indexID); ok {
		field := typeutil.GetField(s.collection.Schema(), indexInfo.IndexInfo.FieldID)
		if typeutil.IsJSONType(field.GetDataType()) {
			nestedPath, err := funcutil.GetAttrByKeyFromRepeatedKV(common.JSONPathKey, indexInfo.IndexInfo.GetIndexParams())
			if err != nil {
				return err
			}
			err = s.csegment.DropJSONIndex(ctx, indexInfo.IndexInfo.FieldID, nestedPath)
			if err != nil {
				return err
			}
		} else {
			err := s.csegment.DropIndex(ctx, indexInfo.IndexInfo.FieldID)
			if err != nil {
				return err
			}
		}

		s.fieldIndexes.Remove(indexID)
	}
	return nil
}

func (s *LocalSegment) Indexes() []*IndexedFieldInfo {
	var result []*IndexedFieldInfo
	s.fieldIndexes.Range(func(key int64, value *IndexedFieldInfo) bool {
		result = append(result, value)
		return true
	})
	return result
}

func (s *LocalSegment) IsLazyLoad() bool {
	for _, indexInfo := range s.Indexes() {
		if !indexInfo.IsLoaded {
			return true
		}
	}
	return false
}

func (s *LocalSegment) ResetIndexesLazyLoad(lazyState bool) {
	for _, indexInfo := range s.Indexes() {
		indexInfo.IsLoaded = lazyState
	}
}

func (s *LocalSegment) syncFieldIndexes(indexInfos []*querypb.FieldIndexInfo) {
	isLoadedByIndexID := make(map[int64]bool)
	loadedForNewIndex := !s.IsLazyLoad()
	for _, index := range s.Indexes() {
		isLoadedByIndexID[index.IndexInfo.GetIndexID()] = index.IsLoaded
	}

	indexIDs := typeutil.NewSet[int64]()
	for _, indexInfo := range indexInfos {
		indexID := indexInfo.GetIndexID()
		indexIDs.Insert(indexID)
		isLoaded, ok := isLoadedByIndexID[indexID]
		if !ok {
			isLoaded = loadedForNewIndex
		}
		s.fieldIndexes.Insert(indexID, &IndexedFieldInfo{
			FieldBinlog: &datapb.FieldBinlog{
				FieldID: indexInfo.GetFieldID(),
			},
			IndexInfo: indexInfo,
			IsLoaded:  isLoaded,
		})
	}

	// QueryCoord builds reopen LoadInfo from DataCoord's full finished-index list for this segment.
	// Treat absent index IDs as stale local metadata.
	s.fieldIndexes.Range(func(indexID int64, _ *IndexedFieldInfo) bool {
		if !indexIDs.Contain(indexID) {
			s.fieldIndexes.Remove(indexID)
		}
		return true
	})
}

// Search executes a search on the segment.
// If searchReq.FilterOnly() is true, only executes the filter and returns valid_count (Stage 1 of two-stage search).
func (s *LocalSegment) Search(ctx context.Context, searchReq *segcore.SearchRequest) (*segcore.SearchResult, error) {
	filterOnly := searchReq.FilterOnly()
	log := mlog.WithLazy(
		mlog.Uint64("mvcc", searchReq.MVCC()),
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldSegmentID(s.ID()),
		mlog.String("segmentType", s.segmentType.String()),
		mlog.Bool("filterOnly", filterOnly),
	)

	if !s.ptrLock.PinIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	hasIndex := s.ExistIndex(searchReq.SearchFieldID())
	log = log.With(mlog.Bool("withIndex", hasIndex))
	log.Debug(ctx, "search segment...")

	tr := timerecord.NewTimeRecorder("cgoSearch")
	result, err := s.csegment.Search(ctx, searchReq)
	if err != nil {
		log.Warn(ctx, "Search failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(paramtable.GetStringNodeID(), metrics.SearchLabel).Observe(float64(tr.ElapseSpan().Microseconds()) / 1000.0)
	if filterOnly {
		log.Debug(ctx, "search filter only segment done", mlog.Int64("validCount", result.ValidCount()))
	} else {
		log.Debug(ctx, "search segment done")
	}
	return result, nil
}

func (s *LocalSegment) retrieve(ctx context.Context, plan *segcore.RetrievePlan, log *mlog.Logger) (*segcore.RetrieveResult, error) {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log.Debug(ctx, "begin to retrieve")

	tr := timerecord.NewTimeRecorder("cgoRetrieve")
	result, err := s.csegment.Retrieve(ctx, plan)
	if err != nil {
		log.Warn(ctx, "Retrieve failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(paramtable.GetStringNodeID(),
		contextutil.GetQueryLabel(ctx)).Observe(float64(tr.ElapseSpan().Microseconds()) / 1000.0)
	return result, nil
}

func (s *LocalSegment) Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	log := mlog.WithLazy(
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.Uint64("mvcc", plan.Timestamp),
		mlog.String("segmentType", s.segmentType.String()),
	)

	result, err := s.retrieve(ctx, plan, log)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "partial-segcore-results-deserialization")
	defer span.End()

	retrieveResult, err := result.GetResult()
	if err != nil {
		log.Warn(ctx, "unmarshal retrieve result failed", mlog.Err(err))
		return nil, err
	}
	log.Debug(ctx, "retrieve segment done", mlog.Int("resultNum", len(retrieveResult.Offset)))
	return retrieveResult, nil
}

func (s *LocalSegment) retrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets, log *mlog.Logger) (*segcore.RetrieveResult, error) {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		// TODO: check if the segment is readable but not released. too many related logic need to be refactor.
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log.Debug(ctx, "begin to retrieve by offsets")
	tr := timerecord.NewTimeRecorder("cgoRetrieveByOffsets")
	result, err := s.csegment.RetrieveByOffsets(ctx, plan)
	if err != nil {
		log.Warn(ctx, "RetrieveByOffsets failed")
		return nil, err
	}
	metrics.QueryNodeSQSegmentLatencyInCore.WithLabelValues(paramtable.GetStringNodeID(),
		contextutil.GetQueryLabel(ctx)).Observe(float64(tr.ElapseSpan().Microseconds()) / 1000.0)
	return result, nil
}

func (s *LocalSegment) RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
	log := mlog.WithLazy(mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.Int64("msgID", plan.MsgID()),
		mlog.String("segmentType", s.segmentType.String()),
		mlog.Int("resultNum", len(plan.Offsets)),
	)

	result, err := s.retrieveByOffsets(ctx, plan, log)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	_, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "reduced-segcore-results-deserialization")
	defer span.End()

	retrieveResult, err := result.GetResult()
	if err != nil {
		log.Warn(ctx, "unmarshal retrieve by offsets result failed", mlog.Err(err))
		return nil, err
	}
	log.Debug(ctx, "retrieve by segment offsets done", mlog.Int("resultNum", len(retrieveResult.Offset)))
	return retrieveResult, nil
}

func (s *LocalSegment) Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error {
	if s.Type() != SegmentTypeGrowing {
		return merr.WrapErrServiceInternalMsg("unexpected segmentType when segmentInsert, segmentType = %s", s.segmentType.String())
	}
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	var result *segcore.InsertResult
	var err error
	GetMutatePool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				paramtable.GetStringNodeID(),
				"Insert",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()

		result, err = s.csegment.Insert(ctx, &segcore.InsertRequest{
			RowIDs:     rowIDs,
			Timestamps: timestamps,
			Record:     record,
		})
		return nil, nil
	}).Await()

	if err != nil {
		return err
	}
	s.insertCount.Add(result.InsertedRows)
	s.rowNum.Store(-1)
	s.memSize.Store(-1)
	return nil
}

func (s *LocalSegment) Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	/*
		CStatus
		Delete(CSegmentInterface c_segment,
		           long int reserved_offset,
		           long size,
		           const long* primary_keys,
		           const unsigned long* timestamps);
	*/

	if primaryKeys.Len() == 0 {
		return nil
	}
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	s.deltaMut.Lock()
	defer s.deltaMut.Unlock()

	// segcore DeletedRecord::InternalPush is idempotent on (PK, ts):
	// duplicate deletes against already-deleted rows are discarded internally.
	// Do NOT add a ts-watermark skip here. In L0-forward + partial-L0-compaction
	// scenarios, batches contain ts values below the watermark that have NOT yet
	// been applied to this segment, and skipping them causes silent data loss.

	var err error
	GetMutatePool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				paramtable.GetStringNodeID(),
				"Delete",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		_, err = s.csegment.Delete(ctx, &segcore.DeleteRequest{
			PrimaryKeys: primaryKeys,
			Timestamps:  timestamps,
		})
		return nil, nil
	}).Await()

	if err != nil {
		return err
	}

	s.rowNum.Store(-1)
	// Track max ts as a high-water-mark (consumed by file-level skip in
	// LoadDeltaLogs and by dist_handler for QueryCoord reporting). Using
	// tss[last] on unsorted batches underestimates the watermark.
	s.advanceLastDeltaTimestamp(timestamps)
	return nil
}

// -------------------------------------------------------------------------------------- interfaces for sealed segment
func (s *LocalSegment) LoadFieldData(ctx context.Context, fieldID int64, rowCount int64, field *datapb.FieldBinlog) error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadFieldData-%d-%d", s.ID(), fieldID))
	defer sp.End()

	log := mlog.With(
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.FieldFieldID(fieldID),
		mlog.Int64("rowCount", rowCount),
	)
	log.Info(ctx, "start loading field data for field")

	// TODO retrieve_enable should be considered
	collection := s.collection
	fieldSchema, err := getFieldSchema(collection.Schema(), fieldID)
	if err != nil {
		return err
	}
	mmapEnabled := isDataMmapEnable(fieldSchema)
	evictableEnabled := isDataEvictableEnable(fieldSchema)
	fieldWarmupPolicy := getFieldWarmupPolicy(fieldSchema)

	req := &segcore.LoadFieldDataRequest{
		Fields: []segcore.LoadFieldDataInfo{{
			Field:           field,
			EnableMMap:      mmapEnabled,
			SupportEviction: evictableEnabled,
			WarmupPolicy:    fieldWarmupPolicy,
		}},
		RowCount:       rowCount,
		StorageVersion: s.LoadInfo().GetStorageVersion(),
		LoadPriority:   s.LoadInfo().GetPriority(),
		Shard:          s.LoadInfo().GetInsertChannel(),
	}

	GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				paramtable.GetStringNodeID(),
				"LoadFieldData",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		_, err = s.csegment.LoadFieldData(ctx, req)
		log.Info(ctx, "submitted loadFieldData task to load pool")
		return nil, nil
	}).Await()

	if err != nil {
		log.Warn(ctx, "LoadFieldData failed", mlog.Err(err))
		return err
	}
	log.Info(ctx, "load field done")
	return nil
}

func (s *LocalSegment) LoadDeltaData(ctx context.Context, deltaData *storage.DeltaData) error {
	if deltaData.DeleteRowCount() == 0 {
		return nil
	}

	pks, tss := deltaData.DeletePks(), deltaData.DeleteTimestamps()
	rowNum := deltaData.DeleteRowCount()

	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	log := mlog.With(
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
	)

	s.deltaMut.Lock()
	defer s.deltaMut.Unlock()

	// See comment in Delete(): segcore dedups at (PK, ts) level, and tss is
	// NOT sorted across L0 segments (BufferForwarder appends in iteration
	// order), so comparing against tss[last] is both unnecessary and incorrect.

	ids, err := storage.ParsePrimaryKeysBatch2IDs(pks)
	if err != nil {
		return err
	}

	idsBlob, err := proto.Marshal(ids)
	if err != nil {
		return err
	}

	loadInfo := C.CLoadDeletedRecordInfo{
		timestamps:        unsafe.Pointer(&tss[0]),
		primary_keys:      (*C.uint8_t)(unsafe.Pointer(&idsBlob[0])),
		primary_keys_size: C.uint64_t(len(idsBlob)),
		row_count:         C.int64_t(rowNum),
	}
	/*
		CStatus
		LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info)
	*/
	var status C.CStatus
	// Delta-log replay during segment load runs on the load pool, not the
	// online-write mutate pool, so a large post-compaction replay cannot starve
	// online insert/delete (and thus tSafe advancement).
	GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				paramtable.GetStringNodeID(),
				"LoadDeletedRecord",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		status = C.LoadDeletedRecord(s.ptr, loadInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "LoadDeletedRecord failed",
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID())); err != nil {
		return err
	}

	s.rowNum.Store(-1)
	s.advanceLastDeltaTimestamp(tss)

	log.Info(ctx, "load deleted record done",
		mlog.Int64("rowNum", rowNum),
		mlog.String("segmentType", s.Type().String()))
	return nil
}

func GetCLoadInfoWithFunc(ctx context.Context,
	fieldSchema *schemapb.FieldSchema,
	loadInfo *querypb.SegmentLoadInfo,
	indexInfo *querypb.FieldIndexInfo,
	f func(c *LoadIndexInfo) error,
) error {
	// 1.
	loadIndexInfo, err := newLoadIndexInfo(ctx)
	if err != nil {
		return err
	}
	defer deleteLoadIndexInfo(loadIndexInfo)

	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	// as Knowhere reports error if encounter an unknown param, we need to delete it
	delete(indexParams, common.MmapEnabledKey)
	delete(indexParams, common.EvictableEnabledKey)

	// some build params also exist in indexParams, which are useless during loading process
	if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexParams["index_type"]) {
		if err := indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, indexInfo.GetNumRows()); err != nil {
			return err
		}
	}

	// set whether enable offset cache for bitmap index
	if indexParams["index_type"] == indexparamcheck.IndexBitmap {
		indexparams.SetBitmapIndexLoadParams(paramtable.Get(), indexParams)
	}

	if err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams); err != nil {
		return err
	}

	enableMmap := isIndexMmapEnable(fieldSchema, indexInfo)
	supportEviction := isIndexEvictableEnable(fieldSchema, indexInfo)
	// Add warmup policy to index_params if not already present
	// C++ will pass it to Knowhere for index loading
	if existingWarmup, exists := indexParams[common.WarmupKey]; exists {
		mlog.Info(ctx, "warmup policy already in index params (from QueryCoord)",
			mlog.FieldSegmentID(loadInfo.GetSegmentID()),
			mlog.FieldFieldID(indexInfo.GetFieldID()),
			mlog.String("warmup", existingWarmup))
	} else {
		warmupPolicy := getIndexWarmupPolicy(fieldSchema, indexInfo)
		mlog.Info(ctx, "warmup policy from getIndexWarmupPolicy",
			mlog.FieldSegmentID(loadInfo.GetSegmentID()),
			mlog.FieldFieldID(indexInfo.GetFieldID()),
			mlog.String("warmup", warmupPolicy))
		if warmupPolicy != "" {
			indexParams[common.WarmupKey] = warmupPolicy
		}
	}
	// Pass DataCoord-built index file paths through; QueryNode should not
	// attach v0/v1 path layout semantics to the read path.
	indexInfoProto := &cgopb.LoadIndexInfo{
		CollectionID:              loadInfo.GetCollectionID(),
		PartitionID:               loadInfo.GetPartitionID(),
		SegmentID:                 loadInfo.GetSegmentID(),
		Field:                     fieldSchema,
		EnableMmap:                enableMmap,
		IndexID:                   indexInfo.GetIndexID(),
		IndexBuildID:              indexInfo.GetBuildID(),
		IndexVersion:              indexInfo.GetIndexVersion(),
		IndexParams:               indexParams,
		IndexFiles:                indexInfo.GetIndexFilePaths(),
		IndexEngineVersion:        indexInfo.GetCurrentIndexVersion(),
		IndexFileSize:             indexInfo.GetIndexSize(),
		NumRows:                   indexInfo.GetNumRows(),
		CurrentScalarIndexVersion: indexInfo.GetCurrentScalarIndexVersion(),
		IndexStorePathVersion:     indexInfo.GetIndexStorePathVersion(),
		SupportEviction:           supportEviction,
	}

	// 2.
	if err := loadIndexInfo.appendLoadIndexInfo(ctx, indexInfoProto); err != nil {
		mlog.Warn(ctx, "fail to append load index info", mlog.Err(err))
		return err
	}
	loadIndexInfo.setShard(loadInfo.GetInsertChannel())
	return f(loadIndexInfo)
}

func (s *LocalSegment) LoadIndex(ctx context.Context, indexInfo *querypb.FieldIndexInfo, fieldType schemapb.DataType) error {
	log := mlog.With(
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.FieldFieldID(indexInfo.GetFieldID()),
		mlog.FieldIndexID(indexInfo.GetIndexID()),
	)

	old := s.GetIndexByID(indexInfo.GetIndexID())
	// the index loaded
	if old != nil && old.IsLoaded {
		log.Warn(ctx, "index already loaded")
		return nil
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, fmt.Sprintf("LoadIndex-%d-%d", s.ID(), indexInfo.GetFieldID()))
	defer sp.End()

	tr := timerecord.NewTimeRecorder("loadIndex")

	schemaHelper, err := typeutil.CreateSchemaHelper(s.GetCollection().Schema())
	if err != nil {
		return err
	}
	fieldSchema, err := schemaHelper.GetFieldFromID(indexInfo.GetFieldID())
	if err != nil {
		return err
	}

	// // if segment is pk sorted, user created indexes bring no performance gain but extra memory usage
	if s.IsSorted() && fieldSchema.GetIsPrimaryKey() {
		log.Info(ctx, "skip loading index for pk field in sorted segment")
		// set field index, preventing repeated loading index task
		s.fieldIndexes.Insert(indexInfo.GetFieldID(), &IndexedFieldInfo{
			FieldBinlog: &datapb.FieldBinlog{
				FieldID: indexInfo.GetFieldID(),
			},
			IndexInfo: indexInfo,
			IsLoaded:  true,
		})
		return nil
	}

	return s.innerLoadIndex(ctx, fieldSchema, indexInfo, tr, fieldType)
}

func (s *LocalSegment) innerLoadIndex(ctx context.Context,
	fieldSchema *schemapb.FieldSchema,
	indexInfo *querypb.FieldIndexInfo,
	tr *timerecord.TimeRecorder,
	fieldType schemapb.DataType,
) error {
	err := GetCLoadInfoWithFunc(ctx, fieldSchema,
		s.LoadInfo(), indexInfo, func(loadIndexInfo *LoadIndexInfo) error {
			newLoadIndexInfoSpan := tr.RecordSpan()

			if err := loadIndexInfo.loadIndex(ctx); err != nil {
				if loadIndexInfo.cleanLocalData(ctx) != nil {
					mlog.Warn(ctx, "failed to clean cached data on disk after append index failed",
						mlog.FieldBuildID(indexInfo.BuildID),
						mlog.Int64("index version", indexInfo.IndexVersion))
				}
				return err
			}
			if s.Type() != SegmentTypeSealed {
				return merr.WrapErrServiceInternalMsg("updateSegmentIndex failed, illegal segment type %s, segmentID = %d", s.segmentType, s.ID())
			}
			appendLoadIndexInfoSpan := tr.RecordSpan()

			// 3.
			err := s.UpdateIndexInfo(ctx, indexInfo, loadIndexInfo)
			if err != nil {
				return err
			}
			updateIndexInfoSpan := tr.RecordSpan()

			mlog.Info(ctx, "Finish loading index",
				mlog.Duration("newLoadIndexInfoSpan", newLoadIndexInfoSpan),
				mlog.Duration("appendLoadIndexInfoSpan", appendLoadIndexInfoSpan),
				mlog.Duration("updateIndexInfoSpan", updateIndexInfoSpan),
			)
			return nil
		})
	if err != nil {
		mlog.Warn(ctx, "load index failed", mlog.Err(err))
	}
	return err
}

func (s *LocalSegment) LoadJSONKeyIndex(ctx context.Context, jsonKeyStats *datapb.JsonKeyStats, schemaHelper *typeutil.SchemaHelper, basePath string) error {
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	if !paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetAsBool() {
		mlog.Warn(ctx, "load json key index failed, json key stats is not enabled")
		return nil
	}

	// for compatibility, we only support load data format version equal to the current data format version
	// if the data format version is less than the current version, wait for trigger a stats task again
	if jsonKeyStats.GetJsonKeyStatsDataFormat() != common.JSONStatsDataFormatVersion {
		mlog.Warn(ctx, "load json key index failed dataformat invalid", mlog.Int64("dataformat", jsonKeyStats.GetJsonKeyStatsDataFormat()), mlog.Int64("field id", jsonKeyStats.GetFieldID()), mlog.Any("json key logs", jsonKeyStats))
		return nil
	}
	mlog.Info(ctx, "load json key index", mlog.Int64("field id", jsonKeyStats.GetFieldID()), mlog.Any("json key logs", jsonKeyStats))
	s.fieldJSONStatsMu.RLock()
	_, loaded := s.fieldJSONStats[jsonKeyStats.GetFieldID()]
	s.fieldJSONStatsMu.RUnlock()
	if loaded {
		mlog.Warn(ctx, "JsonKeyIndexStats already loaded", mlog.Int64("field id", jsonKeyStats.GetFieldID()), mlog.Any("json key logs", jsonKeyStats))
		return nil
	}

	f, err := schemaHelper.GetFieldFromID(jsonKeyStats.GetFieldID())
	if err != nil {
		return err
	}

	// JSON key stats should based on scala field's warmup policy
	warmupPolicy := getScalarDataWarmupPolicy(f)
	supportEviction := isScalarStatsEvictableEnable(f)

	cgoProto := &indexcgopb.LoadJsonKeyIndexInfo{
		FieldID:         jsonKeyStats.GetFieldID(),
		Version:         jsonKeyStats.GetVersion(),
		BuildID:         jsonKeyStats.GetBuildID(),
		Files:           jsonKeyStats.GetFiles(),
		Schema:          f,
		CollectionID:    s.Collection(),
		PartitionID:     s.Partition(),
		LoadPriority:    s.loadInfo.Load().GetPriority(),
		EnableMmap:      paramtable.Get().QueryNodeCfg.MmapJSONStats.GetAsBool(),
		MmapDirPath:     paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue(),
		StatsSize:       jsonKeyStats.GetLogSize(),
		WarmupPolicy:    warmupPolicy,
		BasePath:        basePath,
		SupportEviction: supportEviction,
	}

	marshaled, err := proto.Marshal(cgoProto)
	if err != nil {
		return err
	}

	guard := segcore.NewCancellationGuard(ctx)
	defer guard.Close()

	var status C.CStatus
	_, _ = GetLoadPool().Submit(func() (any, error) {
		traceCtx := ParseCTraceContext(ctx)
		status = C.LoadJsonKeyIndex(traceCtx.ctx, s.ptr, (*C.uint8_t)(unsafe.Pointer(&marshaled[0])), (C.uint64_t)(len(marshaled)), (C.CLoadCancellationSource)(guard.Source()))
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "Load JsonKeyStats failed"); err != nil {
		return err
	}
	s.fieldJSONStatsMu.Lock()
	s.fieldJSONStats[jsonKeyStats.GetFieldID()] = &querypb.JsonStatsInfo{
		FieldID:           jsonKeyStats.GetFieldID(),
		DataFormatVersion: jsonKeyStats.GetJsonKeyStatsDataFormat(),
		BuildID:           jsonKeyStats.GetBuildID(),
		VersionID:         jsonKeyStats.GetVersion(),
	}
	s.fieldJSONStatsMu.Unlock()
	return nil
}

func (s *LocalSegment) UpdateIndexInfo(ctx context.Context, indexInfo *querypb.FieldIndexInfo, info *LoadIndexInfo) error {
	log := mlog.With(
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.FieldFieldID(indexInfo.FieldID),
	)
	if !s.ptrLock.PinIf(state.IsNotReleased) {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released")
	}
	defer s.ptrLock.Unpin()

	var status C.CStatus
	GetDynamicPool().Submit(func() (any, error) {
		status = C.UpdateSealedSegmentIndex(s.ptr, info.cLoadIndexInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "UpdateSealedSegmentIndex failed",
		mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.FieldFieldID(indexInfo.FieldID)); err != nil {
		return err
	}

	s.fieldIndexes.Insert(indexInfo.GetIndexID(), &IndexedFieldInfo{
		FieldBinlog: &datapb.FieldBinlog{
			FieldID: indexInfo.GetFieldID(),
		},
		IndexInfo: indexInfo,
		IsLoaded:  true,
	})
	log.Info(ctx, "updateSegmentIndex done")
	return nil
}

func (s *LocalSegment) UpdateFieldRawDataSize(ctx context.Context, numRows int64, fieldBinlog *datapb.FieldBinlog) error {
	var status C.CStatus
	fieldID := fieldBinlog.FieldID
	fieldDataSize := int64(0)
	for _, binlog := range fieldBinlog.GetBinlogs() {
		fieldDataSize += binlog.GetMemorySize()
	}
	GetDynamicPool().Submit(func() (any, error) {
		status = C.UpdateFieldRawDataSize(s.ptr, C.int64_t(fieldID), C.int64_t(numRows), C.int64_t(fieldDataSize))
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "updateFieldRawDataSize failed"); err != nil {
		return err
	}
	mlog.Info(ctx, "updateFieldRawDataSize done", mlog.FieldSegmentID(s.ID()))

	return nil
}

func (s *LocalSegment) syncFieldJSONStatsFromLoadInfo(ctx context.Context, loadInfo *querypb.SegmentLoadInfo) {
	jsonStatsInfo := make(map[int64]*querypb.JsonStatsInfo)
	if !paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetAsBool() {
		mlog.Warn(ctx, "skip sync json key stats, json key stats is not enabled", mlog.FieldSegmentID(s.ID()))
		s.fieldJSONStatsMu.Lock()
		s.fieldJSONStats = jsonStatsInfo
		s.fieldJSONStatsMu.Unlock()
		return
	}

	statsResult := packed.NewStatsResolverFromLoadInfo(loadInfo).TextAndJSONIndexStatsWithBasePaths()
	jsonKeyStats := statsResult.JSONKeyStats
	if statsResult.Err() != nil {
		mlog.Warn(ctx, "failed to resolve json key stats from manifest",
			mlog.FieldSegmentID(loadInfo.GetSegmentID()),
			mlog.String("manifestPath", loadInfo.GetManifestPath()),
			mlog.Err(statsResult.Err()))
		jsonKeyStats = loadInfo.GetJsonKeyStatsLogs()
	}

	for fieldID, stats := range jsonKeyStats {
		if stats == nil {
			continue
		}
		if stats.GetJsonKeyStatsDataFormat() != common.JSONStatsDataFormatVersion {
			mlog.Warn(ctx, "skip sync json key stats, data format invalid",
				mlog.FieldSegmentID(loadInfo.GetSegmentID()),
				mlog.FieldFieldID(fieldID),
				mlog.FieldBuildID(stats.GetBuildID()),
				mlog.Int64("version", stats.GetVersion()),
				mlog.Int64("dataFormat", stats.GetJsonKeyStatsDataFormat()),
				mlog.Int64("expectedDataFormat", common.JSONStatsDataFormatVersion))
			continue
		}
		jsonStatsInfo[fieldID] = &querypb.JsonStatsInfo{
			FieldID:           stats.GetFieldID(),
			DataFormatVersion: stats.GetJsonKeyStatsDataFormat(),
			BuildID:           stats.GetBuildID(),
			VersionID:         stats.GetVersion(),
		}
	}

	s.fieldJSONStatsMu.Lock()
	s.fieldJSONStats = jsonStatsInfo
	s.fieldJSONStatsMu.Unlock()
}

func (s *LocalSegment) Load(ctx context.Context) error {
	if err := s.csegment.Load(ctx); err != nil {
		return err
	}
	s.syncFieldJSONStatsFromLoadInfo(ctx, s.LoadInfo())
	return nil
}

func (s *LocalSegment) Reopen(ctx context.Context, newLoadInfo *querypb.SegmentLoadInfo) error {
	if !s.ptrLock.PinIfNotReleased() {
		return merr.WrapErrSegmentNotLoaded(s.ID(), "segment released during reopen")
	}
	defer s.ptrLock.Unpin()

	schema, schemaVersion := s.collection.SchemaAndSegcoreVersion()
	err := s.csegment.Reopen(ctx, &segcore.ReopenRequest{
		LoadInfo:      newLoadInfo,
		Schema:        schema,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return err
	}
	s.syncFieldIndexes(newLoadInfo.GetIndexInfos())
	s.loadInfo.Store(newLoadInfo)
	s.syncFieldJSONStatsFromLoadInfo(ctx, newLoadInfo)
	return nil
}

type ReleaseScope int

const (
	ReleaseScopeAll ReleaseScope = iota
	ReleaseScopeData
)

type releaseOptions struct {
	Scope ReleaseScope
}

func newReleaseOptions() *releaseOptions {
	return &releaseOptions{
		Scope: ReleaseScopeAll,
	}
}

type releaseOption func(*releaseOptions)

func WithReleaseScope(scope ReleaseScope) releaseOption {
	return func(options *releaseOptions) {
		options.Scope = scope
	}
}

func (s *LocalSegment) Release(ctx context.Context, opts ...releaseOption) {
	options := newReleaseOptions()
	for _, opt := range opts {
		opt(options)
	}
	stateLockGuard := s.startRelease(options.Scope)
	if stateLockGuard == nil { // release is already done.
		return
	}
	// release will never fail
	defer stateLockGuard.Done(nil)

	log := mlog.With(mlog.FieldCollectionID(s.Collection()),
		mlog.FieldPartitionID(s.Partition()),
		mlog.FieldSegmentID(s.ID()),
		mlog.String("segmentType", s.segmentType.String()),
		mlog.Int64("insertCount", s.InsertCount()),
	)

	// wait all read ops finished
	ptr := s.ptr
	if options.Scope == ReleaseScopeData {
		s.ReleaseSegmentData()
		log.Info(ctx, "release segment data done and the field indexes info has been set lazy load=true")
		return
	}

	if paramtable.Get().QueryNodeCfg.ExprResCacheEnabled.GetAsBool() {
		// erase expr-cache for this segment before deleting C segment
		C.ExprResCacheEraseSegment(C.int64_t(s.ID()))
	}

	GetDynamicPool().Submit(func() (any, error) {
		C.DeleteSegment(ptr)
		return nil, nil
	}).Await()

	// TODO: disable logical resource handling for now
	// usage := s.ResourceUsageEstimate()
	// s.manager.SubLogicalResource(usage)

	// Refund PK candidate resource
	s.pkCandidate.Refund()

	binlogSize := s.binlogSize.Load()
	if binlogSize > 0 {
		// no concurrent change to s.binlogSize, so the subtraction is safe
		s.manager.SubLoadedBinlogSize(binlogSize)
		s.binlogSize.Store(0)
	}

	log.Info(ctx, "delete segment from memory")
}

// ReleaseSegmentData releases the segment data.
func (s *LocalSegment) ReleaseSegmentData() {
	GetDynamicPool().Submit(func() (any, error) {
		C.ClearSegmentData(s.ptr)
		return nil, nil
	}).Await()
	for _, indexInfo := range s.Indexes() {
		indexInfo.IsLoaded = false
	}
}

// StartLoadData starts the loading process of the segment.
func (s *LocalSegment) StartLoadData() (state.LoadStateLockGuard, error) {
	return s.ptrLock.StartLoadData()
}

// startRelease starts the releasing process of the segment.
func (s *LocalSegment) startRelease(scope ReleaseScope) state.LoadStateLockGuard {
	switch scope {
	case ReleaseScopeData:
		return s.ptrLock.StartReleaseData()
	case ReleaseScopeAll:
		return s.ptrLock.StartReleaseAll()
	default:
		panic(fmt.Sprintf("unexpected release scope %d", scope))
	}
}

func (s *LocalSegment) RemoveFieldFile(fieldId int64) {
	GetDynamicPool().Submit(func() (any, error) {
		C.RemoveFieldFile(s.ptr, C.int64_t(fieldId))
		return nil, nil
	}).Await()
}

func (s *LocalSegment) RemoveUnusedFieldFiles() error {
	schema := s.collection.Schema()
	indexInfos, _ := separateIndexAndBinlog(s.LoadInfo())
	for _, indexInfo := range indexInfos {
		need, err := s.indexNeedLoadRawData(schema, indexInfo)
		if err != nil {
			return err
		}
		if !need {
			s.RemoveFieldFile(indexInfo.IndexInfo.FieldID)
		}
	}
	return nil
}

func (s *LocalSegment) indexNeedLoadRawData(schema *schemapb.CollectionSchema, indexInfo *IndexedFieldInfo) (bool, error) {
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return false, err
	}
	fieldSchema, err := schemaHelper.GetFieldFromID(indexInfo.IndexInfo.FieldID)
	if err != nil {
		return false, err
	}
	return !typeutil.IsVectorType(fieldSchema.DataType) && s.HasRawData(indexInfo.IndexInfo.FieldID), nil
}

func (s *LocalSegment) GetFieldJSONIndexStats() map[int64]*querypb.JsonStatsInfo {
	s.fieldJSONStatsMu.RLock()
	defer s.fieldJSONStatsMu.RUnlock()

	stats := make(map[int64]*querypb.JsonStatsInfo, len(s.fieldJSONStats))
	for fieldID, info := range s.fieldJSONStats {
		if info == nil {
			continue
		}
		stats[fieldID] = proto.Clone(info).(*querypb.JsonStatsInfo)
	}
	return stats
}

// FlushData flushes data from the growing segment directly to storage via C++ milvus-storage.
// This is a unified interface that combines data extraction from segcore and writing to storage.
// The C++ side handles: extracting raw field data from ConcurrentVector, converting to Arrow,
// and writing to storage via milvus-storage with TEXT column LOB handling.
func (s *LocalSegment) FlushData(ctx context.Context, startOffset, endOffset int64, config *FlushConfig) (*FlushResult, error) {
	// currently only growing segments support FlushData
	if s.Type() != SegmentTypeGrowing {
		return nil, merr.WrapErrServiceInternalMsg("FlushData is only supported for growing segments, got %s", s.Type().String())
	}

	// validate offsets
	if startOffset < 0 || endOffset < startOffset {
		return nil, merr.WrapErrServiceInternalMsg("invalid offsets: start=%d, end=%d", startOffset, endOffset)
	}

	// no data to flush
	if startOffset == endOffset {
		return nil, nil
	}
	if config == nil {
		return nil, merr.WrapErrServiceInternalMsg("flush config is nil")
	}
	if config.Schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("flush schema is nil")
	}

	schemaBlob, err := proto.Marshal(config.Schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalMsg("marshal flush schema failed: %s", err.Error())
	}
	if len(schemaBlob) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("marshal flush schema returned empty blob")
	}

	// build C flush config
	var cConfig C.CFlushConfig
	cSegmentPath := C.CString(config.SegmentBasePath)
	defer C.free(unsafe.Pointer(cSegmentPath))
	cConfig.segment_path = cSegmentPath
	cSchemaBlob := C.CBytes(schemaBlob)
	defer C.free(cSchemaBlob)
	cConfig.schema_blob = cSchemaBlob
	cConfig.schema_length = C.int64_t(len(schemaBlob))

	cConfig.read_version = C.int64_t(config.ReadVersion)
	cConfig.retry_limit = C.uint32_t(3)
	writerFormat := config.WriterFormat
	if writerFormat != "" {
		cWriterFormat := C.CString(writerFormat)
		defer C.free(unsafe.Pointer(cWriterFormat))
		cConfig.writer_format = cWriterFormat
	}
	schemaBasedPattern := config.SchemaBasedPattern
	if schemaBasedPattern != "" {
		cSchemaBasedPattern := C.CString(schemaBasedPattern)
		defer C.free(unsafe.Pointer(cSchemaBasedPattern))
		cConfig.schema_based_pattern = cSchemaBasedPattern
	}
	schemaBasedFormats := config.SchemaBasedFormats
	if schemaBasedFormats != "" {
		cSchemaBasedFormats := C.CString(schemaBasedFormats)
		defer C.free(unsafe.Pointer(cSchemaBasedFormats))
		cConfig.schema_based_formats = cSchemaBasedFormats
	}
	numAllowedFields := len(config.AllowedFieldIDs)
	if numAllowedFields > 0 {
		cAllowedFieldIDs := (*C.int64_t)(C.malloc(C.size_t(numAllowedFields) * C.size_t(unsafe.Sizeof(C.int64_t(0)))))
		defer C.free(unsafe.Pointer(cAllowedFieldIDs))
		allowedFieldIDSlice := unsafe.Slice(cAllowedFieldIDs, numAllowedFields)
		for i, fieldID := range config.AllowedFieldIDs {
			allowedFieldIDSlice[i] = C.int64_t(fieldID)
		}
		cConfig.allowed_field_ids = cAllowedFieldIDs
		cConfig.num_allowed_fields = C.size_t(numAllowedFields)
	}
	numColumnGroups := len(config.ColumnGroups)
	if numColumnGroups > 0 {
		totalFieldCount := 0
		for _, columnGroup := range config.ColumnGroups {
			totalFieldCount += len(columnGroup.Fields)
		}
		cColumnGroupIDs := (*C.int64_t)(C.malloc(C.size_t(numColumnGroups) * C.size_t(unsafe.Sizeof(C.int64_t(0)))))
		defer C.free(unsafe.Pointer(cColumnGroupIDs))
		cColumnGroupFieldCounts := (*C.size_t)(C.malloc(C.size_t(numColumnGroups) * C.size_t(unsafe.Sizeof(C.size_t(0)))))
		defer C.free(unsafe.Pointer(cColumnGroupFieldCounts))
		var cColumnGroupFieldIDs *C.int64_t
		if totalFieldCount > 0 {
			cColumnGroupFieldIDs = (*C.int64_t)(C.malloc(C.size_t(totalFieldCount) * C.size_t(unsafe.Sizeof(C.int64_t(0)))))
			defer C.free(unsafe.Pointer(cColumnGroupFieldIDs))
		}
		groupIDSlice := unsafe.Slice(cColumnGroupIDs, numColumnGroups)
		fieldCountSlice := unsafe.Slice(cColumnGroupFieldCounts, numColumnGroups)
		var fieldIDSlice []C.int64_t
		if totalFieldCount > 0 {
			fieldIDSlice = unsafe.Slice(cColumnGroupFieldIDs, totalFieldCount)
		}
		fieldOffset := 0
		for i, columnGroup := range config.ColumnGroups {
			groupIDSlice[i] = C.int64_t(columnGroup.GroupID)
			fieldCountSlice[i] = C.size_t(len(columnGroup.Fields))
			for _, fieldID := range columnGroup.Fields {
				fieldIDSlice[fieldOffset] = C.int64_t(fieldID)
				fieldOffset++
			}
		}
		cConfig.column_group_ids = cColumnGroupIDs
		cConfig.column_group_field_ids = cColumnGroupFieldIDs
		cConfig.column_group_field_counts = cColumnGroupFieldCounts
		cConfig.num_column_groups = C.size_t(numColumnGroups)
	}

	// populate TEXT column configs
	// All arrays must be C-allocated to avoid "Go pointer to unpinned Go pointer" panic
	numTextCols := len(config.TextFieldIDs)
	if numTextCols > 0 {
		// allocate C arrays via C.malloc (not Go slices) to satisfy CGO pointer rules
		cFieldIDs := (*C.int64_t)(C.malloc(C.size_t(numTextCols) * C.size_t(unsafe.Sizeof(C.int64_t(0)))))
		defer C.free(unsafe.Pointer(cFieldIDs))

		cLobPathPtrs := (**C.char)(C.malloc(C.size_t(numTextCols) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
		defer C.free(unsafe.Pointer(cLobPathPtrs))

		fieldIDSlice := unsafe.Slice(cFieldIDs, numTextCols)
		lobPathSlice := unsafe.Slice(cLobPathPtrs, numTextCols)
		for i := 0; i < numTextCols; i++ {
			fieldIDSlice[i] = C.int64_t(config.TextFieldIDs[i])
			lobPathSlice[i] = C.CString(config.TextLobPaths[i])
			defer C.free(unsafe.Pointer(lobPathSlice[i]))
		}

		cConfig.text_field_ids = cFieldIDs
		cConfig.text_lob_paths = cLobPathPtrs
		cConfig.text_inline_threshold = C.int64_t(config.TextInlineThreshold)
		cConfig.text_max_lob_file_bytes = C.int64_t(config.TextMaxLobFileBytes)
		cConfig.text_flush_threshold_bytes = C.int64_t(config.TextFlushThresholdBytes)
		cConfig.num_text_columns = C.size_t(numTextCols)
	} else {
		cConfig.text_field_ids = nil
		cConfig.text_lob_paths = nil
		cConfig.num_text_columns = 0
	}
	numBM25Fields := len(config.BM25FieldIDs)
	if numBM25Fields > 0 {
		if len(config.BM25StatsLogIDs) != numBM25Fields {
			return nil, merr.WrapErrServiceInternalMsg("BM25 stats log IDs count mismatch, fields=%d logIDs=%d", numBM25Fields, len(config.BM25StatsLogIDs))
		}
		cBM25FieldIDs := (*C.int64_t)(C.malloc(C.size_t(numBM25Fields) * C.size_t(unsafe.Sizeof(C.int64_t(0)))))
		defer C.free(unsafe.Pointer(cBM25FieldIDs))
		cBM25StatsLogIDs := (*C.int64_t)(C.malloc(C.size_t(numBM25Fields) * C.size_t(unsafe.Sizeof(C.int64_t(0)))))
		defer C.free(unsafe.Pointer(cBM25StatsLogIDs))
		bm25FieldIDSlice := unsafe.Slice(cBM25FieldIDs, numBM25Fields)
		bm25StatsLogIDSlice := unsafe.Slice(cBM25StatsLogIDs, numBM25Fields)
		for i, fieldID := range config.BM25FieldIDs {
			bm25FieldIDSlice[i] = C.int64_t(fieldID)
			bm25StatsLogIDSlice[i] = C.int64_t(config.BM25StatsLogIDs[i])
		}
		cConfig.bm25_field_ids = cBM25FieldIDs
		cConfig.bm25_stats_log_ids = cBM25StatsLogIDs
		cConfig.num_bm25_fields = C.size_t(numBM25Fields)
	} else {
		cConfig.bm25_field_ids = nil
		cConfig.bm25_stats_log_ids = nil
		cConfig.num_bm25_fields = 0
	}
	cConfig.write_merged_bm25_stats = C.bool(config.WriteMergedBM25Stats)

	// call C FFI
	var cResult C.CFlushResult
	status := C.FlushGrowingSegmentData(
		s.ptr,
		C.int64_t(startOffset),
		C.int64_t(endOffset),
		&cConfig,
		&cResult,
	)
	defer C.FreeFlushResult(&cResult)

	if err := HandleCStatus(ctx, &status, "FlushGrowingSegmentData"); err != nil {
		return nil, err
	}

	// no data flushed
	if cResult.manifest_path == nil {
		return nil, nil
	}

	// C++ SegmentWriter returns a raw file path like:
	//   "files/insert_log/{coll}/{part}/{seg}/_metadata/manifest-{ver}.avro"
	// But GetLoonManifest() expects a JSON-encoded manifest path like:
	//   {"ver":{ver},"base_path":"files/insert_log/{coll}/{part}/{seg}"}
	// Convert using the committed_version and base path extraction.
	rawPath := C.GoString(cResult.manifest_path)
	committedVersion := int64(cResult.committed_version)

	// Extract base path: strip "/_metadata/manifest-{ver}.avro" suffix
	basePath := rawPath
	if idx := strings.Index(rawPath, "/_metadata/"); idx >= 0 {
		basePath = rawPath[:idx]
	}
	manifestPath := packed.MarshalManifestPath(basePath, committedVersion)
	fieldNullCounts := make(map[int64]int64, int(cResult.num_field_stats))
	if cResult.num_field_stats > 0 {
		fieldIDs := unsafe.Slice(cResult.field_ids, int(cResult.num_field_stats))
		nullCounts := unsafe.Slice(cResult.field_null_counts, int(cResult.num_field_stats))
		for i := 0; i < int(cResult.num_field_stats); i++ {
			fieldID := int64(fieldIDs[i])
			fieldNullCounts[fieldID] = int64(nullCounts[i])
		}
	}
	columnGroupMemorySizes := make(map[int64]int64, int(cResult.num_column_groups))
	if cResult.num_column_groups > 0 {
		columnGroupIDs := unsafe.Slice(cResult.column_group_ids, int(cResult.num_column_groups))
		memorySizes := unsafe.Slice(cResult.column_group_memory_sizes, int(cResult.num_column_groups))
		for i := 0; i < int(cResult.num_column_groups); i++ {
			columnGroupMemorySizes[int64(columnGroupIDs[i])] = int64(memorySizes[i])
		}
	}
	bm25Stats := make(map[int64]*storage.BM25Stats, int(cResult.num_bm25_stats))
	if cResult.num_bm25_stats > 0 {
		fieldIDs := unsafe.Slice(cResult.bm25_field_ids, int(cResult.num_bm25_stats))
		statsBytes := unsafe.Slice(cResult.bm25_stats, int(cResult.num_bm25_stats))
		statsSizes := unsafe.Slice(cResult.bm25_stats_sizes, int(cResult.num_bm25_stats))
		for i := 0; i < int(cResult.num_bm25_stats); i++ {
			bytes := C.GoBytes(unsafe.Pointer(statsBytes[i]), C.int(statsSizes[i]))
			stats, err := storage.NewBM25StatsWithBytes(bytes)
			if err != nil {
				return nil, err
			}
			bm25Stats[int64(fieldIDs[i])] = stats
		}
	}

	return &FlushResult{
		ManifestPath:           manifestPath,
		NumRows:                int64(cResult.num_rows),
		TimestampFrom:          uint64(cResult.timestamp_from),
		TimestampTo:            uint64(cResult.timestamp_to),
		ColumnGroupMemorySizes: columnGroupMemorySizes,
		FieldNullCounts:        fieldNullCounts,
		BM25Stats:              bm25Stats,
	}, nil
}
