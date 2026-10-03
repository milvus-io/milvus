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

package delegator

/*
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
*/
import "C"

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const memoryHeadroom = 4 * 1024 * 1024 // 4MB headroom for Insert path, ~50K unique tokens

type IDFOracle interface {
	SetNext(snapshot *snapshot)
	TargetVersion() int64

	UpdateGrowing(segmentID int64, stats bm25Stats)
	// mark growing segment remove target version
	LazyRemoveGrowings(targetVersion int64, segmentIDs ...int64)

	RegisterGrowing(segmentID int64, stats bm25Stats)
	// LoadSealed loads BM25 stats for a sealed segment from remote storage.
	// Internally handles: streaming download → local disk → optional parse → register.
	// Idempotent: skips if segment already loaded.
	LoadSealed(ctx context.Context, segmentID int64, loadInfo *querypb.SegmentLoadInfo, cm storage.ChunkManager) error
	LoadSealedForReopen(ctx context.Context, segmentID int64, loadInfo *querypb.SegmentLoadInfo, cm storage.ChunkManager, activateIfReadable bool) error
	SyncFunctions(functions []*schemapb.FunctionSchema) error

	BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)

	DirPath() string

	Start()
	Close()
}

type bm25FunctionSet map[typeutil.UniqueID]*schemapb.FunctionSchema

type bm25Stats map[int64]*storage.BM25Stats

func (s bm25Stats) Clone() bm25Stats {
	if len(s) == 0 {
		return bm25Stats{}
	}
	cloned := make(bm25Stats, len(s))
	for fieldID, stats := range s {
		if stats != nil {
			cloned[fieldID] = stats.Clone()
		}
	}
	return cloned
}

func newBM25FunctionSet(schema *schemapb.CollectionSchema) bm25FunctionSet {
	result := make(bm25FunctionSet)
	if schema == nil {
		return result
	}
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		result[function.GetOutputFieldIds()[0]] = function
	}
	return result
}

func (s bm25FunctionSet) IsSupersetOf(old bm25FunctionSet) bool {
	for outputFieldID, oldFunction := range old {
		newFunction, ok := s[outputFieldID]
		if !ok || !sameBM25Function(newFunction, oldFunction) {
			return false
		}
	}
	return true
}

func (s bm25FunctionSet) HasIncompatibleCommonFunction(old bm25FunctionSet) bool {
	// Do not reuse IsSupersetOf here: loaded collections must allow BM25
	// function fields to be dropped and re-added with new output field IDs,
	// while still rejecting in-place changes to an existing output field.
	for outputFieldID, newFunction := range s {
		oldFunction, ok := old[outputFieldID]
		if ok && !sameBM25Function(newFunction, oldFunction) {
			return true
		}
	}
	return false
}

func (s bm25FunctionSet) Equal(other bm25FunctionSet) bool {
	return len(s) == len(other) && s.IsSupersetOf(other)
}

func sameBM25Function(a, b *schemapb.FunctionSchema) bool {
	return a.GetType() == b.GetType() &&
		slices.Equal(a.GetInputFieldIds(), b.GetInputFieldIds()) &&
		slices.Equal(a.GetOutputFieldIds(), b.GetOutputFieldIds()) &&
		common.KeyValuePairs(a.GetParams()).Equal(b.GetParams())
}

func (s bm25Stats) Merge(stats bm25Stats) {
	for fieldID, newstats := range stats {
		if stats, ok := s[fieldID]; ok {
			stats.Merge(newstats)
		} else {
			s[fieldID] = storage.NewBM25Stats()
			s[fieldID].Merge(newstats)
		}
	}
}

func (s bm25Stats) Minus(stats bm25Stats) {
	for fieldID, newstats := range stats {
		if stats, ok := s[fieldID]; ok {
			stats.Minus(newstats)
		} else {
			s[fieldID] = storage.NewBM25Stats()
			s[fieldID].Minus(newstats)
		}
	}
}

func (s bm25Stats) GetStats(fieldID int64) (*storage.BM25Stats, error) {
	stats, ok := s[fieldID]
	if !ok {
		return nil, merr.WrapErrFieldNotFound(fieldID, "not in idf oracle BM25 stats")
	}
	return stats, nil
}

func (s bm25Stats) NumRow() int64 {
	for _, stats := range s {
		return stats.NumRow()
	}
	return 0
}

type sealedBm25Stats struct {
	sync.RWMutex // Protect all data in struct except activate

	activate *atomic.Bool

	removed   bool
	segmentID int64
	ts        time.Time // Time of segment register
	localDir  string
	fieldList []int64 // bm25 field list
	diskSize  int64   // total disk size of local files

	// version changes whenever what the segment contributes to current changes:
	// activation and its field list. Written with the struct lock held.
	version uint64
}

func (s *sealedBm25Stats) HasField(fieldID int64) bool {
	s.RLock()
	defer s.RUnlock()
	return s.hasFieldLocked(fieldID)
}

func (s *sealedBm25Stats) hasFieldLocked(fieldID int64) bool {
	for _, existingFieldID := range s.fieldList {
		if existingFieldID == fieldID {
			return true
		}
	}
	return false
}

func (s *sealedBm25Stats) AddFields(fieldIDs []int64) {
	s.Lock()
	defer s.Unlock()
	s.addFieldsLocked(fieldIDs)
}

func (s *sealedBm25Stats) addFieldsLocked(fieldIDs []int64) {
	for _, fieldID := range fieldIDs {
		if s.hasFieldLocked(fieldID) {
			continue
		}
		s.fieldList = append(s.fieldList, fieldID)
		s.version++
	}
}

func (s *sealedBm25Stats) RetainFields(fieldIDs map[int64]struct{}) int64 {
	s.Lock()
	defer s.Unlock()

	kept := s.fieldList[:0]
	removedDiskSize := int64(0)
	for _, fieldID := range s.fieldList {
		if _, ok := fieldIDs[fieldID]; ok {
			kept = append(kept, fieldID)
			continue
		}
		if s.localDir == "" {
			continue
		}
		fieldDir := path.Join(s.localDir, fmt.Sprintf("%d", fieldID))
		fieldDiskSize := bm25FieldDirDiskSize(fieldDir)
		if err := os.RemoveAll(fieldDir); err != nil {
			// Removal failed: the files remain on disk, so keep tracking the
			// field and do not count its bytes as freed — tracked disk usage
			// must match what is physically present.
			mlog.Warn(context.TODO(), "remove dropped bm25 stats field failed", mlog.Err(err), mlog.String("path", fieldDir))
			kept = append(kept, fieldID)
			continue
		}
		removedDiskSize += fieldDiskSize
	}
	if len(kept) != len(s.fieldList) {
		s.version++
	}
	s.fieldList = kept
	if removedDiskSize > s.diskSize {
		removedDiskSize = s.diskSize
	}
	s.diskSize -= removedDiskSize
	return removedDiskSize
}

func (s *sealedBm25Stats) FieldList() []int64 {
	s.RLock()
	defer s.RUnlock()
	return append([]int64(nil), s.fieldList...)
}

func (s *sealedBm25Stats) Remove() {
	s.Lock()
	defer s.Unlock()
	s.removed = true

	if s.localDir != "" {
		err := os.RemoveAll(s.localDir)
		if err != nil {
			mlog.Warn(context.TODO(), "remove local bm25 stats failed", mlog.Err(err), mlog.String("path", s.localDir))
		}
	}
}

// FetchStats reads stats from local multi-file directory and merges per field.
// Local directory structure: {localDir}/{fieldID}/0.data, 1.data, ...
func (s *sealedBm25Stats) FetchStats() (map[int64]*storage.BM25Stats, error) {
	s.RLock()
	defer s.RUnlock()
	return s.fetchFieldsLocked(s.fieldList)
}

// fetchSnapshot reads the stats together with the version and field list they belong to.
func (s *sealedBm25Stats) fetchSnapshot() (bm25Stats, uint64, []int64, error) {
	s.RLock()
	defer s.RUnlock()
	stats, err := s.fetchFieldsLocked(s.fieldList)
	if err != nil {
		return nil, 0, nil, err
	}
	return stats, s.version, slices.Clone(s.fieldList), nil
}

// fetchFieldsLocked reads the stats of the given fields. Caller must hold the lock (read or write).
func (s *sealedBm25Stats) fetchFieldsLocked(fieldIDs []int64) (map[int64]*storage.BM25Stats, error) {
	if s.removed {
		return nil, merr.WrapErrServiceInternalMsg("sealed bm25 stats for segment %d already removed", s.segmentID)
	}

	stats := make(map[int64]*storage.BM25Stats)
	for _, fieldID := range fieldIDs {
		fieldDir := path.Join(s.localDir, fmt.Sprintf("%d", fieldID))
		entries, err := os.ReadDir(fieldDir)
		if err != nil {
			return nil, merr.WrapErrIoFailed(fieldDir, err)
		}

		fieldStats := storage.NewBM25Stats()
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			filePath := path.Join(fieldDir, entry.Name())
			f, err := os.Open(filePath)
			if err != nil {
				return nil, merr.WrapErrIoFailed(filePath, err)
			}
			err = fieldStats.DeserializeFromReader(bufio.NewReader(f))
			f.Close()
			if err != nil {
				return nil, merr.WrapErrSerializationFailed(err, "deserialize local file %s", filePath)
			}
		}
		stats[fieldID] = fieldStats
	}

	return stats, nil
}

type growingBm25Stats struct {
	bm25Stats

	activate       bool
	droppedVersion int64
}

func newBm25Stats(functions []*schemapb.FunctionSchema) bm25Stats {
	stats := make(map[int64]*storage.BM25Stats)

	for _, function := range functions {
		if function.GetType() == schemapb.FunctionType_BM25 {
			stats[function.GetOutputFieldIds()[0]] = storage.NewBM25Stats()
		}
	}
	return stats
}

func bm25FunctionFieldIDs(functions []*schemapb.FunctionSchema) map[int64]struct{} {
	fieldIDs := make(map[int64]struct{})
	for _, function := range functions {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		fieldIDs[function.GetOutputFieldIds()[0]] = struct{}{}
	}
	return fieldIDs
}

func (s bm25Stats) SyncFunctions(functions []*schemapb.FunctionSchema) map[int64]struct{} {
	fieldIDs := bm25FunctionFieldIDs(functions)
	s.RetainFields(fieldIDs)
	for fieldID := range fieldIDs {
		if _, ok := s[fieldID]; !ok {
			s[fieldID] = storage.NewBM25Stats()
		}
	}
	return fieldIDs
}

func (s bm25Stats) RetainFields(fieldIDs map[int64]struct{}) {
	for fieldID := range s {
		if _, ok := fieldIDs[fieldID]; !ok {
			delete(s, fieldID)
		}
	}
}

// idfSyncParallelism bounds the goroutines that read sealed segment stats in SyncDistribution.
const idfSyncParallelism = 8

// idfSyncMaxAttempts bounds how often SyncDistribution restarts when the BM25 fields change while it reads stats.
const idfSyncMaxAttempts = 3

// syncDistributionAfterFetchHook runs in tests between reading stats outside the lock and applying them.
var syncDistributionAfterFetchHook func()

// statsTable is the shard-wide BM25 stats of each BM25 field. Merge and Minus are safe for
// concurrent use, so loads holding only the oracle read lock can merge in parallel.
// Changing the field set (SyncFunctions) needs the oracle write lock.
type statsTable struct {
	mu     sync.RWMutex // guards fields
	fields map[int64]*storage.ConcurrentBM25Stats
}

func newStatsTable(functions []*schemapb.FunctionSchema) *statsTable {
	t := &statsTable{fields: make(map[int64]*storage.ConcurrentBM25Stats)}
	for _, function := range functions {
		if function.GetType() == schemapb.FunctionType_BM25 {
			t.fields[function.GetOutputFieldIds()[0]] = storage.NewConcurrentBM25Stats()
		}
	}
	return t
}

func (t *statsTable) field(fieldID int64) *storage.ConcurrentBM25Stats {
	t.mu.RLock()
	stats, ok := t.fields[fieldID]
	t.mu.RUnlock()
	if ok {
		return stats
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if stats, ok = t.fields[fieldID]; !ok {
		stats = storage.NewConcurrentBM25Stats()
		t.fields[fieldID] = stats
	}
	return stats
}

func (t *statsTable) Merge(stats bm25Stats) {
	for fieldID, s := range stats {
		if s != nil {
			t.field(fieldID).Merge(s)
		}
	}
}

func (t *statsTable) Minus(stats bm25Stats) {
	for fieldID, s := range stats {
		if s != nil {
			t.field(fieldID).Minus(s)
		}
	}
}

// MergeExclusive and MinusExclusive are Merge and Minus for callers holding the oracle write lock,
// which keeps every other reader and writer of current out, so the shard locks are skipped.
func (t *statsTable) MergeExclusive(stats bm25Stats) {
	for fieldID, s := range stats {
		if s != nil {
			t.field(fieldID).MergeUnlocked(s)
		}
	}
}

func (t *statsTable) MinusExclusive(stats bm25Stats) {
	for fieldID, s := range stats {
		if s != nil {
			t.field(fieldID).MinusUnlocked(s)
		}
	}
}

func (t *statsTable) GetStats(fieldID int64) (*storage.ConcurrentBM25Stats, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	stats, ok := t.fields[fieldID]
	if !ok {
		return nil, merr.WrapErrFieldNotFound(fieldID, "not in idf oracle BM25 stats")
	}
	return stats, nil
}

func (t *statsTable) NumRow() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, stats := range t.fields {
		return stats.NumRow()
	}
	return 0
}

func (t *statsTable) MemSize() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	size := int64(0)
	for _, stats := range t.fields {
		size += stats.MemSize()
	}
	return size
}

// SyncFunctions keeps the fields of the given BM25 functions, dropping the others and adding missing ones.
func (t *statsTable) SyncFunctions(functions []*schemapb.FunctionSchema) map[int64]struct{} {
	fieldIDs := bm25FunctionFieldIDs(functions)
	t.mu.Lock()
	defer t.mu.Unlock()
	for fieldID := range t.fields {
		if _, ok := fieldIDs[fieldID]; !ok {
			delete(t.fields, fieldID)
		}
	}
	for fieldID := range fieldIDs {
		if _, ok := t.fields[fieldID]; !ok {
			t.fields[fieldID] = storage.NewConcurrentBM25Stats()
		}
	}
	return fieldIDs
}

// MergeTable adds other into t. other must not be modified concurrently.
func (t *statsTable) MergeTable(other *statsTable) {
	other.mu.RLock()
	defer other.mu.RUnlock()
	for fieldID, stats := range other.fields {
		t.field(fieldID).MergeFrom(stats)
	}
}

type idfTarget struct {
	sync.RWMutex
	snapshot *snapshot
	ts       time.Time // time of target generate
}

func (t *idfTarget) SetSnapshot(snapshot *snapshot) {
	t.Lock()
	defer t.Unlock()
	t.snapshot = snapshot
	t.ts = time.Now()
}

func (t *idfTarget) GetSnapshot() (*snapshot, time.Time) {
	t.RLock()
	defer t.RUnlock()
	return t.snapshot, t.ts
}

type idfOracle struct {
	// protect growing segment stats and the target state. current is safe for concurrent use:
	// preloads update it under the read lock, everything else that updates it holds the write lock.
	sync.RWMutex
	current *statsTable
	growing map[int64]*growingBm25Stats

	sealed         typeutil.ConcurrentMap[int64, *sealedBm25Stats]
	sealedDiskSize *atomic.Int64

	channel string

	// for sync distribution
	next          idfTarget
	targetVersion *atomic.Int64
	syncNotify    chan struct{}

	dirPath string

	closeCh chan struct{}
	sf      conc.Singleflight[any]
	wg      sync.WaitGroup

	// resource tracking for caching layer
	resourceMu    sync.Mutex
	chargedMemory int64
	chargedDisk   int64

	// syncMu serializes SyncDistribution, which SetNext and the sync loop can both call.
	syncMu sync.Mutex
	// schemaEpoch changes whenever SyncFunctions changes the BM25 fields; written with the write lock held.
	schemaEpoch *atomic.Uint64
}

// now only used for test
func (o *idfOracle) TargetVersion() int64 {
	return o.targetVersion.Load()
}

func (o *idfOracle) DirPath() string {
	return o.dirPath
}

func (o *idfOracle) preloadSealed(segmentID int64, stats *sealedBm25Stats, memoryStats bm25Stats) {
	// Read lock only, so loads of different segments merge into current in parallel.
	// SyncDistribution sets the target version under the write lock, so the check below
	// and the merge are atomic with respect to it.
	o.RLock()
	defer o.RUnlock()

	// skip preload if first target was loaded.
	if o.targetVersion.Load() != 0 {
		o.sealed.Insert(segmentID, stats)
		return
	}
	o.sealed.Insert(segmentID, stats)
	o.current.Merge(memoryStats)
	stats.activate.Store(true)
}

func (o *idfOracle) activateSealedStatsLocked(segStats *sealedBm25Stats, stats bm25Stats) bool {
	if segStats.activate.Load() {
		return false
	}
	o.current.MergeExclusive(stats)
	segStats.activate.Store(true)
	segStats.version++
	return true
}

func (o *idfOracle) activateExistingSealedStats(segmentID int64, stats bm25Stats) (bool, error) {
	o.Lock()
	defer o.Unlock()

	segStats, existed := o.sealed.Get(segmentID)
	if !existed {
		return false, nil
	}

	segStats.Lock()
	defer segStats.Unlock()
	if segStats.removed {
		return false, merr.WrapErrServiceInternalMsg("sealed bm25 stats for segment %d already removed", segmentID)
	}
	return o.activateSealedStatsLocked(segStats, stats), nil
}

func (o *idfOracle) RegisterGrowing(segmentID int64, stats bm25Stats) {
	clonedStats := stats.Clone()

	o.Lock()
	if _, ok := o.growing[segmentID]; ok {
		o.Unlock()
		return
	}
	o.growing[segmentID] = &growingBm25Stats{
		bm25Stats: clonedStats,
		activate:  true,
	}
	o.current.MergeExclusive(clonedStats)
	o.Unlock()
	o.syncResource()
}

func (o *idfOracle) SyncFunctions(functions []*schemapb.FunctionSchema) error {
	o.Lock()
	o.schemaEpoch.Inc()
	fieldIDs := o.current.SyncFunctions(functions)
	for _, stats := range o.growing {
		stats.RetainFields(fieldIDs)
	}
	removedDiskSize := int64(0)
	o.sealed.Range(func(_ int64, stats *sealedBm25Stats) bool {
		removedDiskSize += stats.RetainFields(fieldIDs)
		return true
	})
	if removedDiskSize > 0 {
		o.sealedDiskSize.Add(-removedDiskSize)
	}
	o.Unlock()
	o.syncResource()
	return nil
}

// LoadSealed loads BM25 stats for a sealed segment from remote storage to local disk.
// Idempotent: skips if segment already loaded.
func (o *idfOracle) LoadSealed(ctx context.Context, segmentID int64, loadInfo *querypb.SegmentLoadInfo, cm storage.ChunkManager) error {
	_, err, _ := o.sf.Do(fmt.Sprintf("load_sealed_%d", segmentID), func() (any, error) {
		if o.sealed.Contain(segmentID) {
			return nil, nil
		}

		logpaths, err := packed.NewStatsResolverFromLoadInfo(loadInfo).BM25StatsPaths()
		if err != nil {
			mlog.Warn(ctx, "load remote segment bm25 stats failed",
				mlog.FieldSegmentID(segmentID),
				mlog.Err(err),
			)
			return nil, err
		}

		if len(logpaths) == 0 {
			return nil, nil
		}

		needParse := o.targetVersion.Load() == 0 && paramtable.Get().QueryNodeCfg.IDFPreload.GetAsBool()

		result, err := o.streamLoad(ctx, segmentID, logpaths, cm, needParse)
		if err != nil {
			// cleanup on failure
			cleanupPath := path.Join(o.dirPath, fmt.Sprintf("%d", segmentID))
			if rmErr := os.RemoveAll(cleanupPath); rmErr != nil {
				mlog.Warn(ctx, "failed to cleanup bm25 stats dir on load failure", mlog.Err(rmErr), mlog.String("path", cleanupPath))
			}
			return nil, err
		}

		segStats := &sealedBm25Stats{
			ts:        time.Now(),
			activate:  atomic.NewBool(false),
			segmentID: segmentID,
			localDir:  result.localDir,
			fieldList: result.fieldList,
			diskSize:  result.diskSize,
		}

		if needParse && result.stats != nil {
			o.preloadSealed(segmentID, segStats, result.stats)
		} else {
			o.sealed.Insert(segmentID, segStats)
		}
		o.sealedDiskSize.Add(result.diskSize)

		o.syncResource()
		return nil, nil
	})
	return err
}

func (o *idfOracle) LoadSealedForReopen(ctx context.Context, segmentID int64, loadInfo *querypb.SegmentLoadInfo, cm storage.ChunkManager, activateIfReadable bool) error {
	// QueryCoord deduplicates same sealed-segment load/reopen tasks by replica, segment, and scope.
	// This shared singleflight key only coalesces duplicate calls; it is not relied on to serialize different tasks.
	_, err, _ := o.sf.Do(fmt.Sprintf("load_sealed_%d", segmentID), func() (any, error) {
		logger := mlog.With(mlog.FieldSegmentID(segmentID))
		logpaths, err := packed.NewStatsResolverFromLoadInfo(loadInfo).BM25StatsPaths()
		if err != nil {
			logger.Warn(ctx, "load remote segment bm25 stats for reopen failed", mlog.Err(err))
			return nil, err
		}
		if len(logpaths) == 0 {
			return nil, nil
		}

		segStats, existedBeforeLoad := o.sealed.Get(segmentID)
		missingPaths := make(map[int64][]string, len(logpaths))
		for fieldID, paths := range logpaths {
			if existedBeforeLoad && segStats.HasField(fieldID) {
				continue
			}
			missingPaths[fieldID] = paths
		}
		if len(missingPaths) == 0 {
			if existedBeforeLoad && activateIfReadable && !segStats.activate.Load() {
				existingStats, err := segStats.FetchStats()
				if err != nil {
					return nil, err
				}
				activated, err := o.activateExistingSealedStats(segmentID, existingStats)
				if err != nil {
					return nil, err
				}
				if activated {
					o.syncResource()
				}
			}
			return nil, nil
		}

		installed := false
		cleanup := func() {
			if installed {
				return
			}
			for fieldID := range missingPaths {
				cleanupPath := path.Join(o.dirPath, fmt.Sprintf("%d", segmentID), fmt.Sprintf("%d", fieldID))
				if rmErr := os.RemoveAll(cleanupPath); rmErr != nil {
					logger.Warn(ctx, "failed to cleanup reopened bm25 stats field dir", mlog.Err(rmErr), mlog.String("path", cleanupPath))
				}
			}
		}
		defer cleanup()

		result, err := o.streamLoad(ctx, segmentID, missingPaths, cm, true)
		if err != nil {
			return nil, err
		}

		var existingStats bm25Stats
		if existedBeforeLoad && activateIfReadable && !segStats.activate.Load() {
			existingStats, err = segStats.FetchStats()
			if err != nil {
				return nil, err
			}
		}

		o.Lock()
		segStats, existed := o.sealed.Get(segmentID)
		if existed {
			segStats.Lock()
			if segStats.removed {
				segStats.Unlock()
				o.Unlock()
				return nil, merr.WrapErrServiceInternalMsg("sealed bm25 stats for segment %d already removed", segmentID)
			}

			wasActive := segStats.activate.Load()
			installedFields := make([]int64, 0, len(result.fieldList))
			installedStats := make(bm25Stats, len(result.fieldList))
			for _, fieldID := range result.fieldList {
				if segStats.hasFieldLocked(fieldID) {
					continue
				}
				installedFields = append(installedFields, fieldID)
				if result.stats != nil {
					installedStats[fieldID] = result.stats[fieldID]
				}
			}
			if len(installedFields) == 0 {
				segStats.Unlock()
				o.Unlock()
				return nil, nil
			}

			segStats.addFieldsLocked(installedFields)
			segStats.diskSize += result.diskSize
			switch {
			case wasActive:
				o.current.MergeExclusive(installedStats)
			case activateIfReadable:
				// Inactive entries have not contributed any field to current, so activation must merge the full segment.
				if existingStats == nil {
					existingStats = make(bm25Stats, len(installedStats))
				}
				existingStats.Merge(installedStats)
				o.activateSealedStatsLocked(segStats, existingStats)
			}
			segStats.Unlock()
		} else {
			segStats = &sealedBm25Stats{
				ts:        time.Now(),
				activate:  atomic.NewBool(false),
				segmentID: segmentID,
				localDir:  result.localDir,
				fieldList: result.fieldList,
				diskSize:  result.diskSize,
			}
			if activateIfReadable {
				o.activateSealedStatsLocked(segStats, result.stats)
			}
			o.sealed.Insert(segmentID, segStats)
		}
		o.sealedDiskSize.Add(result.diskSize)
		installed = true
		o.Unlock()

		o.syncResource()
		return nil, nil
	})
	return err
}

type streamLoadResult struct {
	localDir  string
	fieldList []int64
	stats     bm25Stats // non-nil only when needParse=true
	diskSize  int64
}

func bm25FieldDirDiskSize(fieldDir string) int64 {
	entries, err := os.ReadDir(fieldDir)
	if err != nil {
		return 0
	}

	size := int64(0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			mlog.Warn(context.TODO(), "stat bm25 stats field file failed", mlog.Err(err), mlog.String("path", path.Join(fieldDir, entry.Name())))
			continue
		}
		size += info.Size()
	}
	return size
}

// streamLoad downloads BM25 stats from remote storage to local disk.
// When needParse is true, also parses stats using TeeReader.
func (o *idfOracle) streamLoad(ctx context.Context, segmentID int64, binlogPaths map[int64][]string, cm storage.ChunkManager, needParse bool) (streamLoadResult, error) {
	log := mlog.With(mlog.FieldSegmentID(segmentID))
	startTs := time.Now()

	segDir := path.Join(o.dirPath, fmt.Sprintf("%d", segmentID))
	var totalDiskSize int64
	var stats map[int64]*storage.BM25Stats
	fieldList := make([]int64, 0, len(binlogPaths))

	if needParse {
		stats = make(map[int64]*storage.BM25Stats, len(binlogPaths))
	}

	for fieldID, paths := range binlogPaths {
		fieldList = append(fieldList, fieldID)
		fieldDir := path.Join(segDir, fmt.Sprintf("%d", fieldID))
		if err := os.MkdirAll(fieldDir, os.ModePerm); err != nil {
			return streamLoadResult{}, err
		}

		var fieldStats *storage.BM25Stats
		if needParse {
			fieldStats = storage.NewBM25Stats()
		}

		for i, remotePath := range paths {
			localFile := path.Join(fieldDir, fmt.Sprintf("%d.data", i))
			written, err := streamOneFile(ctx, cm, remotePath, localFile, fieldStats)
			if err != nil {
				return streamLoadResult{}, merr.Wrapf(err, "stream bm25 stats file %s", remotePath)
			}
			totalDiskSize += written
		}

		if needParse {
			stats[fieldID] = fieldStats
			log.Info(ctx, "loaded bm25 stats", mlog.Duration("time", time.Since(startTs)), mlog.Int64("numRow", fieldStats.NumRow()), mlog.FieldFieldID(fieldID))
		}
	}

	log.Info(ctx, "stream load bm25 stats done", mlog.Duration("time", time.Since(startTs)), mlog.Int64("diskSize", totalDiskSize), mlog.Bool("parsed", needParse))

	return streamLoadResult{
		localDir:  segDir,
		fieldList: fieldList,
		stats:     stats,
		diskSize:  totalDiskSize,
	}, nil
}

// streamOneFile streams a single remote file to a local file.
// If parseInto is non-nil, uses TeeReader to simultaneously parse stats.
func streamOneFile(ctx context.Context, cm storage.ChunkManager, remotePath, localPath string, parseInto *storage.BM25Stats) (int64, error) {
	reader, err := cm.Reader(ctx, remotePath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	if parseInto != nil {
		br := bufio.NewReaderSize(reader, paramtable.Get().QueryNodeCfg.IDFReadBufferSize.GetAsInt())
		bw := bufio.NewWriter(f)
		tee := io.TeeReader(br, bw)
		err = parseInto.DeserializeFromReader(tee)
		if err != nil {
			return 0, err
		}
		if err := bw.Flush(); err != nil {
			return 0, err
		}
		if err := f.Sync(); err != nil {
			return 0, err
		}
		info, err := f.Stat()
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}

	written, err := io.Copy(f, reader)
	if err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return written, nil
}

func (o *idfOracle) UpdateGrowing(segmentID int64, stats bm25Stats) {
	if len(stats) == 0 {
		return
	}

	o.Lock()

	old, ok := o.growing[segmentID]
	if !ok {
		o.Unlock()
		return
	}

	old.Merge(stats)
	if old.activate {
		o.current.MergeExclusive(stats)
		o.checkMemoryResource()
	}
	o.Unlock()
}

func (o *idfOracle) LazyRemoveGrowings(targetVersion int64, segmentIDs ...int64) {
	o.Lock()
	defer o.Unlock()

	for _, segmentID := range segmentIDs {
		if stats, ok := o.growing[segmentID]; ok && stats.droppedVersion == 0 {
			stats.droppedVersion = targetVersion
		}
	}
}

// memSize estimates total in-memory size of current + all growing stats.
// Caller must hold RLock or Lock.
func (o *idfOracle) memSize() int64 {
	size := o.current.MemSize()
	for _, g := range o.growing {
		for _, stats := range g.bm25Stats {
			size += stats.MemSize()
		}
	}
	return size
}

// MemorySize returns the estimated in-memory size with RLock protection.
func (o *idfOracle) MemorySize() int64 {
	o.RLock()
	defer o.RUnlock()
	return o.memSize()
}

// diskSize returns total disk size of all sealed segment local files.
func (o *idfOracle) diskSize() int64 {
	return o.sealedDiskSize.Load()
}

// resourceTrackingEnabled reports whether to charge/refund the C++ caching layer.
// When tiered storage eviction is disabled, the caching layer's resource accounting is
// inert (no eviction will be driven by it), so we skip the cgo calls entirely.
func resourceTrackingEnabled() bool {
	return paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool()
}

// syncResource precisely syncs resource usage to the caching layer.
// Used for segment lifecycle events (Register/Unregister/SyncDistribution).
// Caller must NOT hold the RWMutex.
func (o *idfOracle) syncResource() {
	if !resourceTrackingEnabled() {
		return
	}
	actualMem := o.MemorySize()
	actualDisk := o.diskSize()

	o.resourceMu.Lock()
	defer o.resourceMu.Unlock()
	o.doSyncResource(actualMem, actualDisk)
}

// checkMemoryResource checks if memory usage exceeds charged amount.
// Only charges (with headroom), never refunds. Used in Insert path (UpdateGrowing).
// Caller must hold RWMutex.Lock (so memSize is safe to call without RLock).
func (o *idfOracle) checkMemoryResource() {
	if !resourceTrackingEnabled() {
		return
	}
	actualMem := o.memSize()

	o.resourceMu.Lock()
	defer o.resourceMu.Unlock()

	if actualMem > o.chargedMemory {
		charge := actualMem + memoryHeadroom - o.chargedMemory
		C.ChargeLoadedResource(C.CResourceUsage{
			memory_bytes: C.int64_t(charge),
			disk_bytes:   0,
		})
		o.chargedMemory = actualMem + memoryHeadroom
	}
}

// doSyncResource performs the actual Charge/Refund. Caller must hold resourceMu.
func (o *idfOracle) doSyncResource(actualMem, actualDisk int64) {
	memDelta := actualMem - o.chargedMemory
	diskDelta := actualDisk - o.chargedDisk

	if memDelta > 0 || diskDelta > 0 {
		C.ChargeLoadedResource(C.CResourceUsage{
			memory_bytes: C.int64_t(max(memDelta, 0)),
			disk_bytes:   C.int64_t(max(diskDelta, 0)),
		})
	}
	if memDelta < 0 || diskDelta < 0 {
		C.RefundLoadedResource(C.CResourceUsage{
			memory_bytes: C.int64_t(max(-memDelta, 0)),
			disk_bytes:   C.int64_t(max(-diskDelta, 0)),
		})
	}

	o.chargedMemory = actualMem
	o.chargedDisk = actualDisk
}

func (o *idfOracle) Start() {
	o.wg.Add(1)
	go o.syncloop()
}

func (o *idfOracle) Close() {
	close(o.closeCh)
	o.wg.Wait()

	// Refund all charged resources
	o.resourceMu.Lock()
	if o.chargedMemory > 0 || o.chargedDisk > 0 {
		C.RefundLoadedResource(C.CResourceUsage{
			memory_bytes: C.int64_t(o.chargedMemory),
			disk_bytes:   C.int64_t(o.chargedDisk),
		})
		o.chargedMemory = 0
		o.chargedDisk = 0
	}
	o.resourceMu.Unlock()

	if err := os.RemoveAll(o.dirPath); err != nil {
		mlog.Warn(context.TODO(), "failed to remove bm25 stats dir on close", mlog.Err(err), mlog.String("path", o.dirPath))
	}
}

func (o *idfOracle) SetNext(snapshot *snapshot) {
	o.next.SetSnapshot(snapshot)

	// sync SyncDistibution when first load target
	if o.targetVersion.Load() == 0 {
		o.SyncDistribution()
	} else {
		o.NotifySync()
	}
}

func (o *idfOracle) NotifySync() {
	select {
	case o.syncNotify <- struct{}{}:
	default:
	}
}

func (o *idfOracle) syncloop() {
	defer o.wg.Done()
	for {
		select {
		case <-o.syncNotify:
			err := o.SyncDistribution()
			if err != nil {
				mlog.Warn(context.TODO(), "idf oracle sync distribution failed", mlog.Err(err))
				time.Sleep(time.Second * 10)
				o.NotifySync()
			}
		case <-o.closeCh:
			return
		}
	}
}

// SyncDistribution syncs current to the latest target: it activates the sealed segments that joined
// the target and deactivates the ones that left it.
//
// The stats of those segments are read from disk outside the oracle lock and folded into one diff,
// so this does not keep a copy of every segment's stats. A reopen or SyncFunctions may change a
// segment while its stats are read; each segment's version tells which ones changed, and only
// those are re-read under the lock to correct the diff before it is applied.
func (o *idfOracle) SyncDistribution() error {
	o.syncMu.Lock()
	defer o.syncMu.Unlock()

	for attempt := 1; ; attempt++ {
		retry, err := o.syncDistributionOnce()
		if err != nil || !retry {
			return err
		}
		if attempt >= idfSyncMaxAttempts {
			return merr.WrapErrServiceInternalMsg("idf oracle sync distribution conflicted with BM25 field changes %d times", attempt)
		}
	}
}

// statsCandidate is a sealed segment to activate (or deactivate, if minus) in this sync.
type statsCandidate struct {
	seg   *sealedBm25Stats
	minus bool

	// what was folded into the diff outside the lock: the version and fields the stats were read at
	version uint64
	fields  []int64

	// decided under the lock: whether the activation change still applies
	apply bool
}

// syncDistributionOnce runs one sync. It returns retry when the BM25 fields changed while stats were
// read, in which case nothing has been changed and the sync must start over.
func (o *idfOracle) syncDistributionOnce() (retry bool, err error) {
	snapshot, snapshotTs := o.next.GetSnapshot()
	if snapshot.targetVersion <= o.targetVersion.Load() {
		return false, nil
	}

	sealed, _ := snapshot.Peek()

	// intarget segment map
	targetMap := typeutil.NewSet[UniqueID]()
	// segment with unreadable target version was not been used,
	// not remove them till it update version or remove from snapshot(released)
	reserveMap := typeutil.NewSet[UniqueID]()

	for _, item := range sealed {
		for _, segment := range item.Segments {
			if segment.Level == datapb.SegmentLevel_L0 {
				continue
			}

			switch segment.TargetVersion {
			case snapshot.targetVersion:
				targetMap.Insert(segment.SegmentID)
				if !o.sealed.Contain(segment.SegmentID) {
					mlog.Warn(context.TODO(), "idf oracle lack some sealed segment", mlog.Int64("segment", segment.SegmentID))
				}
			case unreadableTargetVersion:
				reserveMap.Insert(segment.SegmentID)
			}
		}
	}

	schemaEpoch := o.schemaEpoch.Load()
	candidates := make(map[int64]*statsCandidate)
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		intarget := targetMap.Contain(segmentID)

		activate := stats.activate.Load()
		// activate segment if segment in target
		if intarget && !activate {
			candidates[segmentID] = &statsCandidate{seg: stats}
		} else
		// deactivate segment if segment not in target.
		if !intarget && activate {
			candidates[segmentID] = &statsCandidate{seg: stats, minus: true}
		}
		return true
	})

	diff, err := fetchStatsDiff(candidates)
	if err != nil {
		return false, err
	}

	if syncDistributionAfterFetchHook != nil {
		syncDistributionAfterFetchHook()
	}

	o.Lock()
	// fields dropped or re-added by SyncFunctions cannot be corrected from disk, start over
	if o.schemaEpoch.Load() != schemaEpoch {
		o.Unlock()
		return true, nil
	}

	// Settle every segment against its state now, correcting the diff where it changed since its stats
	// were read. Nothing is changed until all corrections succeed, so an error leaves the oracle as it was.
	settled := 0
	toDeactivate := make([]*sealedBm25Stats, 0)
	var settleErr error
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		stats.RLock()
		defer stats.RUnlock()

		c, ok := candidates[segmentID]
		if !ok {
			// A segment activated after the unlocked read (by a reopen) that this sync removes would
			// leave its stats in current forever, so deactivate it here.
			intarget := targetMap.Contain(segmentID)
			remove := !intarget && !reserveMap.Contain(segmentID) && stats.ts.Before(snapshotTs)
			if remove && stats.activate.Load() {
				contributed, err := stats.fetchFieldsLocked(stats.fieldList)
				if err != nil {
					settleErr = err
					return false
				}
				diff.Minus(contributed)
				toDeactivate = append(toDeactivate, stats)
			}
			return true
		}
		if c.seg != stats {
			settleErr = merr.WrapErrServiceInternalMsg("sealed bm25 stats for segment %d replaced during sync", segmentID)
			return false
		}
		settled++

		// the change still applies if the segment's activation is what it was when picked
		c.apply = stats.activate.Load() == c.minus
		changed := stats.version != c.version
		if !c.apply || changed {
			// take back what was folded in for this segment; its fields read then are unchanged on disk
			// (reopen only adds fields, and dropping fields restarts the sync)
			taken, err := stats.fetchFieldsLocked(c.fields)
			if err != nil {
				settleErr = err
				return false
			}
			if c.minus {
				diff.Merge(taken)
			} else {
				diff.Minus(taken)
			}
		}
		if c.apply && changed {
			latest, err := stats.fetchFieldsLocked(stats.fieldList)
			if err != nil {
				settleErr = err
				return false
			}
			if c.minus {
				diff.Minus(latest)
			} else {
				diff.Merge(latest)
			}
		}
		return true
	})
	if settleErr == nil && settled != len(candidates) {
		settleErr = merr.WrapErrServiceInternalMsg("%d sealed bm25 stats removed during sync", len(candidates)-settled)
	}
	if settleErr != nil {
		o.Unlock()
		return false, merr.Wrap(settleErr, "settle sealed bm25 stats failed")
	}

	for segmentID, stats := range o.growing {
		// drop growing segment bm25 stats
		if stats.droppedVersion != 0 && stats.droppedVersion <= snapshot.targetVersion {
			if stats.activate {
				o.current.MinusExclusive(stats.bm25Stats)
			}
			delete(o.growing, segmentID)
		}
	}

	for _, c := range candidates {
		if c.apply {
			c.seg.Lock()
			c.seg.activate.Store(!c.minus)
			c.seg.version++
			c.seg.Unlock()
		}
	}
	for _, stats := range toDeactivate {
		stats.Lock()
		stats.activate.Store(false)
		stats.version++
		stats.Unlock()
	}

	// remove sealed segment not in target
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		reserve := reserveMap.Contain(segmentID)
		intarget := targetMap.Contain(segmentID)

		// remove
		// if segment not in target and not in reserve list
		// (means segment target version was old version or segment not in snapshot)
		// and add before snapshot Ts
		// (forbid remove some new segment register after current snapshot)
		stats.RLock()
		remove := !intarget && !reserve && stats.ts.Before(snapshotTs)
		diskSize := stats.diskSize
		stats.RUnlock()
		if remove {
			o.sealedDiskSize.Add(-diskSize)
			stats.Remove()
			o.sealed.Remove(segmentID)
		}
		return true
	})

	// still under the write lock, so no reader sees current between the activation changes and this merge
	o.current.MergeTable(diff)

	o.targetVersion.Store(snapshot.targetVersion)
	numRow := o.current.NumRow()
	growingLen := len(o.growing)
	sealedLen := o.sealed.Len()
	o.Unlock()

	o.syncResource()
	mlog.Info(context.TODO(), "sync idf distribution finished", mlog.Int64("version", snapshot.targetVersion), mlog.Int64("numrow", numRow), mlog.Int("growing", growingLen), mlog.Int("sealed", sealedLen))
	return false, nil
}

// fetchStatsDiff reads the stats of all candidates from local disk on up to idfSyncParallelism goroutines,
// adding the ones to activate and subtracting the ones to deactivate into one diff, and records the
// version and fields each candidate was read at.
func fetchStatsDiff(candidates map[int64]*statsCandidate) (*statsTable, error) {
	diff := newStatsTable(nil)
	all := make([]*statsCandidate, 0, len(candidates))
	for _, c := range candidates {
		all = append(all, c)
	}
	workers := min(idfSyncParallelism, len(all))
	errs := make([]error, workers)
	next := atomic.NewInt64(-1)
	failed := atomic.NewBool(false)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for !failed.Load() {
				i := int(next.Inc())
				if i >= len(all) {
					return
				}
				c := all[i]
				stats, version, fields, err := c.seg.fetchSnapshot()
				if err != nil {
					errs[w] = merr.Wrap(err, "fetch stats failed")
					failed.Store(true)
					return
				}
				c.version, c.fields = version, fields
				if c.minus {
					diff.Minus(stats)
				} else {
					diff.Merge(stats)
				}
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return diff, nil
}

func (o *idfOracle) BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	o.RLock()
	defer o.RUnlock()

	stats, err := o.current.GetStats(fieldID)
	if err != nil {
		return nil, 0, err
	}

	// Preloads merge into current under the read lock only before the first target is synced, and the
	// target version changes only under the write lock. Past that point every writer holds the write
	// lock, so the shard locks can be skipped on this per-search path.
	buildIDF := stats.BuildIDFUnlocked
	if o.targetVersion.Load() == 0 {
		buildIDF = stats.BuildIDF
	}
	idfBytes := make([][]byte, len(tfs.GetContents()))
	for i, tf := range tfs.GetContents() {
		idfBytes[i] = buildIDF(tf)
	}
	return idfBytes, stats.GetAvgdl(), nil
}

func NewIDFOracle(channel string, functions []*schemapb.FunctionSchema) IDFOracle {
	return &idfOracle{
		channel:        channel,
		targetVersion:  atomic.NewInt64(0),
		current:        newStatsTable(functions),
		growing:        make(map[int64]*growingBm25Stats),
		sealed:         typeutil.ConcurrentMap[int64, *sealedBm25Stats]{},
		sealedDiskSize: atomic.NewInt64(0),
		dirPath:        path.Join(pathutil.GetPath(pathutil.BM25Path, paramtable.GetNodeID()), channel),
		syncNotify:     make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		sf:             conc.Singleflight[any]{},
		schemaEpoch:    atomic.NewUint64(0),
	}
}
