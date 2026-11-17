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

	// version changes whenever activation or the field list changes.
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

// fetchFieldsLocked reads the given fields while the caller holds the entry lock.
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
	sync.RWMutex // protect current and growing segment stats
	current      bm25Stats
	growing      map[int64]bm25Stats

	// Record removal intent without waiting for the oracle lock held by stats merges.
	growingDropVersions typeutil.ConcurrentMap[int64, int64]

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

	// Serialize target syncs through physical cleanup outside the oracle lock.
	syncMu sync.Mutex
}

// now only used for test
func (o *idfOracle) TargetVersion() int64 {
	return o.targetVersion.Load()
}

func (o *idfOracle) DirPath() string {
	return o.dirPath
}

func (o *idfOracle) registerSealedStats(segmentID int64, stats *sealedBm25Stats, memoryStats bm25Stats) {
	// Keep registration, disk accounting and optional preload atomic with target sync.
	// The 3.0 aggregate needs the write lock for merges.
	o.Lock()
	defer o.Unlock()

	// Publish the disk charge with the entry: sync may detach and remove it as
	// soon as the oracle lock is released, without waiting for the loader.
	o.sealedDiskSize.Add(stats.diskSize)
	// Skip preload if disabled or the first target was already loaded.
	if memoryStats == nil || o.targetVersion.Load() != 0 {
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
	o.current.Merge(stats)
	segStats.activate.Store(true)
	segStats.version++
	return true
}

// errReopenStateChanged tells LoadSealedForReopen's caller to retry: the segment changed between what
// the reopen read outside the oracle lock and what it found under it.
func errReopenStateChanged(segmentID int64) error {
	return merr.WrapErrServiceUnavailableMsg("sealed bm25 stats for segment %d changed during reopen", segmentID)
}

// reopenView is what LoadSealedForReopen saw of a segment outside the oracle lock and decided from.
type reopenView struct {
	seg     *sealedBm25Stats // nil if the segment had no entry
	version uint64
	active  bool
	// the stats of every field the segment had, read only when the reopen activates an inactive segment
	existing bm25Stats
}

// observeForReopen reads the segment's state, and its existing stats if the reopen is going to activate it.
func observeForReopen(seg *sealedBm25Stats, activateIfReadable bool) (reopenView, error) {
	if seg == nil {
		return reopenView{}, nil
	}
	seg.RLock()
	defer seg.RUnlock()
	if seg.removed {
		return reopenView{}, errReopenStateChanged(seg.segmentID)
	}
	view := reopenView{seg: seg, version: seg.version, active: seg.activate.Load()}
	if activateIfReadable && !view.active {
		existing, err := seg.fetchFieldsLocked(seg.fieldList)
		if err != nil {
			return reopenView{}, err
		}
		view.existing = existing
	}
	return view, nil
}

// stillHolds reports whether the segment's entry is the same and unchanged since view was taken.
// Caller holds the oracle write lock and seg's lock if seg is not nil.
func (v reopenView) stillHolds(seg *sealedBm25Stats, existed bool) bool {
	if v.seg == nil {
		return !existed
	}
	return existed && seg == v.seg && !seg.removed && seg.version == v.version
}

func (o *idfOracle) activateExistingSealedStats(segmentID int64, view reopenView) (bool, error) {
	o.Lock()
	defer o.Unlock()

	segStats, existed := o.sealed.Get(segmentID)
	if !existed {
		return false, errReopenStateChanged(segmentID)
	}

	segStats.Lock()
	defer segStats.Unlock()
	if segStats.removed {
		return false, errReopenStateChanged(segmentID)
	}
	if !view.stillHolds(segStats, existed) {
		return false, errReopenStateChanged(segmentID)
	}
	return o.activateSealedStatsLocked(segStats, view.existing), nil
}

func (o *idfOracle) RegisterGrowing(segmentID int64, stats bm25Stats) {
	clonedStats := stats.Clone()

	o.Lock()
	if _, ok := o.growing[segmentID]; ok {
		o.Unlock()
		return
	}
	o.growing[segmentID] = clonedStats
	o.current.Merge(clonedStats)
	o.Unlock()
	o.syncResource()
}

func (o *idfOracle) SyncFunctions(functions []*schemapb.FunctionSchema) error {
	o.Lock()
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
		if _, loaded := o.sealed.Get(segmentID); loaded {
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

		o.registerSealedStats(segmentID, segStats, result.stats)

		o.syncResource()
		return nil, nil
	})
	return err
}

// reopenBeforeLockHook runs in tests after LoadSealedForReopen has decided outside the oracle lock and before it takes it.
var reopenBeforeLockHook func()

// LoadSealedForReopen installs the BM25 fields a reopened segment does not have yet, and activates the
// segment if activateIfReadable is set and it is inactive. What to do is decided from the segment's state
// read outside the oracle lock; if the segment changes before the lock is taken (a target sync activates
// or deactivates it, SyncFunctions drops a field, or it is removed), it returns a retryable error and
// changes nothing, so the caller's retry decides again from the new state.
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
				view, err := observeForReopen(segStats, true)
				if err != nil {
					return nil, err
				}
				if reopenBeforeLockHook != nil {
					reopenBeforeLockHook()
				}
				activated, err := o.activateExistingSealedStats(segmentID, view)
				if err != nil {
					return nil, err
				}
				if activated {
					o.syncResource()
				}
			}
			return nil, nil
		}

		result, err := o.streamLoad(ctx, segmentID, missingPaths, cm, true)
		if err != nil {
			return nil, err
		}
		installed := false
		defer func() {
			// An existing entry adopts the fields, not the staging directory.
			// A new entry takes ownership of the whole directory on success.
			if !installed || existedBeforeLoad {
				if err := os.RemoveAll(result.localDir); err != nil {
					logger.Warn(ctx, "failed to cleanup reopened bm25 stats staging dir", mlog.Err(err), mlog.String("path", result.localDir))
				}
			}
		}()

		var observed *sealedBm25Stats
		if existedBeforeLoad {
			observed = segStats
		}
		view, err := observeForReopen(observed, activateIfReadable)
		if err != nil {
			return nil, err
		}
		if reopenBeforeLockHook != nil {
			reopenBeforeLockHook()
		}

		o.Lock()
		segStats, existed := o.sealed.Get(segmentID)
		if existed {
			segStats.Lock()
		}
		unlock := func() {
			if existed {
				segStats.Unlock()
			}
			o.Unlock()
		}
		if existed && segStats.removed {
			unlock()
			return nil, errReopenStateChanged(segmentID)
		}
		if !view.stillHolds(segStats, existed) {
			unlock()
			return nil, errReopenStateChanged(segmentID)
		}

		if existed {
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
				unlock()
				return nil, nil
			}

			if err := installReopenedFields(ctx, segStats.localDir, result.localDir, installedFields); err != nil {
				unlock()
				return nil, err
			}
			segStats.addFieldsLocked(installedFields)
			segStats.diskSize += result.diskSize
			switch {
			case view.active:
				o.current.Merge(installedStats)
			case activateIfReadable:
				// Inactive entries have not contributed any field to current, so activation must merge the
				// full segment: the existing fields read with the view, which are unchanged since, and the new ones.
				full := make(bm25Stats, len(view.existing)+len(installedStats))
				full.Merge(view.existing)
				full.Merge(installedStats)
				o.activateSealedStatsLocked(segStats, full)
			}
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
		unlock()

		o.syncResource()
		return nil, nil
	})
	return err
}

// Install only unpublished fields, under the oracle and entry write locks.
func installReopenedFields(ctx context.Context, localDir, stagingDir string, fieldIDs []int64) error {
	moved := make([]string, 0, len(fieldIDs))
	installed := false
	defer func() {
		if installed {
			return
		}
		for _, dir := range moved {
			if err := os.RemoveAll(dir); err != nil {
				mlog.Warn(ctx, "failed to rollback reopened bm25 stats field", mlog.Err(err), mlog.String("path", dir))
			}
		}
	}()
	for _, fieldID := range fieldIDs {
		name := fmt.Sprintf("%d", fieldID)
		dst := path.Join(localDir, name)
		// Also recover an unpublished destination left by a failed rollback.
		if err := os.RemoveAll(dst); err != nil {
			return merr.WrapErrIoFailed(dst, err)
		}
		if err := os.Rename(path.Join(stagingDir, name), dst); err != nil {
			return merr.WrapErrIoFailed(dst, err)
		}
		moved = append(moved, dst)
	}
	installed = true
	return nil
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

	// Both fresh entries and reopen attempts own a unique directory. Cleanup
	// of a retired entry must never touch a replacement or an in-flight download.
	if err := os.MkdirAll(o.dirPath, os.ModePerm); err != nil {
		return streamLoadResult{}, merr.WrapErrIoFailed(o.dirPath, err)
	}
	segDir, err := os.MkdirTemp(o.dirPath, fmt.Sprintf("%d-", segmentID))
	if err != nil {
		return streamLoadResult{}, merr.WrapErrIoFailed(o.dirPath, err)
	}
	loaded := false
	defer func() {
		if !loaded {
			if err := os.RemoveAll(segDir); err != nil {
				log.Warn(ctx, "failed to cleanup bm25 stats download", mlog.Err(err), mlog.String("path", segDir))
			}
		}
	}()
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

	loaded = true
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
	o.current.Merge(stats)
	o.checkMemoryResource()
	o.Unlock()
}

func (o *idfOracle) LazyRemoveGrowings(targetVersion int64, segmentIDs ...int64) {
	if targetVersion <= o.targetVersion.Load() {
		return
	}

	for _, segmentID := range segmentIDs {
		o.growingDropVersions.GetOrInsert(segmentID, targetVersion)
	}
}

// Caller holds the oracle write lock so pending removals use the latest growing stats.
func (o *idfOracle) removeDroppedGrowings(targetVersion int64) {
	o.growingDropVersions.Range(func(segmentID, droppedVersion int64) bool {
		if droppedVersion <= targetVersion {
			if stats, ok := o.growing[segmentID]; ok {
				o.current.Minus(stats)
				delete(o.growing, segmentID)
			}
			o.growingDropVersions.Remove(segmentID)
		}
		return true
	})
}

// memSize estimates total in-memory size of current + all growing stats.
// Caller must hold RLock or Lock.
func (o *idfOracle) memSize() int64 {
	size := int64(0)
	for _, stats := range o.current {
		size += stats.MemSize()
	}
	for _, g := range o.growing {
		for _, stats := range g {
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

// SyncDistribution sync current target to idf oracle.
func (o *idfOracle) SyncDistribution() error {
	o.syncMu.Lock()
	defer o.syncMu.Unlock()
	snapshot, snapshotTs := o.next.GetSnapshot()
	if snapshot.targetVersion <= o.targetVersion.Load() {
		return nil
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

	activateStats := make(map[int64]bm25Stats)
	deactivateStats := make(map[int64]bm25Stats)

	var rangeErr error
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		intarget := targetMap.Contain(segmentID)

		activate := stats.activate.Load()
		// activate segment if segment in target
		if intarget && !activate {
			stats, err := stats.FetchStats()
			if err != nil {
				rangeErr = merr.Wrap(err, "fetch stats failed")
				return false
			}
			activateStats[segmentID] = stats
		} else
		// deactivate segment if segment not in target.
		if !intarget && activate {
			stats, err := stats.FetchStats()
			if err != nil {
				rangeErr = merr.Wrap(err, "fetch stats failed")
				return false
			}
			deactivateStats[segmentID] = stats
		}
		return true
	})

	if rangeErr != nil {
		return rangeErr
	}

	o.Lock()

	o.removeDroppedGrowings(snapshot.targetVersion)

	// Detach obsolete entries under the oracle lock; remove their files after publication.
	toRemove := make([]*sealedBm25Stats, 0)
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		intarget := targetMap.Contain(segmentID)
		activate := stats.activate.Load()
		if intarget && !activate {
			if segmentStats, ok := activateStats[segmentID]; ok {
				stats.Lock()
				o.activateSealedStatsLocked(stats, segmentStats)
				stats.Unlock()
			}
		}
		if !intarget && activate {
			if segmentStats, ok := deactivateStats[segmentID]; ok {
				stats.Lock()
				o.current.Minus(segmentStats)
				stats.activate.Store(false)
				stats.version++
				stats.Unlock()
			}
		}

		stats.RLock()
		// Preserve unreadable entries and loads registered after this snapshot.
		remove := !intarget && !reserveMap.Contain(segmentID) && stats.ts.Before(snapshotTs)
		stats.RUnlock()
		if remove {
			o.sealed.Remove(segmentID)
			toRemove = append(toRemove, stats)
		}
		return true
	})

	o.targetVersion.Store(snapshot.targetVersion)
	numRow := o.current.NumRow()
	growingLen := len(o.growing)
	sealedLen := o.sealed.Len()
	o.Unlock()

	// Each detached entry owns its directory; replacement downloads are independent.
	for _, stats := range toRemove {
		stats.Remove()
		o.sealedDiskSize.Add(-stats.diskSize)
	}
	o.syncResource()
	mlog.Info(context.TODO(), "sync idf distribution finished", mlog.Int64("version", snapshot.targetVersion), mlog.Int64("numrow", numRow), mlog.Int("growing", growingLen), mlog.Int("sealed", sealedLen))
	return nil
}

func (o *idfOracle) BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	o.RLock()
	defer o.RUnlock()

	stats, err := o.current.GetStats(fieldID)
	if err != nil {
		return nil, 0, err
	}

	idfBytes := make([][]byte, len(tfs.GetContents()))
	for i, tf := range tfs.GetContents() {
		idf := stats.BuildIDF(tf)
		idfBytes[i] = idf
	}
	return idfBytes, stats.GetAvgdl(), nil
}

func NewIDFOracle(channel string, functions []*schemapb.FunctionSchema) IDFOracle {
	return &idfOracle{
		channel:        channel,
		targetVersion:  atomic.NewInt64(0),
		current:        newBm25Stats(functions),
		growing:        make(map[int64]bm25Stats),
		sealed:         typeutil.ConcurrentMap[int64, *sealedBm25Stats]{},
		sealedDiskSize: atomic.NewInt64(0),
		dirPath:        path.Join(pathutil.GetPath(pathutil.BM25Path, paramtable.GetNodeID()), channel),
		syncNotify:     make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		sf:             conc.Singleflight[any]{},
	}
}
