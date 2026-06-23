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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
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

	removed         bool
	segmentID       int64
	ts              time.Time // Time of segment register
	localDir        string
	remoteFetchOnly bool
	remotePaths     map[int64][]string
	cm              storage.ChunkManager
	fieldList       []int64 // bm25 field list
	diskSize        int64   // total disk size of local files
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
	}
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
			log.Warn("remove local bm25 stats failed", zap.Error(err), zap.String("path", s.localDir))
		}
	}
}

// FetchStats reads stats from the configured source and merges per field.
// Local directory structure: {localDir}/{fieldID}/0.data, 1.data, ...
func (s *sealedBm25Stats) FetchStats() (map[int64]*storage.BM25Stats, error) {
	s.RLock()
	defer s.RUnlock()

	if s.removed {
		return nil, merr.WrapErrServiceInternalMsg("sealed bm25 stats for segment %d already removed", s.segmentID)
	}

	if s.remoteFetchOnly {
		return s.fetchRemoteStats()
	}
	return s.fetchLocalStats()
}

func (s *sealedBm25Stats) fetchLocalStats() (map[int64]*storage.BM25Stats, error) {
	stats := make(map[int64]*storage.BM25Stats)
	for _, fieldID := range s.fieldList {
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

func (s *sealedBm25Stats) fetchRemoteStats() (map[int64]*storage.BM25Stats, error) {
	if s.cm == nil {
		return nil, errors.Newf("remote chunk manager is nil for segment %d", s.segmentID)
	}

	stats := make(map[int64]*storage.BM25Stats, len(s.remotePaths))
	for _, fieldID := range s.fieldList {
		fieldStats := storage.NewBM25Stats()
		for _, remotePath := range s.remotePaths[fieldID] {
			if err := readRemoteBM25Stats(context.Background(), s.cm, remotePath, fieldStats); err != nil {
				return nil, errors.Wrapf(err, "read remote bm25 stats %s", remotePath)
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

func (s bm25Stats) AddMissingFunctions(functions []*schemapb.FunctionSchema) {
	for _, function := range functions {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		fieldID := function.GetOutputFieldIds()[0]
		if _, ok := s[fieldID]; !ok {
			s[fieldID] = storage.NewBM25Stats()
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
	growing      map[int64]*growingBm25Stats

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
}

// now only used for test
func (o *idfOracle) TargetVersion() int64 {
	return o.targetVersion.Load()
}

func (o *idfOracle) DirPath() string {
	return o.dirPath
}

func (o *idfOracle) preloadSealed(segmentID int64, stats *sealedBm25Stats, memoryStats bm25Stats) {
	o.Lock()
	defer o.Unlock()

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
	o.current.Merge(stats)
	segStats.activate.Store(true)
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
	o.current.Merge(clonedStats)
	o.Unlock()
	o.syncResource()
}

func (o *idfOracle) SyncFunctions(functions []*schemapb.FunctionSchema) error {
	o.Lock()
	o.current.AddMissingFunctions(functions)
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
			log.Warn("load remote segment bm25 stats failed",
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return nil, err
		}

		if len(logpaths) == 0 {
			return nil, nil
		}

		needParse := o.targetVersion.Load() == 0 && paramtable.Get().QueryNodeCfg.IDFPreload.GetAsBool()

		remoteFetchOnly := paramtable.Get().QueryNodeCfg.IDFRemoteFetchOnly.GetAsBool()
		result, err := o.streamLoad(ctx, segmentID, logpaths, cm, needParse, remoteFetchOnly)
		if err != nil {
			// cleanup on failure
			cleanupPath := path.Join(o.dirPath, fmt.Sprintf("%d", segmentID))
			if rmErr := os.RemoveAll(cleanupPath); rmErr != nil {
				log.Warn("failed to cleanup bm25 stats dir on load failure", zap.Error(rmErr), zap.String("path", cleanupPath))
			}
			return nil, err
		}

		segStats := &sealedBm25Stats{
			ts:              time.Now(),
			activate:        atomic.NewBool(false),
			segmentID:       segmentID,
			localDir:        result.localDir,
			remoteFetchOnly: result.remoteFetchOnly,
			remotePaths:     result.remotePaths,
			cm:              result.cm,
			fieldList:       result.fieldList,
			diskSize:        result.diskSize,
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
		logger := log.Ctx(ctx).With(zap.Int64("segmentID", segmentID))
		logpaths, err := packed.NewStatsResolverFromLoadInfo(loadInfo).BM25StatsPaths()
		if err != nil {
			logger.Warn("load remote segment bm25 stats for reopen failed", zap.Error(err))
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
					logger.Warn("failed to cleanup reopened bm25 stats field dir", zap.Error(rmErr), zap.String("path", cleanupPath))
				}
			}
		}
		defer cleanup()

		remoteFetchOnly := paramtable.Get().QueryNodeCfg.IDFRemoteFetchOnly.GetAsBool()
		if existedBeforeLoad {
			segStats.RLock()
			remoteFetchOnly = segStats.remoteFetchOnly
			segStats.RUnlock()
		}

		result, err := o.streamLoad(ctx, segmentID, missingPaths, cm, true, remoteFetchOnly)
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
			if result.remoteFetchOnly {
				if segStats.remotePaths == nil {
					segStats.remotePaths = make(map[int64][]string, len(installedFields))
				}
				for _, fieldID := range installedFields {
					segStats.remotePaths[fieldID] = result.remotePaths[fieldID]
				}
				segStats.cm = result.cm
			}
			segStats.diskSize += result.diskSize
			switch {
			case wasActive:
				o.current.Merge(installedStats)
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
				ts:              time.Now(),
				activate:        atomic.NewBool(false),
				segmentID:       segmentID,
				localDir:        result.localDir,
				remoteFetchOnly: result.remoteFetchOnly,
				remotePaths:     result.remotePaths,
				cm:              result.cm,
				fieldList:       result.fieldList,
				diskSize:        result.diskSize,
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
	localDir        string
	remoteFetchOnly bool
	remotePaths     map[int64][]string
	cm              storage.ChunkManager
	fieldList       []int64
	stats           bm25Stats // non-nil only when needParse=true
	diskSize        int64
}

// streamLoad downloads BM25 stats from remote storage to local disk.
// When needParse is true, also parses stats using TeeReader.
func (o *idfOracle) streamLoad(ctx context.Context, segmentID int64, binlogPaths map[int64][]string, cm storage.ChunkManager, needParse bool, remoteFetchOnly bool) (streamLoadResult, error) {
	log := log.Ctx(ctx).With(zap.Int64("segmentID", segmentID))
	startTs := time.Now()

	segDir := path.Join(o.dirPath, fmt.Sprintf("%d", segmentID))
	var totalDiskSize int64
	var stats map[int64]*storage.BM25Stats
	fieldList := make([]int64, 0, len(binlogPaths))

	if remoteFetchOnly {
		if needParse {
			stats = make(map[int64]*storage.BM25Stats, len(binlogPaths))
			for fieldID, paths := range binlogPaths {
				fieldList = append(fieldList, fieldID)
				fieldStats := storage.NewBM25Stats()
				for _, remotePath := range paths {
					if err := readRemoteBM25Stats(ctx, cm, remotePath, fieldStats); err != nil {
						return streamLoadResult{}, errors.Wrapf(err, "read remote bm25 stats %s", remotePath)
					}
				}
				stats[fieldID] = fieldStats
				log.Info("loaded remote bm25 stats", zap.Duration("time", time.Since(startTs)), zap.Int64("numRow", fieldStats.NumRow()), zap.Int64("fieldID", fieldID))
			}
		} else {
			for fieldID := range binlogPaths {
				fieldList = append(fieldList, fieldID)
			}
		}

		log.Info("stream load bm25 stats done", zap.Duration("time", time.Since(startTs)), zap.Int64("diskSize", 0), zap.Bool("parsed", needParse), zap.Bool("remoteFetchOnly", true))
		return streamLoadResult{
			remoteFetchOnly: true,
			remotePaths:     binlogPaths,
			cm:              cm,
			fieldList:       fieldList,
			stats:           stats,
		}, nil
	}

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
			log.Info("loaded bm25 stats", zap.Duration("time", time.Since(startTs)), zap.Int64("numRow", fieldStats.NumRow()), zap.Int64("fieldID", fieldID))
		}
	}

	log.Info("stream load bm25 stats done", zap.Duration("time", time.Since(startTs)), zap.Int64("diskSize", totalDiskSize), zap.Bool("parsed", needParse))

	return streamLoadResult{
		localDir:  segDir,
		fieldList: fieldList,
		stats:     stats,
		diskSize:  totalDiskSize,
	}, nil
}

func readRemoteBM25Stats(ctx context.Context, cm storage.ChunkManager, remotePath string, parseInto *storage.BM25Stats) error {
	reader, err := cm.Reader(ctx, remotePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	br := bufio.NewReaderSize(reader, paramtable.Get().QueryNodeCfg.IDFReadBufferSize.GetAsInt())
	return parseInto.DeserializeFromReader(br)
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
		o.current.Merge(stats)
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
	size := int64(0)
	for _, stats := range o.current {
		size += stats.MemSize()
	}
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
		log.Warn("failed to remove bm25 stats dir on close", zap.Error(err), zap.String("path", o.dirPath))
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
				log.Warn("idf oracle sync distribution failed", zap.Error(err))
				time.Sleep(time.Second * 10)
				o.NotifySync()
			}
		case <-o.closeCh:
			return
		}
	}
}

// WARN: SyncDistribution not concurrent safe.
// SyncDistribution sync current target to idf oracle.
func (o *idfOracle) SyncDistribution() error {
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
					log.Warn("idf oracle lack some sealed segment", zap.Int64("segment", segment.SegmentID))
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

	for segmentID, stats := range o.growing {
		// drop growing segment bm25 stats
		if stats.droppedVersion != 0 && stats.droppedVersion <= snapshot.targetVersion {
			if stats.activate {
				o.current.Minus(stats.bm25Stats)
			}
			delete(o.growing, segmentID)
		}
	}

	// remove sealed segment not in target
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		reserve := reserveMap.Contain(segmentID)
		intarget := targetMap.Contain(segmentID)

		stats.Lock()
		activate := stats.activate.Load()
		// save activate if segment in target.
		if intarget && !activate {
			if segmentStats, ok := activateStats[segmentID]; ok {
				o.activateSealedStatsLocked(stats, segmentStats)
			}
		}

		// deactivate if segment not in target.
		if !intarget && activate {
			if segmentStats, ok := deactivateStats[segmentID]; ok {
				o.current.Minus(segmentStats)
				stats.activate.Store(false)
			}
		}

		// remove
		// if segment not in target and not in reserve list
		// (means segment target version was old version or segment not in snapshot)
		// and add before snapshot Ts
		// (forbid remove some new segment register after current snapshot)
		remove := !intarget && !reserve && stats.ts.Before(snapshotTs)
		diskSize := stats.diskSize
		stats.Unlock()
		if remove {
			o.sealedDiskSize.Add(-diskSize)
			stats.Remove()
			o.sealed.Remove(segmentID)
		}
		return true
	})

	o.targetVersion.Store(snapshot.targetVersion)
	numRow := o.current.NumRow()
	growingLen := len(o.growing)
	sealedLen := o.sealed.Len()
	o.Unlock()

	o.syncResource()
	log.Ctx(context.TODO()).Info("sync idf distribution finished", zap.Int64("version", snapshot.targetVersion), zap.Int64("numrow", numRow), zap.Int("growing", growingLen), zap.Int("sealed", sealedLen))
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
		growing:        make(map[int64]*growingBm25Stats),
		sealed:         typeutil.ConcurrentMap[int64, *sealedBm25Stats]{},
		sealedDiskSize: atomic.NewInt64(0),
		dirPath:        path.Join(pathutil.GetPath(pathutil.BM25Path, paramtable.GetNodeID()), channel),
		syncNotify:     make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		sf:             conc.Singleflight[any]{},
	}
}
