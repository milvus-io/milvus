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

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type IDFOracle interface {
	SetNext(snapshot *snapshot)
	TargetVersion() int64

	UpdateGrowing(segmentID int64, stats bm25Stats)
	// mark growing segment remove target version
	LazyRemoveGrowings(targetVersion int64, segmentIDs ...int64)

	Register(segmentID int64, stats bm25Stats, state commonpb.SegmentState)

	BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)

	Start()
	Close()
}

type bm25Stats map[int64]*storage.BM25Stats

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
		return nil, errors.New("field not found in idf oracle BM25 stats")
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
	bm25Stats

	activate *atomic.Bool

	inmemory  bool
	removed   bool
	segmentID int64
	ts        time.Time // Time of segemnt register, all segment resgister after target generate will don't remove
	localDir  string
	fieldList []int64 // bm25 field list
}

func (s *sealedBm25Stats) writeFile(localDir string) (error, bool) {
	s.RLock()

	if s.removed || !s.inmemory {
		return nil, true
	}

	stats := s.bm25Stats
	s.RUnlock()

	err := os.MkdirAll(localDir, fs.ModePerm)
	if err != nil {
		return err, false
	}

	// RUnlock when stats serialize and write to file
	// to avoid block remove stats too long when sync distribution
	for fieldID, stats := range stats {
		file, err := os.Create(path.Join(localDir, fmt.Sprintf("%d.data", fieldID)))
		if err != nil {
			return err, false
		}

		defer file.Close()
		writer := bufio.NewWriter(file)

		err = stats.SerializeToWriter(writer)
		if err != nil {
			return err, false
		}

		err = writer.Flush()
		if err != nil {
			return err, false
		}
	}

	return nil, false
}

func (s *sealedBm25Stats) ShouldOffLoadToDisk() bool {
	s.RLock()
	defer s.RUnlock()
	return s.inmemory && s.activate.Load() && !s.removed
}

// After merged the stats of a segment into the overall stats, Delegator still need to store the segment stats,
// so that later when the segment is removed from target, we can Minus its stats. To reduce memory usage,
// idfOracle store such per segment stats to disk, and load them when removing the segment.
func (s *sealedBm25Stats) ToLocal(dirPath string) error {
	dir := path.Join(dirPath, fmt.Sprint(s.segmentID))
	if err, skip := s.writeFile(dir); err != nil {
		os.RemoveAll(dir)
		return err
	} else if skip {
		return nil
	}

	s.Lock()
	defer s.Unlock()
	s.fieldList = lo.Keys(s.bm25Stats)
	s.inmemory = false
	s.bm25Stats = nil
	s.localDir = dir

	if s.removed {
		err := os.RemoveAll(s.localDir)
		if err != nil {
			log.Warn("remove local bm25 stats failed", zap.Error(err), zap.String("path", s.localDir))
		}
	}
	return nil
}

func (s *sealedBm25Stats) Remove() {
	s.Lock()
	defer s.Unlock()
	s.removed = true

	if !s.inmemory {
		err := os.RemoveAll(s.localDir)
		if err != nil {
			log.Warn("remove local bm25 stats failed", zap.Error(err), zap.String("path", s.localDir))
		}
	}
}

// Fetch sealed bm25 stats
// load local file and return it when stats not in memeory
func (s *sealedBm25Stats) FetchStats() (map[int64]*storage.BM25Stats, error) {
	s.RLock()
	defer s.RUnlock()

	if s.inmemory {
		return s.bm25Stats, nil
	}

	stats := make(map[int64]*storage.BM25Stats)
	for _, fieldID := range s.fieldList {
		path := path.Join(s.localDir, fmt.Sprintf("%d.data", fieldID))
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Newf("read local file %s: failed: %v", path, err)
		}

		stats[fieldID] = storage.NewBM25Stats()
		err = stats[fieldID].Deserialize(b)
		if err != nil {
			return nil, errors.Newf("deserialize local file : %s failed: %v", path, err)
		}
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

	sealed typeutil.ConcurrentMap[int64, *sealedBm25Stats]

	channel string

	// for sync distribution
	next          idfTarget
	targetVersion *atomic.Int64
	syncNotify    chan struct{}

	// for disk cache
	localNotify chan struct{}
	dirPath     string

	closeCh chan struct{}
	wg      sync.WaitGroup
}

// now only used for test
func (o *idfOracle) TargetVersion() int64 {
	return o.targetVersion.Load()
}

func (o *idfOracle) preloadSealed(segmentID int64, stats bm25Stats) {
	o.Lock()
	defer o.Unlock()

	// skip preload if first target was loaded.
	if o.targetVersion.Load() != 0 {
		return
	}
	o.sealed.Insert(segmentID, &sealedBm25Stats{
		bm25Stats: stats,
		ts:        time.Now(),
		activate:  atomic.NewBool(true),
		inmemory:  true,
		segmentID: segmentID,
	})
	o.current.Merge(stats)
}

func (o *idfOracle) Register(segmentID int64, stats bm25Stats, state commonpb.SegmentState) {
	switch state {
	case segments.SegmentTypeGrowing:
		o.Lock()
		defer o.Unlock()

		if _, ok := o.growing[segmentID]; ok {
			return
		}
		o.growing[segmentID] = &growingBm25Stats{
			bm25Stats: stats,
			activate:  true,
		}
		o.current.Merge(stats)
	case segments.SegmentTypeSealed:
		if ok := o.sealed.Contain(segmentID); ok {
			return
		}

		// preload sealed segment to channel before first target
		if o.targetVersion.Load() == 0 {
			o.preloadSealed(segmentID, stats)
		} else {
			o.sealed.Insert(segmentID, &sealedBm25Stats{
				bm25Stats: stats,
				ts:        time.Now(),
				activate:  atomic.NewBool(false),
				inmemory:  true,
				segmentID: segmentID,
			})
		}
	default:
		log.Warn("register segment with unknown state", zap.String("stats", state.String()))
		return
	}
}

func (o *idfOracle) UpdateGrowing(segmentID int64, stats bm25Stats) {
	if len(stats) == 0 {
		return
	}

	o.Lock()
	defer o.Unlock()

	old, ok := o.growing[segmentID]
	if !ok {
		return
	}

	old.Merge(stats)
	if old.activate {
		o.current.Merge(stats)
	}
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

func (o *idfOracle) Start() {
	o.wg.Add(1)
	go o.syncloop()

	if paramtable.Get().QueryNodeCfg.IDFEnableDisk.GetAsBool() {
		o.wg.Add(1)
		go o.localloop()
	}
}

func (o *idfOracle) Close() {
	close(o.closeCh)
	o.wg.Wait()

	os.RemoveAll(o.dirPath)
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

func (o *idfOracle) NotifyLocal() {
	select {
	case o.localNotify <- struct{}{}:
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

func (o *idfOracle) localloop() {
	pool := conc.NewPool[struct{}](paramtable.Get().QueryNodeCfg.IDFWriteConcurrenct.GetAsInt())
	// write local file to /{bm25_path}/{node_id}/{channel_name}
	o.dirPath = path.Join(pathutil.GetPath(pathutil.BM25Path, paramtable.GetNodeID()), o.channel)

	defer o.wg.Done()
	for {
		select {
		case <-o.localNotify:
			statsList := []*sealedBm25Stats{}
			o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
				if stats.ShouldOffLoadToDisk() {
					statsList = append(statsList, stats)
				}
				return true
			})

			if _, err := os.Stat(o.dirPath); os.IsNotExist(err) {
				err = os.MkdirAll(o.dirPath, 0o755)
				if err != nil {
					log.Warn("create idf local path failed", zap.Error(err))
				}
			}

			features := []*conc.Future[struct{}]{}
			for _, stats := range statsList {
				features = append(features, pool.Submit(func() (struct{}, error) {
					err := stats.ToLocal(o.dirPath)
					if err != nil {
						log.Warn("idf oracle to local failed", zap.Error(err))
						return struct{}{}, nil
					}
					return struct{}{}, nil
				}))
			}

			conc.AwaitAll(features...)
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

	diff := bm25Stats{}

	var rangeErr error
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		intarget := targetMap.Contain(segmentID)

		activate := stats.activate.Load()
		// activate segment if segment in target
		if intarget && !activate {
			stats, err := stats.FetchStats()
			if err != nil {
				rangeErr = fmt.Errorf("fetch stats failed with error: %v", err)
				return false
			}
			diff.Merge(stats)
		} else
		// deactivate segment if segment not in target.
		if !intarget && activate {
			stats, err := stats.FetchStats()
			if err != nil {
				rangeErr = fmt.Errorf("fetch stats failed with error: %v", err)
				return false
			}
			diff.Minus(stats)
		}
		return true
	})

	if rangeErr != nil {
		return rangeErr
	}

	o.Lock()
	defer o.Unlock()

	for segmentID, stats := range o.growing {
		// drop growing segment bm25 stats
		if stats.droppedVersion != 0 && stats.droppedVersion <= snapshot.targetVersion {
			if stats.activate {
				o.current.Minus(stats.bm25Stats)
			}
			delete(o.growing, segmentID)
		}
	}
	o.current.Merge(diff)

	// remove sealed segment not in target
	o.sealed.Range(func(segmentID int64, stats *sealedBm25Stats) bool {
		reserve := reserveMap.Contain(segmentID)
		intarget := targetMap.Contain(segmentID)

		activate := stats.activate.Load()
		// save activate if segment in target.
		if intarget && !activate {
			stats.activate.Store(true)
		}

		// deactivate if segment not in target.
		if !intarget && activate {
			stats.activate.Store(false)
		}

		// remove
		// if segment not in target and not in reserve list
		// (means segment target version was old version or segment not in snapshot)
		// and add before snapshot Ts
		// (forbid remove some new segment register after current snapshot)
		if !intarget && !reserve && stats.ts.Before(snapshotTs) {
			stats.Remove()
			o.sealed.Remove(segmentID)
		}
		return true
	})

	o.targetVersion.Store(snapshot.targetVersion)
	o.NotifyLocal()
	log.Ctx(context.TODO()).Info("sync idf distribution finished", zap.Int64("version", snapshot.targetVersion), zap.Int64("numrow", o.current.NumRow()), zap.Int("growing", len(o.growing)), zap.Int("sealed", o.sealed.Len()))
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
		channel:       channel,
		targetVersion: atomic.NewInt64(0),
		current:       newBm25Stats(functions),
		growing:       make(map[int64]*growingBm25Stats),
		sealed:        typeutil.ConcurrentMap[int64, *sealedBm25Stats]{},
		syncNotify:    make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
		localNotify:   make(chan struct{}, 1),
	}
}
