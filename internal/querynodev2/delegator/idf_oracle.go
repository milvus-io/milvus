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
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type IDFOracle interface {
	// Activate(segmentID int64, state commonpb.SegmentState) error
	// Deactivate(segmentID int64, state commonpb.SegmentState) error

	SyncDistribution(snapshot *snapshot)

	UpdateGrowing(segmentID int64, stats map[int64]*storage.BM25Stats)

	Register(segmentID int64, stats map[int64]*storage.BM25Stats, state commonpb.SegmentState)
	RemoveGrowing(segmentID int64)

	BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)
}

type bm25Stats struct {
	stats         map[int64]*storage.BM25Stats
	activate      bool
	targetVersion int64
}

func (s *bm25Stats) Merge(stats map[int64]*storage.BM25Stats) {
	for fieldID, newstats := range stats {
		if stats, ok := s.stats[fieldID]; ok {
			stats.Merge(newstats)
		} else {
			s.stats[fieldID] = storage.NewBM25Stats()
			s.stats[fieldID].Merge(newstats)
		}
	}
}

func (s *bm25Stats) Minus(stats map[int64]*storage.BM25Stats) {
	for fieldID, newstats := range stats {
		if stats, ok := s.stats[fieldID]; ok {
			stats.Minus(newstats)
		} else {
			log.Panic("minus failed, BM25 stats not exist", zap.Int64("fieldID", fieldID))
		}
	}
}

func (s *bm25Stats) GetStats(fieldID int64) (*storage.BM25Stats, error) {
	stats, ok := s.stats[fieldID]
	if !ok {
		return nil, fmt.Errorf("field not found in idf oracle BM25 stats")
	}
	return stats, nil
}

func (s *bm25Stats) NumRow() int64 {
	for _, stats := range s.stats {
		return stats.NumRow()
	}
	return 0
}

func newBm25Stats(functions []*schemapb.FunctionSchema) *bm25Stats {
	stats := &bm25Stats{
		stats: make(map[int64]*storage.BM25Stats),
	}

	for _, function := range functions {
		if function.GetType() == schemapb.FunctionType_BM25 {
			stats.stats[function.GetOutputFieldIds()[0]] = storage.NewBM25Stats()
		}
	}
	return stats
}

type idfOracle struct {
	sync.RWMutex

	current *bm25Stats

	growing map[int64]*bm25Stats
	sealed  map[int64]*bm25Stats

	targetVersion int64
}

func (o *idfOracle) Register(segmentID int64, stats map[int64]*storage.BM25Stats, state commonpb.SegmentState) {
	o.Lock()
	defer o.Unlock()

	switch state {
	case segments.SegmentTypeGrowing:
		if _, ok := o.growing[segmentID]; ok {
			return
		}
		o.growing[segmentID] = &bm25Stats{
			stats:         stats,
			activate:      true,
			targetVersion: initialTargetVersion,
		}
		o.current.Merge(stats)
	case segments.SegmentTypeSealed:
		if _, ok := o.sealed[segmentID]; ok {
			return
		}
		o.sealed[segmentID] = &bm25Stats{
			stats:         stats,
			activate:      false,
			targetVersion: initialTargetVersion,
		}
	default:
		log.Warn("register segment with unknown state", zap.String("stats", state.String()))
		return
	}
}

func (o *idfOracle) UpdateGrowing(segmentID int64, stats map[int64]*storage.BM25Stats) {
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

func (o *idfOracle) RemoveGrowing(segmentID int64) {
	o.Lock()
	defer o.Unlock()

	if stats, ok := o.growing[segmentID]; ok {
		if stats.activate {
			o.current.Minus(stats.stats)
		}
		delete(o.growing, segmentID)
	}
}

func (o *idfOracle) activate(stats *bm25Stats) {
	stats.activate = true
	o.current.Merge(stats.stats)
}

func (o *idfOracle) deactivate(stats *bm25Stats) {
	stats.activate = false
	o.current.Minus(stats.stats)
}

func (o *idfOracle) SyncDistribution(snapshot *snapshot) {
	o.Lock()
	defer o.Unlock()

	sealed, growing := snapshot.Peek()

	for _, item := range sealed {
		for _, segment := range item.Segments {
			if segment.Level == datapb.SegmentLevel_L0 {
				continue
			}

			if stats, ok := o.sealed[segment.SegmentID]; ok {
				stats.targetVersion = segment.TargetVersion
			} else {
				log.Warn("idf oracle lack some sealed segment", zap.Int64("segmentID", segment.SegmentID))
			}
		}
	}

	for _, segment := range growing {
		if stats, ok := o.growing[segment.SegmentID]; ok {
			stats.targetVersion = segment.TargetVersion
		} else {
			log.Warn("idf oracle lack some growing segment", zap.Int64("segmentID", segment.SegmentID))
		}
	}

	o.targetVersion = snapshot.targetVersion

	for segmentID, stats := range o.sealed {
		if !stats.activate && stats.targetVersion == o.targetVersion {
			o.activate(stats)
		} else if stats.targetVersion != o.targetVersion {
			if stats.activate {
				o.current.Minus(stats.stats)
			}
			delete(o.sealed, segmentID)
		}
	}

	for _, stats := range o.growing {
		if !stats.activate && (stats.targetVersion == o.targetVersion || stats.targetVersion == initialTargetVersion) {
			o.activate(stats)
		} else if stats.activate && (stats.targetVersion != o.targetVersion && stats.targetVersion != initialTargetVersion) {
			o.deactivate(stats)
		}
	}

	log.Ctx(context.TODO()).Debug("sync distribution finished", zap.Int64("version", o.targetVersion), zap.Int64("numrow", o.current.NumRow()))
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

func NewIDFOracle(functions []*schemapb.FunctionSchema) IDFOracle {
	return &idfOracle{
		current: newBm25Stats(functions),
		growing: make(map[int64]*bm25Stats),
		sealed:  make(map[int64]*bm25Stats),
	}
}
