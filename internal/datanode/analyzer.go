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

package datanode

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type analyzer interface {
	execute() (*datapb.AnalyzeStatsResult, error)
	getPlanID() UniqueID
}

type AnalyzeTask struct {
	analyzer
	io        io.BinlogIO
	allocator allocator.Allocator
	metaCache metacache.MetaCache

	ctx    context.Context
	cancel context.CancelFunc
	tr     *timerecord.TimeRecorder

	plan *datapb.CompactionPlan
}

func newAnalyzeTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	alloc allocator.Allocator,
	metaCache metacache.MetaCache,
	plan *datapb.CompactionPlan,
) *AnalyzeTask {
	ctx, cancel := context.WithCancel(ctx)
	return &AnalyzeTask{
		ctx:       ctx,
		cancel:    cancel,
		io:        binlogIO,
		allocator: alloc,
		metaCache: metaCache,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("level2 analyze"),
	}
}

func (t *AnalyzeTask) execute() (*datapb.AnalyzeStatsResult, error) {
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}

	var pkField *schemapb.FieldSchema
	for _, fs := range t.metaCache.Schema().GetFields() {
		if fs.GetIsPrimaryKey() && fs.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(fs.GetDataType()) {
			pkField = fs
		}
	}
	if pkField == nil {
		log.Warn("failed to get pk field from schema")
		// todo wrap error
		return nil, fmt.Errorf("no pk field in schema")
	}

	flightSegments := t.plan.GetSegmentBinlogs()
	for _, segment := range flightSegments {
		analyzeResult, err := t.analyze(t.ctx, pkField, segment)
		if err != nil {
			return nil, err
		}
		for k, v := range analyzeResult {
			log.Info("analyze result", zap.Any("key", k), zap.Int64("count", v))
		}
	}
	return &datapb.AnalyzeStatsResult{
		PlanID:   t.getPlanID(),
		Finished: true,
		//ResultPath:
	}, nil
}

func (t *AnalyzeTask) analyze(
	ctx context.Context,
	pkField *schemapb.FieldSchema, // must not be nil
	segment *datapb.CompactionSegmentBinlogs,
) (map[interface{}]int64, error) {
	log := log.With(zap.Int64("planID", t.getPlanID()), zap.Int64("segmentID", segment.GetSegmentID()))

	// vars
	processStart := time.Now()
	fieldBinlogPaths := make([][]string, 0)
	currentTs := tsoutil.GetCurrentTime()
	// initial timestampFrom, timestampTo = -1, -1 is an illegal value, only to mark initial state
	var (
		timestampTo   int64                 = -1
		timestampFrom int64                 = -1
		expired       int64                 = 0
		deleted       int64                 = 0
		remained      int64                 = 0
		analyzeResult map[interface{}]int64 = make(map[interface{}]int64, 0)
	)

	pkID := pkField.GetFieldID()
	pkType := pkField.GetDataType()

	// Get the number of field binlog files from non-empty segment
	var binlogNum int
	for _, b := range segment.GetFieldBinlogs() {
		if b != nil {
			binlogNum = len(b.GetBinlogs())
			break
		}
	}
	// Unable to deal with all empty segments cases, so return error
	if binlogNum == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errIllegalCompactionPlan
	}
	log.Info("binlogNum", zap.Int("binlogNum", binlogNum))
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, f := range segment.GetFieldBinlogs() {
			if f.FieldID == pkID || f.FieldID == common.RowIDField || f.FieldID == common.TimeStampField {
				log.Info("add ", zap.Int64("fieldID", f.FieldID))
				ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
			}
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}
	log.Info("fieldBinlogPaths", zap.Int("length", len(fieldBinlogPaths)))

	for _, path := range fieldBinlogPaths {
		bytesArr, err := t.io.Download(ctx, path)
		blobs := make([]*Blob, len(bytesArr))
		for i := range bytesArr {
			blobs[i] = &Blob{Value: bytesArr[i]}
		}
		if err != nil {
			log.Warn("download insertlogs wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		pkIter, err := storage.NewInsertBinlogIterator(blobs, pkID, pkType)
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		//log.Info("pkIter.RowNum()", zap.Int("pkIter.RowNum()", pkIter.RowNum()), zap.Bool("hasNext", pkIter.HasNext()))
		for pkIter.HasNext() {
			vInter, _ := pkIter.Next()
			v, ok := vInter.(*storage.Value)
			if !ok {
				log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}

			// Filtering expired entity
			ts := Timestamp(v.Timestamp)
			if isExpiredEntity(t.plan.GetCollectionTtl(), ts, currentTs) {
				expired++
				continue
			}

			// Update timestampFrom, timestampTo
			if v.Timestamp < timestampFrom || timestampFrom == -1 {
				timestampFrom = v.Timestamp
			}
			if v.Timestamp > timestampTo || timestampFrom == -1 {
				timestampTo = v.Timestamp
			}

			row, ok := v.Value.(map[UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}
			key := row[pkID]
			if _, exist := analyzeResult[key]; exist {
				analyzeResult[key] = analyzeResult[key] + 1
			} else {
				analyzeResult[key] = 1
			}
			remained++
		}
	}

	log.Info("analyze segment end",
		zap.Int64("remained entities", remained),
		zap.Int64("deleted entities", deleted),
		zap.Int64("expired entities", expired),
		zap.Duration("map elapse", time.Since(processStart)))
	return analyzeResult, nil
}

func isExpiredEntity(ttl int64, ts, now Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if ttl <= 0 {
		return false
	}

	pts, _ := tsoutil.ParseTS(ts)
	pnow, _ := tsoutil.ParseTS(now)
	expireTime := pts.Add(time.Duration(ttl))
	return expireTime.Before(pnow)
}

func (t *AnalyzeTask) getPlanID() UniqueID {
	return t.plan.GetPlanID()
}
