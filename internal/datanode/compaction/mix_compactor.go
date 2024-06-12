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

package compaction

import (
	"context"
	"fmt"
	sio "io"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// for MixCompaction only
type mixCompactionTask struct {
	binlogIO io.BinlogIO
	allocator.Allocator
	currentTs typeutil.Timestamp

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	tr   *timerecord.TimeRecorder
}

// make sure compactionTask implements compactor interface
var _ Compactor = (*mixCompactionTask)(nil)

func NewMixCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	alloc allocator.Allocator,
	plan *datapb.CompactionPlan,
) *mixCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &mixCompactionTask{
		ctx:       ctx1,
		cancel:    cancel,
		binlogIO:  binlogIO,
		Allocator: alloc,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("mix compaction"),
		currentTs: tsoutil.GetCurrentTime(),
		done:      make(chan struct{}, 1),
	}
}

func (t *mixCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *mixCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *mixCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *mixCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

// return num rows of all segment compaction from
func (t *mixCompactionTask) getNumRows() int64 {
	numRows := int64(0)
	for _, binlog := range t.plan.SegmentBinlogs {
		if len(binlog.GetFieldBinlogs()) > 0 {
			for _, ct := range binlog.GetFieldBinlogs()[0].GetBinlogs() {
				numRows += ct.GetEntriesNum()
			}
		}
	}
	return numRows
}

func (t *mixCompactionTask) merge(
	ctx context.Context,
	binlogPaths [][]string,
	delta map[interface{}]typeutil.Timestamp,
	writer *SegmentWriter,
) (*datapb.CompactionSegment, error) {
	_ = t.tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "CompactMerge")
	defer span.End()

	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.Int64("compactTo segment", writer.GetSegmentID()))

	var (
		syncBatchCount    int   // binlog batch count
		remainingRowCount int64 // the number of remaining entities
		expiredRowCount   int64 // the number of expired entities
		unflushedRowCount int64 = 0

		// All binlog meta of a segment
		allBinlogs = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
	)

	isValueDeleted := func(v *storage.Value) bool {
		ts, ok := delta[v.PK.GetValue()]
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if ok && uint64(v.Timestamp) < ts {
			return true
		}
		return false
	}

	downloadTimeCost := time.Duration(0)
	serWriteTimeCost := time.Duration(0)
	uploadTimeCost := time.Duration(0)

	for _, paths := range binlogPaths {
		log := log.With(zap.Strings("paths", paths))
		downloadStart := time.Now()
		allValues, err := t.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
		}
		downloadTimeCost += time.Since(downloadStart)

		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})

		iter, err := storage.NewBinlogDeserializeReader(blobs, writer.GetPkID())
		if err != nil {
			log.Warn("compact wrong, failed to new insert binlogs reader", zap.Error(err))
			return nil, err
		}

		for {
			err := iter.Next()
			if err != nil {
				if err == sio.EOF {
					break
				} else {
					log.Warn("compact wrong, failed to iter through data", zap.Error(err))
					return nil, err
				}
			}
			v := iter.Value()
			if isValueDeleted(v) {
				continue
			}

			// Filtering expired entity
			if isExpiredEntity(t.plan.GetCollectionTtl(), t.currentTs, typeutil.Timestamp(v.Timestamp)) {
				expiredRowCount++
				continue
			}

			err = writer.Write(v)
			if err != nil {
				log.Warn("compact wrong, failed to writer row", zap.Error(err))
				return nil, err
			}
			unflushedRowCount++
			remainingRowCount++

			if (unflushedRowCount+1)%100 == 0 && writer.IsFull() {
				serWriteStart := time.Now()
				kvs, partialBinlogs, err := serializeWrite(ctx, t.Allocator, writer)
				if err != nil {
					log.Warn("compact wrong, failed to serialize writer", zap.Error(err))
					return nil, err
				}
				serWriteTimeCost += time.Since(serWriteStart)

				uploadStart := time.Now()
				if err := t.binlogIO.Upload(ctx, kvs); err != nil {
					log.Warn("compact wrong, failed to upload kvs", zap.Error(err))
				}
				uploadTimeCost += time.Since(uploadStart)
				mergeFieldBinlogs(allBinlogs, partialBinlogs)
				syncBatchCount++
				unflushedRowCount = 0
			}
		}
	}

	if !writer.IsEmpty() {
		serWriteStart := time.Now()
		kvs, partialBinlogs, err := serializeWrite(ctx, t.Allocator, writer)
		if err != nil {
			log.Warn("compact wrong, failed to serialize writer", zap.Error(err))
			return nil, err
		}
		serWriteTimeCost += time.Since(serWriteStart)

		uploadStart := time.Now()
		if err := t.binlogIO.Upload(ctx, kvs); err != nil {
			log.Warn("compact wrong, failed to upload kvs", zap.Error(err))
		}
		uploadTimeCost += time.Since(uploadStart)

		mergeFieldBinlogs(allBinlogs, partialBinlogs)
		syncBatchCount++
	}

	serWriteStart := time.Now()
	sPath, err := statSerializeWrite(ctx, t.binlogIO, t.Allocator, writer, remainingRowCount)
	if err != nil {
		log.Warn("compact wrong, failed to serialize write segment stats",
			zap.Int64("remaining row count", remainingRowCount), zap.Error(err))
		return nil, err
	}
	serWriteTimeCost += time.Since(serWriteStart)

	pack := &datapb.CompactionSegment{
		SegmentID:           writer.GetSegmentID(),
		InsertLogs:          lo.Values(allBinlogs),
		Field2StatslogPaths: []*datapb.FieldBinlog{sPath},
		NumOfRows:           remainingRowCount,
		Channel:             t.plan.GetChannel(),
	}

	totalElapse := t.tr.RecordSpan()

	log.Info("compact merge end",
		zap.Int64("remaining row count", remainingRowCount),
		zap.Int64("expired entities", expiredRowCount),
		zap.Int("binlog batch count", syncBatchCount),
		zap.Duration("download binlogs elapse", downloadTimeCost),
		zap.Duration("upload binlogs elapse", uploadTimeCost),
		zap.Duration("serWrite elapse", serWriteTimeCost),
		zap.Duration("deRead elapse", totalElapse-serWriteTimeCost-downloadTimeCost-uploadTimeCost),
		zap.Duration("total elapse", totalElapse))

	return pack, nil
}

func mergeFieldBinlogs(base, paths map[typeutil.UniqueID]*datapb.FieldBinlog) {
	for fID, fpath := range paths {
		if _, ok := base[fID]; !ok {
			base[fID] = &datapb.FieldBinlog{FieldID: fID, Binlogs: make([]*datapb.Binlog, 0)}
		}
		base[fID].Binlogs = append(base[fID].Binlogs, fpath.GetBinlogs()...)
	}
}

func (t *mixCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	durInQueue := t.tr.RecordSpan()
	compactStart := time.Now()
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("MixCompact-%d", t.GetPlanID()))
	defer span.End()

	if len(t.plan.GetSegmentBinlogs()) < 1 {
		log.Warn("compact wrong, there's no segments in segment binlogs", zap.Int64("planID", t.plan.GetPlanID()))
		return nil, errors.New("compaction plan is illegal")
	}

	collectionID := t.plan.GetSegmentBinlogs()[0].GetCollectionID()
	partitionID := t.plan.GetSegmentBinlogs()[0].GetPartitionID()

	log := log.Ctx(ctx).With(zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))

	if ok := funcutil.CheckCtxValid(ctx); !ok {
		log.Warn("compact wrong, task context done or timeout")
		return nil, ctx.Err()
	}

	ctxTimeout, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	log.Info("compact start")

	targetSegID, err := t.AllocOne()
	if err != nil {
		log.Warn("compact wrong, unable to allocate segmentID", zap.Error(err))
		return nil, err
	}

	previousRowCount := t.getNumRows()

	writer, err := NewSegmentWriter(t.plan.GetSchema(), previousRowCount, targetSegID, partitionID, collectionID)
	if err != nil {
		log.Warn("compact wrong, unable to init segment writer", zap.Error(err))
		return nil, err
	}

	segIDs := lo.Map(t.plan.GetSegmentBinlogs(), func(binlogs *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return binlogs.GetSegmentID()
	})

	deltaPaths, allPath, err := loadDeltaMap(t.plan.GetSegmentBinlogs())
	if err != nil {
		log.Warn("fail to merge deltalogs", zap.Error(err))
		return nil, err
	}

	// Unable to deal with all empty segments cases, so return error
	if len(allPath) == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errors.New("illegal compaction plan")
	}

	deltaPk2Ts, err := mergeDeltalogs(ctxTimeout, t.binlogIO, deltaPaths)
	if err != nil {
		log.Warn("compact wrong, fail to merge deltalogs", zap.Error(err))
		return nil, err
	}

	compactToSeg, err := t.merge(ctxTimeout, allPath, deltaPk2Ts, writer)
	if err != nil {
		log.Warn("compact wrong, fail to merge", zap.Error(err))
		return nil, err
	}

	log.Info("compact done",
		zap.Int64("compact to segment", targetSegID),
		zap.Int64s("compact from segments", segIDs),
		zap.Int("num of binlog paths", len(compactToSeg.GetInsertLogs())),
		zap.Int("num of stats paths", 1),
		zap.Int("num of delta paths", len(compactToSeg.GetDeltalogs())),
		zap.Duration("compact elapse", time.Since(compactStart)),
	)

	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	metrics.DataNodeCompactionLatencyInQueue.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(durInQueue.Milliseconds()))

	planResult := &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Channel:  t.GetChannelName(),
		Segments: []*datapb.CompactionSegment{compactToSeg},
		Type:     t.plan.GetType(),
	}

	return planResult, nil
}

func (t *mixCompactionTask) GetCollection() typeutil.UniqueID {
	// The length of SegmentBinlogs is checked before task enqueueing.
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *mixCompactionTask) isExpiredEntity(ts typeutil.Timestamp) bool {
	now := t.currentTs

	// entity expire is not enabled if duration <= 0
	if t.plan.GetCollectionTtl() <= 0 {
		return false
	}

	entityT, _ := tsoutil.ParseTS(ts)
	nowT, _ := tsoutil.ParseTS(now)

	return entityT.Add(time.Duration(t.plan.GetCollectionTtl())).Before(nowT)
}
