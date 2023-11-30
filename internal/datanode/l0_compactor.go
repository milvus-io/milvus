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

package datanode

import (
	"context"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type levelZeroCompactionTask struct {
	compactor
	io.BinlogIO

	allocator allocator.Allocator
	metacache metacache.MetaCache
	syncmgr   syncmgr.SyncManager

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	tr   *timerecord.TimeRecorder
}

func newLevelZeroCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	alloc allocator.Allocator,
	metaCache metacache.MetaCache,
	syncmgr syncmgr.SyncManager,
	plan *datapb.CompactionPlan,
) *levelZeroCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &levelZeroCompactionTask{
		ctx:    ctx,
		cancel: cancel,

		BinlogIO:  binlogIO,
		allocator: alloc,
		metacache: metaCache,
		syncmgr:   syncmgr,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("levelzero compaction"),
		done:      make(chan struct{}, 1),
	}
}

func (t *levelZeroCompactionTask) complete() {
	t.done <- struct{}{}
}

func (t *levelZeroCompactionTask) stop() {
	t.cancel()
	<-t.done
}

func (t *levelZeroCompactionTask) getPlanID() UniqueID {
	return t.plan.GetPlanID()
}

func (t *levelZeroCompactionTask) getChannelName() string {
	return t.plan.GetChannel()
}

func (t *levelZeroCompactionTask) getCollection() int64 {
	return t.metacache.Collection()
}

// Do nothing for levelzero compaction
func (t *levelZeroCompactionTask) injectDone() {}

func (t *levelZeroCompactionTask) compact() (*datapb.CompactionPlanResult, error) {
	log := log.With(zap.Int64("planID", t.plan.GetPlanID()), zap.String("type", t.plan.GetType().String()))
	log.Info("L0 compaction", zap.Duration("wait in queue elapse", t.tr.RecordSpan()))

	if !funcutil.CheckCtxValid(t.ctx) {
		log.Warn("compact wrong, task context done or timeout")
		return nil, errContext
	}

	ctxTimeout, cancelAll := context.WithTimeout(t.ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	l0Segments := lo.Filter(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegIDs := lo.FilterMap(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) (int64, bool) {
		if s.Level == datapb.SegmentLevel_L1 {
			return s.GetSegmentID(), true
		}
		return 0, false
	})
	if len(targetSegIDs) == 0 {
		log.Warn("compact wrong, not target sealed segments")
		return nil, errIllegalCompactionPlan
	}

	var (
		totalSize      int64
		totalDeltalogs = make(map[UniqueID][]string)
	)
	for _, s := range l0Segments {
		paths := []string{}
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				paths = append(paths, l.GetLogPath())
				totalSize += l.GetLogSize()
			}
		}
		if len(paths) > 0 {
			totalDeltalogs[s.GetSegmentID()] = paths
		}
	}

	// TODO
	// batchProcess := func() ([]*datapb.CompactionSegment, error) {
	//     resultSegments := make(map[int64]*datapb.CompactionSegment)
	//
	//     iters, err := t.loadDelta(ctxTimeout, lo.Values(totalDeltalogs)...)
	//     if err != nil {
	//         return nil, err
	//     }
	//     log.Info("Batch L0 compaction load delta into memeory", zap.Duration("elapse", t.tr.RecordSpan()))
	//
	//     alteredSegments := make(map[int64]*storage.DeleteData)
	//     err = t.splitDelta(iters, alteredSegments, targetSegIDs)
	//     if err != nil {
	//         return nil, err
	//     }
	//     log.Info("Batch L0 compaction split delta into segments", zap.Duration("elapse", t.tr.RecordSpan()))
	//
	//     err = t.uploadByCheck(ctxTimeout, false, alteredSegments, resultSegments)
	//     log.Info("Batch L0 compaction upload all", zap.Duration("elapse", t.tr.RecordSpan()))
	//
	//     return lo.Values(resultSegments), nil
	// }

	linearProcess := func() ([]*datapb.CompactionSegment, error) {
		var (
			resultSegments  = make(map[int64]*datapb.CompactionSegment)
			alteredSegments = make(map[int64]*storage.DeleteData)
		)
		for segID, deltaLogs := range totalDeltalogs {
			log := log.With(zap.Int64("levelzero segment", segID))
			log.Info("Linear L0 compaction processing segment", zap.Int64s("target segmentIDs", targetSegIDs))

			allIters, err := t.loadDelta(ctxTimeout, deltaLogs)
			if err != nil {
				log.Warn("Linear L0 compaction loadDelta fail", zap.Error(err))
				return nil, err
			}

			err = t.splitDelta(allIters, alteredSegments, targetSegIDs)
			if err != nil {
				log.Warn("Linear L0 compaction splitDelta fail", zap.Error(err))
				return nil, err
			}

			err = t.uploadByCheck(ctxTimeout, true, alteredSegments, resultSegments)
			if err != nil {
				log.Warn("Linear L0 compaction upload buffer fail", zap.Error(err))
				return nil, err
			}
		}

		err := t.uploadByCheck(ctxTimeout, false, alteredSegments, resultSegments)
		if err != nil {
			log.Warn("Linear L0 compaction upload all buffer fail", zap.Error(err))
			return nil, err
		}
		log.Warn("Linear L0 compaction finished", zap.Duration("elapse", t.tr.RecordSpan()))
		return lo.Values(resultSegments), nil
	}

	var (
		resultSegments []*datapb.CompactionSegment
		err            error
	)
	// if totalSize*3 < int64(hardware.GetFreeMemoryCount()) {
	//     resultSegments, err = batchProcess()
	// }
	resultSegments, err = linearProcess()
	if err != nil {
		return nil, err
	}

	result := &datapb.CompactionPlanResult{
		PlanID:   t.plan.GetPlanID(),
		State:    commonpb.CompactionState_Completed,
		Segments: resultSegments,
		Channel:  t.plan.GetChannel(),
	}

	log.Info("L0 compaction finished", zap.Duration("elapse", t.tr.ElapseSpan()))

	return result, nil
}

func (t *levelZeroCompactionTask) loadDelta(ctx context.Context, deltaLogs ...[]string) ([]*iter.DeltalogIterator, error) {
	allIters := make([]*iter.DeltalogIterator, 0)
	for _, paths := range deltaLogs {
		blobs, err := t.Download(ctx, paths)
		if err != nil {
			return nil, err
		}

		deltaIter, err := iter.NewDeltalogIterator(blobs, nil)
		if err != nil {
			return nil, err
		}

		allIters = append(allIters, deltaIter)
	}
	return allIters, nil
}

func (t *levelZeroCompactionTask) splitDelta(
	allIters []*iter.DeltalogIterator,
	targetSegBuffer map[int64]*storage.DeleteData,
	targetSegIDs []int64,
) error {
	// spilt all delete data to segments
	for _, deltaIter := range allIters {
		for deltaIter.HasNext() {
			labeled, err := deltaIter.Next()
			if err != nil {
				return err
			}

			predicted, found := t.metacache.PredictSegments(labeled.GetPk(), metacache.WithSegmentIDs(targetSegIDs...))
			if !found {
				continue
			}

			for _, gotSeg := range predicted {
				delBuffer, ok := targetSegBuffer[gotSeg]
				if !ok {
					delBuffer = &storage.DeleteData{}
					targetSegBuffer[gotSeg] = delBuffer
				}

				delBuffer.Append(labeled.GetPk(), labeled.GetTimestamp())
			}
		}
	}
	return nil
}

func (t *levelZeroCompactionTask) composeDeltalog(segmentID int64, dData *storage.DeleteData) (map[string][]byte, *datapb.Binlog, error) {
	var (
		collID   = t.metacache.Collection()
		uploadKv = make(map[string][]byte)
	)

	seg, ok := t.metacache.GetSegmentByID(segmentID)
	if !ok {
		return nil, nil, merr.WrapErrSegmentLack(segmentID)
	}
	blob, err := storage.NewDeleteCodec().Serialize(collID, seg.PartitionID(), segmentID, dData)
	if err != nil {
		return nil, nil, err
	}

	logID, err := t.allocator.AllocOne()
	if err != nil {
		return nil, nil, err
	}

	blobKey := metautil.JoinIDPath(collID, seg.PartitionID(), segmentID, logID)
	blobPath := t.BinlogIO.JoinFullPath(common.SegmentDeltaLogPath, blobKey)

	uploadKv[blobPath] = blob.GetValue()

	// TODO Timestamp?
	deltalog := &datapb.Binlog{
		LogSize: int64(len(blob.GetValue())),
		LogPath: blobPath,
		LogID:   logID,
	}

	return uploadKv, deltalog, nil
}

func (t *levelZeroCompactionTask) uploadByCheck(ctx context.Context, requireCheck bool, alteredSegments map[int64]*storage.DeleteData, resultSegments map[int64]*datapb.CompactionSegment) error {
	for segID, dData := range alteredSegments {
		if !requireCheck || (dData.Size() >= paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64()) {
			blobs, binlog, err := t.composeDeltalog(segID, dData)
			if err != nil {
				return err
			}
			err = t.Upload(ctx, blobs)
			if err != nil {
				return err
			}

			if _, ok := resultSegments[segID]; !ok {
				resultSegments[segID] = &datapb.CompactionSegment{
					SegmentID: segID,
					Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{binlog}}},
					Channel:   t.plan.GetChannel(),
				}
			} else {
				resultSegments[segID].Deltalogs[0].Binlogs = append(resultSegments[segID].Deltalogs[0].Binlogs, binlog)
			}

			delete(alteredSegments, segID)
		}
	}
	return nil
}
