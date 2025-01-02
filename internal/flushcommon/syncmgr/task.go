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

package syncmgr

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncTask struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface

	collectionID  int64
	partitionID   int64
	segmentID     int64
	channelName   string
	schema        *schemapb.CollectionSchema
	pkField       *schemapb.FieldSchema
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	dataSource    string
	// batchRows is the row number of this sync task,
	// not the total num of rows of segemnt
	batchRows int64
	level     datapb.SegmentLevel

	tsFrom typeutil.Timestamp
	tsTo   typeutil.Timestamp

	metacache  metacache.MetaCache
	metaWriter MetaWriter

	pack *SyncPack

	insertBinlogs map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	statsBinlogs  map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	bm25Binlogs   map[int64]*datapb.FieldBinlog
	deltaBinlog   *datapb.FieldBinlog

	writeRetryOpts []retry.Option

	failureCallback func(err error)

	tr *timerecord.TimeRecorder

	flushedSize int64
	execTime    time.Duration
}

func (t *SyncTask) getLogger() *log.MLogger {
	return log.Ctx(context.Background()).With(
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.String("channel", t.channelName),
		zap.String("level", t.level.String()),
	)
}

func (t *SyncTask) HandleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}

	metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel, t.level.String()).Inc()
	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel, t.level.String()).Inc()
	}
}

func (t *SyncTask) Run(ctx context.Context) (err error) {
	t.tr = timerecord.NewTimeRecorder("syncTask")

	log := t.getLogger()
	defer func() {
		if err != nil {
			t.HandleError(err)
		}
	}()

	_, has := t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		if t.pack.isDrop {
			log.Info("segment dropped, discard sync task")
			return nil
		}
		log.Warn("failed to sync data, segment not found in metacache")
		err := merr.WrapErrSegmentNotFound(t.segmentID)
		return err
	}

	writer := NewBulkPackWriter(t.metacache, t.chunkManager, t.allocator, t.writeRetryOpts...)
	t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.flushedSize, err = writer.Write(ctx, t.pack)

	metrics.DataNodeFlushedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.dataSource, t.level.String()).Add(float64(t.flushedSize))
	metrics.DataNodeFlushedRows.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.dataSource).Add(float64(t.batchRows))

	metrics.DataNodeSave2StorageLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.level.String()).Observe(float64(t.tr.RecordSpan().Milliseconds()))

	if t.metaWriter != nil {
		err = t.writeMeta(ctx)
		if err != nil {
			log.Warn("failed to save serialized data into storage", zap.Error(err))
			return err
		}
	}

	actions := []metacache.SegmentAction{metacache.FinishSyncing(t.batchRows)}
	if t.pack.isFlush {
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}
	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segmentID))

	if t.pack.isDrop {
		t.metacache.RemoveSegments(metacache.WithSegmentIDs(t.segmentID))
		log.Info("segment removed", zap.Int64("segmentID", t.segmentID), zap.String("channel", t.channelName))
	}

	t.execTime = t.tr.ElapseSpan()
	log.Info("task done", zap.Int64("flushedSize", t.flushedSize), zap.Duration("timeTaken", t.execTime))

	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel, t.level.String()).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel, t.level.String()).Inc()

	return nil
}

// writeMeta updates segments via meta writer in option.
func (t *SyncTask) writeMeta(ctx context.Context) error {
	return t.metaWriter.UpdateSync(ctx, t)
}

func (t *SyncTask) SegmentID() int64 {
	return t.segmentID
}

func (t *SyncTask) Checkpoint() *msgpb.MsgPosition {
	return t.checkpoint
}

func (t *SyncTask) StartPosition() *msgpb.MsgPosition {
	return t.startPosition
}

func (t *SyncTask) ChannelName() string {
	return t.channelName
}

func (t *SyncTask) IsFlush() bool {
	return t.pack.isFlush
}

func (t *SyncTask) Binlogs() (map[int64]*datapb.FieldBinlog, map[int64]*datapb.FieldBinlog, *datapb.FieldBinlog, map[int64]*datapb.FieldBinlog) {
	return t.insertBinlogs, t.statsBinlogs, t.deltaBinlog, t.bm25Binlogs
}

func (t *SyncTask) MarshalJSON() ([]byte, error) {
	deltaRowCount := int64(0)
	if t.pack != nil && t.pack.deltaData != nil {
		deltaRowCount = t.pack.deltaData.RowCount
	}
	return json.Marshal(&metricsinfo.SyncTask{
		SegmentID:     t.segmentID,
		BatchRows:     t.batchRows,
		SegmentLevel:  t.level.String(),
		TSFrom:        tsoutil.PhysicalTimeFormat(t.tsFrom),
		TSTo:          tsoutil.PhysicalTimeFormat(t.tsTo),
		DeltaRowCount: deltaRowCount,
		FlushSize:     t.flushedSize,
		RunningTime:   t.execTime.String(),
		NodeID:        paramtable.GetNodeID(),
	})
}
