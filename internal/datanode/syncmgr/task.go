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
	"path"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncTask struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface

	segment       *metacache.SegmentInfo
	collectionID  int64
	partitionID   int64
	segmentID     int64
	channelName   string
	schema        *schemapb.CollectionSchema
	pkField       *schemapb.FieldSchema
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	// batchSize is the row number of this sync task,
	// not the total num of rows of segemnt
	batchSize int64
	level     datapb.SegmentLevel

	tsFrom typeutil.Timestamp
	tsTo   typeutil.Timestamp

	isFlush bool
	isDrop  bool

	metacache  metacache.MetaCache
	metaWriter MetaWriter

	insertBinlogs map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	statsBinlogs  map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	deltaBinlog   *datapb.FieldBinlog

	binlogBlobs     map[int64]*storage.Blob // fieldID => blob
	binlogMemsize   map[int64]int64         // memory size
	batchStatsBlob  *storage.Blob
	mergedStatsBlob *storage.Blob
	deltaBlob       *storage.Blob
	deltaRowCount   int64

	segmentData map[string][]byte

	writeRetryOpts []retry.Option

	failureCallback func(err error)

	tr *timerecord.TimeRecorder
}

func (t *SyncTask) getLogger() *log.MLogger {
	return log.Ctx(context.Background()).With(
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.String("channel", t.channelName),
	)
}

func (t *SyncTask) handleError(err error, metricSegLevel string) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}

	metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel, metricSegLevel).Inc()
	if !t.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel, metricSegLevel).Inc()
	}
}

func (t *SyncTask) Run() (err error) {
	t.tr = timerecord.NewTimeRecorder("syncTask")
	metricSegLevel := t.level.String()

	log := t.getLogger()
	defer func() {
		if err != nil {
			t.handleError(err, metricSegLevel)
		}
	}()

	var has bool
	t.segment, has = t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		log.Warn("failed to sync data, segment not found in metacache")
		err := merr.WrapErrSegmentNotFound(t.segmentID)
		t.handleError(err, metricSegLevel)
		return err
	}

	if t.segment.CompactTo() == metacache.NullSegment {
		log.Info("segment compacted to zero-length segment, discard sync task")
		return nil
	}

	if t.segment.CompactTo() > 0 {
		log.Info("syncing segment compacted, update segment id", zap.Int64("compactTo", t.segment.CompactTo()))
		// update sync task segment id
		// it's ok to use compactTo segmentID here, since there shall be no insert for compacted segment
		t.segmentID = t.segment.CompactTo()
	}

	err = t.processInsertBlobs()
	if err != nil {
		log.Warn("failed to process insert blobs", zap.Error(err))
		return err
	}

	err = t.processStatsBlob()
	if err != nil {
		log.Warn("failed to serialize insert data", zap.Error(err))
		t.handleError(err, metricSegLevel)
		log.Warn("failed to process stats blobs", zap.Error(err))
		return err
	}

	err = t.processDeltaBlob()
	if err != nil {
		log.Warn("failed to serialize delete data", zap.Error(err))
		t.handleError(err, metricSegLevel)
		log.Warn("failed to process delta blobs", zap.Error(err))
		return err
	}

	err = t.writeLogs()
	if err != nil {
		log.Warn("failed to save serialized data into storage", zap.Error(err))
		t.handleError(err, metricSegLevel)
		return err
	}

	var totalSize float64
	totalSize += lo.SumBy(lo.Values(t.binlogMemsize), func(fieldSize int64) float64 {
		return float64(fieldSize)
	})
	if t.deltaBlob != nil {
		totalSize += float64(len(t.deltaBlob.Value))
	}

	metrics.DataNodeFlushedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.AllLabel, metricSegLevel).Add(totalSize)

	metrics.DataNodeSave2StorageLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metricSegLevel).Observe(float64(t.tr.RecordSpan().Milliseconds()))

	if t.metaWriter != nil {
		err = t.writeMeta()
		if err != nil {
			log.Warn("failed to save serialized data into storage", zap.Error(err))
			t.handleError(err, metricSegLevel)
			return err
		}
	}

	actions := []metacache.SegmentAction{metacache.FinishSyncing(t.batchSize)}
	switch {
	case t.isDrop:
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Dropped))
	case t.isFlush:
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}

	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segment.SegmentID()))

	log.Info("task done")

	if !t.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel, metricSegLevel).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel, metricSegLevel).Inc()
	return nil
}

func (t *SyncTask) processInsertBlobs() error {
	if len(t.binlogBlobs) == 0 {
		return nil
	}

	logidx, _, err := t.allocator.Alloc(uint32(len(t.binlogBlobs)))
	if err != nil {
		return err
	}

	for fieldID, blob := range t.binlogBlobs {
		k := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, logidx)
		key := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath, k)
		t.segmentData[key] = blob.GetValue()
		t.appendBinlog(fieldID, &datapb.Binlog{
			EntriesNum:    blob.RowNum,
			TimestampFrom: t.tsFrom,
			TimestampTo:   t.tsTo,
			LogPath:       key,
			LogSize:       t.binlogMemsize[fieldID],
		})
		logidx++
	}
	return nil
}

func (t *SyncTask) processStatsBlob() error {
	if t.batchStatsBlob != nil {
		logidx, err := t.allocator.AllocOne()
		if err != nil {
			return err
		}
		t.convertBlob2StatsBinlog(t.batchStatsBlob, t.pkField.GetFieldID(), logidx, t.batchSize)
	}
	if t.mergedStatsBlob != nil {
		totalRowNum := t.segment.NumOfRows()
		t.convertBlob2StatsBinlog(t.mergedStatsBlob, t.pkField.GetFieldID(), int64(storage.CompoundStatsType), totalRowNum)
	}
	return nil
}

func (t *SyncTask) processDeltaBlob() error {
	if t.deltaBlob != nil {
		logID, err := t.allocator.AllocOne()
		if err != nil {
			log.Error("failed to alloc ID", zap.Error(err))
			return err
		}

		value := t.deltaBlob.GetValue()
		data := &datapb.Binlog{}

		blobKey := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, logID)
		blobPath := path.Join(t.chunkManager.RootPath(), common.SegmentDeltaLogPath, blobKey)

		t.segmentData[blobPath] = value
		data.LogSize = int64(len(t.deltaBlob.Value))
		data.LogPath = blobPath
		data.TimestampFrom = t.tsFrom
		data.TimestampTo = t.tsTo
		data.EntriesNum = t.deltaRowCount
		t.appendDeltalog(data)
	}
	return nil
}

func (t *SyncTask) convertBlob2StatsBinlog(blob *storage.Blob, fieldID, logID int64, rowNum int64) {
	key := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, logID)
	key = path.Join(t.chunkManager.RootPath(), common.SegmentStatslogPath, key)

	value := blob.GetValue()
	t.segmentData[key] = value
	t.appendStatslog(fieldID, &datapb.Binlog{
		EntriesNum:    rowNum,
		TimestampFrom: t.tsFrom,
		TimestampTo:   t.tsTo,
		LogPath:       key,
		LogSize:       int64(len(value)),
	})
}

func (t *SyncTask) appendBinlog(fieldID int64, binlog *datapb.Binlog) {
	fieldBinlog, ok := t.insertBinlogs[fieldID]
	if !ok {
		fieldBinlog = &datapb.FieldBinlog{
			FieldID: fieldID,
		}
		t.insertBinlogs[fieldID] = fieldBinlog
	}

	fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, binlog)
}

func (t *SyncTask) appendStatslog(fieldID int64, statlog *datapb.Binlog) {
	fieldBinlog, ok := t.statsBinlogs[fieldID]
	if !ok {
		fieldBinlog = &datapb.FieldBinlog{
			FieldID: fieldID,
		}
		t.statsBinlogs[fieldID] = fieldBinlog
	}
	fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, statlog)
}

func (t *SyncTask) appendDeltalog(deltalog *datapb.Binlog) {
	t.deltaBinlog.Binlogs = append(t.deltaBinlog.Binlogs, deltalog)
}

// writeLogs writes log files (binlog/deltalog/statslog) into storage via chunkManger.
func (t *SyncTask) writeLogs() error {
	return retry.Do(context.Background(), func() error {
		return t.chunkManager.MultiWrite(context.Background(), t.segmentData)
	}, t.writeRetryOpts...)
}

// writeMeta updates segments via meta writer in option.
func (t *SyncTask) writeMeta() error {
	return t.metaWriter.UpdateSync(t)
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
