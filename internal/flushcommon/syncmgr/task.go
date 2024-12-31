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
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
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

	segment       *metacache.SegmentInfo
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

	isFlush bool
	isDrop  bool

	metacache  metacache.MetaCache
	metaWriter MetaWriter

	insertBinlogs map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	statsBinlogs  map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	bm25Binlogs   map[int64]*datapb.FieldBinlog
	deltaBinlog   *datapb.FieldBinlog

	binlogBlobs   map[int64]*storage.Blob // fieldID => blob
	binlogMemsize map[int64]int64         // memory size

	bm25Blobs      map[int64]*storage.Blob
	mergedBm25Blob map[int64]*storage.Blob

	batchStatsBlob  *storage.Blob
	mergedStatsBlob *storage.Blob

	deltaBlob     *storage.Blob
	deltaRowCount int64

	// prefetched log ids
	ids []int64

	segmentData map[string][]byte

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
	if !t.isFlush {
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

	var has bool
	t.segment, has = t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		if t.isDrop {
			log.Info("segment dropped, discard sync task")
			return nil
		}
		log.Warn("failed to sync data, segment not found in metacache")
		err := merr.WrapErrSegmentNotFound(t.segmentID)
		return err
	}

	err = t.prefetchIDs()
	if err != nil {
		log.Warn("failed allocate ids for sync task", zap.Error(err))
		return err
	}

	t.processInsertBlobs()
	t.processStatsBlob()
	t.processDeltaBlob()

	if len(t.bm25Blobs) > 0 || len(t.mergedBm25Blob) > 0 {
		t.processBM25StastBlob()
	}

	err = t.writeLogs(ctx)
	if err != nil {
		log.Warn("failed to save serialized data into storage", zap.Error(err))
		return err
	}

	var totalSize int64
	for _, size := range t.binlogMemsize {
		totalSize += size
	}
	if t.deltaBlob != nil {
		totalSize += int64(len(t.deltaBlob.Value))
	}
	t.flushedSize = totalSize

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
	if t.isFlush {
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}
	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segment.SegmentID()))

	if t.isDrop {
		t.metacache.RemoveSegments(metacache.WithSegmentIDs(t.segment.SegmentID()))
		log.Info("segment removed", zap.Int64("segmentID", t.segment.SegmentID()), zap.String("channel", t.channelName))
	}

	t.execTime = t.tr.ElapseSpan()
	log.Info("task done", zap.Int64("flushedSize", totalSize), zap.Duration("timeTaken", t.execTime))

	if !t.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel, t.level.String()).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel, t.level.String()).Inc()

	// free blobs and data
	t.binlogBlobs = nil
	t.deltaBlob = nil
	t.mergedStatsBlob = nil
	t.batchStatsBlob = nil
	t.segmentData = nil
	return nil
}

// prefetchIDs pre-allcates ids depending on the number of blobs current task contains.
func (t *SyncTask) prefetchIDs() error {
	totalIDCount := len(t.binlogBlobs)
	if t.batchStatsBlob != nil {
		totalIDCount++
	}
	if t.deltaBlob != nil {
		totalIDCount++
	}
	if t.bm25Blobs != nil {
		totalIDCount += len(t.bm25Blobs)
	}

	start, _, err := t.allocator.Alloc(uint32(totalIDCount))
	if err != nil {
		return err
	}
	t.ids = lo.RangeFrom(start, totalIDCount)
	return nil
}

func (t *SyncTask) nextID() int64 {
	if len(t.ids) == 0 {
		panic("pre-fetched ids exhausted")
	}
	r := t.ids[0]
	t.ids = t.ids[1:]
	return r
}

func (t *SyncTask) processInsertBlobs() {
	for fieldID, blob := range t.binlogBlobs {
		k := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, t.nextID())
		key := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath, k)
		t.segmentData[key] = blob.GetValue()
		t.appendBinlog(fieldID, &datapb.Binlog{
			EntriesNum:    blob.RowNum,
			TimestampFrom: t.tsFrom,
			TimestampTo:   t.tsTo,
			LogPath:       key,
			LogSize:       int64(len(blob.GetValue())),
			MemorySize:    t.binlogMemsize[fieldID],
		})
	}
}

func (t *SyncTask) processBM25StastBlob() {
	for fieldID, blob := range t.bm25Blobs {
		k := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, t.nextID())
		key := path.Join(t.chunkManager.RootPath(), common.SegmentBm25LogPath, k)
		t.segmentData[key] = blob.GetValue()
		t.appendBM25Statslog(fieldID, &datapb.Binlog{
			EntriesNum:    blob.RowNum,
			TimestampFrom: t.tsFrom,
			TimestampTo:   t.tsTo,
			LogPath:       key,
			LogSize:       int64(len(blob.GetValue())),
			MemorySize:    blob.MemorySize,
		})
	}

	for fieldID, blob := range t.mergedBm25Blob {
		k := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, int64(storage.CompoundStatsType))
		key := path.Join(t.chunkManager.RootPath(), common.SegmentBm25LogPath, k)
		t.segmentData[key] = blob.GetValue()
		t.appendBM25Statslog(fieldID, &datapb.Binlog{
			EntriesNum:    blob.RowNum,
			TimestampFrom: t.tsFrom,
			TimestampTo:   t.tsTo,
			LogPath:       key,
			LogSize:       int64(len(blob.GetValue())),
			MemorySize:    blob.MemorySize,
		})
	}
}

func (t *SyncTask) processStatsBlob() {
	if t.batchStatsBlob != nil {
		t.convertBlob2StatsBinlog(t.batchStatsBlob, t.pkField.GetFieldID(), t.nextID(), t.batchRows)
	}
	if t.mergedStatsBlob != nil {
		totalRowNum := t.segment.NumOfRows()
		t.convertBlob2StatsBinlog(t.mergedStatsBlob, t.pkField.GetFieldID(), int64(storage.CompoundStatsType), totalRowNum)
	}
}

func (t *SyncTask) processDeltaBlob() {
	if t.deltaBlob != nil {
		value := t.deltaBlob.GetValue()
		data := &datapb.Binlog{}

		blobKey := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, t.nextID())
		blobPath := path.Join(t.chunkManager.RootPath(), common.SegmentDeltaLogPath, blobKey)

		t.segmentData[blobPath] = value
		data.LogSize = int64(len(t.deltaBlob.Value))
		data.LogPath = blobPath
		data.TimestampFrom = t.tsFrom
		data.TimestampTo = t.tsTo
		data.EntriesNum = t.deltaRowCount
		data.MemorySize = t.deltaBlob.GetMemorySize()
		t.appendDeltalog(data)
	}
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
		MemorySize:    int64(len(value)),
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

func (t *SyncTask) appendBM25Statslog(fieldID int64, log *datapb.Binlog) {
	fieldBinlog, ok := t.bm25Binlogs[fieldID]
	if !ok {
		fieldBinlog = &datapb.FieldBinlog{
			FieldID: fieldID,
		}
		t.bm25Binlogs[fieldID] = fieldBinlog
	}
	fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, log)
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
func (t *SyncTask) writeLogs(ctx context.Context) error {
	return retry.Handle(ctx, func() (bool, error) {
		err := t.chunkManager.MultiWrite(ctx, t.segmentData)
		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}
		return false, nil
	}, t.writeRetryOpts...)
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
	return t.isFlush
}

func (t *SyncTask) Binlogs() (map[int64]*datapb.FieldBinlog, map[int64]*datapb.FieldBinlog, *datapb.FieldBinlog, map[int64]*datapb.FieldBinlog) {
	return t.insertBinlogs, t.statsBinlogs, t.deltaBinlog, t.bm25Binlogs
}

func (t *SyncTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(&metricsinfo.SyncTask{
		SegmentID:     t.segmentID,
		BatchRows:     t.batchRows,
		SegmentLevel:  t.level.String(),
		TSFrom:        tsoutil.PhysicalTimeFormat(t.tsFrom),
		TSTo:          tsoutil.PhysicalTimeFormat(t.tsTo),
		DeltaRowCount: t.deltaRowCount,
		FlushSize:     t.flushedSize,
		RunningTime:   t.execTime.String(),
		NodeID:        paramtable.GetNodeID(),
	})
}
