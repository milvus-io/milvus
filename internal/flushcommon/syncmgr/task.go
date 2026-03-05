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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SyncTask struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface

	collectionID  int64
	partitionID   int64
	segmentID     int64
	channelName   string
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
	schema     *schemapb.CollectionSchema // schema for when buffer created, could be different from current on in metacache

	pack *SyncPack

	insertBinlogs map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	statsBinlogs  map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	bm25Binlogs   map[int64]*datapb.FieldBinlog
	deltaBinlog   *datapb.FieldBinlog

	manifestPath string

	writeRetryOpts []retry.Option

	failureCallback func(err error)

	tr *timerecord.TimeRecorder

	flushedSize int64
	execTime    time.Duration

	// storage config used in pooled tasks, optional
	// use singleton config for non-pooled tasks
	storageConfig *indexpb.StorageConfig
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

	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
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

	segmentInfo, has := t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		if t.pack.isDrop {
			log.Info("segment dropped, discard sync task")
			return nil
		}
		log.Warn("segment not found in metacache, may be already synced")
		return nil
	}

	columnGroups := t.getColumnGroups(segmentInfo)

	switch segmentInfo.GetStorageVersion() {
	case storage.StorageV2:
		// New sync task means needs to flush data immediately, so do not need to buffer data in writer again.
		writer := NewBulkPackWriterV2(t.metacache, t.schema, t.chunkManager, t.allocator, 0,
			packed.DefaultMultiPartUploadSize, t.storageConfig, columnGroups, t.writeRetryOpts...)
		t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.manifestPath, t.flushedSize, err = writer.Write(ctx, t.pack)
	case storage.StorageV3:
		writer := NewBulkPackWriterV3(t.metacache, t.schema, t.chunkManager, t.allocator, 0,
			packed.DefaultMultiPartUploadSize, t.storageConfig, columnGroups, segmentInfo.ManifestPath(), t.writeRetryOpts...)
		t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.manifestPath, t.flushedSize, err = writer.Write(ctx, t.pack)
	default:
		writer := NewBulkPackWriter(t.metacache, t.schema, t.chunkManager, t.allocator, t.writeRetryOpts...)
		t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.flushedSize, err = writer.Write(ctx, t.pack)
	}

	if err != nil {
		log.Warn("failed to write sync data with storage v2 format", zap.Error(err))
		return err
	}

	getDataCount := func(binlogs ...*datapb.FieldBinlog) int64 {
		count := int64(0)
		for _, binlog := range binlogs {
			for _, fbinlog := range binlog.GetBinlogs() {
				count += fbinlog.GetEntriesNum()
			}
		}
		return count
	}
	metrics.DataNodeWriteDataCount.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource, metrics.InsertLabel, fmt.Sprint(t.collectionID)).Add(float64(t.batchRows))
	metrics.DataNodeWriteDataCount.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource, metrics.DeleteLabel, fmt.Sprint(t.collectionID)).Add(float64(getDataCount(t.deltaBinlog)))
	metrics.DataNodeFlushedSize.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource, t.level.String()).Add(float64(t.flushedSize))

	metrics.DataNodeFlushedRows.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource).Add(float64(t.batchRows))

	metrics.DataNodeSave2StorageLatency.WithLabelValues(paramtable.GetStringNodeID(), t.level.String()).Observe(float64(t.tr.RecordSpan().Milliseconds()))

	if t.metaWriter != nil {
		err = t.writeMeta(ctx)
		if err != nil {
			log.Warn("failed to save serialized data into storage", zap.Error(err))
			return err
		}
	}

	t.pack.ReleaseData()

	actions := []metacache.SegmentAction{metacache.FinishSyncing(t.batchRows), metacache.UpdateManifestPath(t.manifestPath)}
	if columnGroups != nil {
		actions = append(actions, metacache.UpdateCurrentSplit(columnGroups))
	}
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
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()

	// Publish filesystem metrics after sync task completion
	storagev2.PublishFilesystemMetricsWithConfig(t.storageConfig)

	return nil
}

func (t *SyncTask) getColumnGroups(segmentInfo *metacache.SegmentInfo) []storagecommon.ColumnGroup {
	// column group only needed for storage v2 segment
	if segmentInfo.GetStorageVersion() != storage.StorageV2 && segmentInfo.GetStorageVersion() != storage.StorageV3 {
		return nil
	}

	// empty pack
	if len(t.pack.insertData) == 0 && t.schema == nil {
		return nil
	}

	allFields := typeutil.GetAllFieldSchemas(t.schema)

	// use previous split if already exists
	if currentSplit := segmentInfo.GetCurrentSplit(); currentSplit != nil {
		for _, cg := range currentSplit {
			// legacy split found, use legacy policy
			if len(cg.Fields) == 0 {
				result := storagecommon.SplitColumns(allFields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
				log.Info("use legacy split policy", zap.Int64("segmentID", t.segmentID), zap.Stringers("columnGroups", result))
				return result
			}
		}
		field2idx := make(map[int64]int)
		for idx, field := range allFields {
			field2idx[field.GetFieldID()] = idx
		}
		for idx, cg := range currentSplit {
			cg.Columns = lo.Map(cg.Fields, func(fieldID int64, _ int) int {
				return field2idx[fieldID]
			})
			currentSplit[idx] = cg
		}
		return currentSplit
	}

	policies := storagecommon.DefaultPolicies()
	result := storagecommon.SplitColumns(allFields, t.calcColumnStats(), policies...)
	log.Info("sync new split columns", zap.Int64("segmentID", t.segmentID), zap.Stringers("columnGroups", result))
	return result
}

func (t *SyncTask) calcColumnStats() map[int64]storagecommon.ColumnStats {
	result := make(map[int64]storagecommon.ColumnStats)

	memorySizes := make(map[int64]int64)
	rowNums := make(map[int64]int64)
	for _, data := range t.pack.insertData {
		for fieldID, fieldData := range data.Data {
			memorySizes[fieldID] += int64(fieldData.GetMemorySize())
			rowNums[fieldID] += int64(fieldData.RowNum())
		}
	}
	for fieldID, rowNum := range rowNums {
		if rowNum > 0 {
			result[fieldID] = storagecommon.ColumnStats{
				AvgSize: memorySizes[fieldID] / rowNum,
			}
		}
	}
	return result
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
