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
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type syncTaskPayloadAccounting struct {
	insertBytes atomic.Int64
	deleteBytes atomic.Int64
	releaseOnce sync.Once
	onRelease   func(int64)
}

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

	// stats is the writer-built Statistics for SegmentInfo.Stats: insert /
	// delta counts and sizes, bloom-filter / BM25 stats_binlog_size,
	// timestamp_from/to/quantiles. DataCoord persists it directly on
	// SaveBinlogPathsRequest.Stats; for V2 (or any flush that returns nil
	// here) the handler falls back to computing from FieldBinlog arrays.
	stats *datapb.Statistics

	// ioRetry is this task's budget and backoff for the retries inside the
	// writers. attempts of 0 means unlimited; see DefaultIORetryAttempts for why
	// both differ by caller.
	ioRetry         ioRetryPolicy
	failureCallback func(err error)
	// abandonOnce makes Abandon safe against CONCURRENT callers, not merely
	// repeated ones. releasePayload has its own once, but the prepared native
	// handle's Destroy+nil is not atomic — two racing Abandons could both
	// observe non-nil and double-free the C memory.
	abandonOnce           sync.Once
	payload               *syncTaskPayloadAccounting
	dataWritten           bool
	preparedStats         *metacache.SegmentStats
	preparedStatsBlobSize int64
	preparedStatsDigested bool
	preparedV3            *preparedV3Write
	v3Prepared            bool
	preparedColumns       []storagecommon.ColumnGroup
	columnsFrozen         bool

	tr *timerecord.TimeRecorder

	flushedSize int64
	execTime    time.Duration

	// storage config used in pooled tasks, optional
	// use singleton config for non-pooled tasks
	storageConfig *indexpb.StorageConfig
}

func (t *SyncTask) getLogger() *mlog.Logger {
	return mlog.With(
		mlog.FieldCollectionID(t.collectionID),
		mlog.FieldPartitionID(t.partitionID),
		mlog.FieldSegmentID(t.segmentID),
		mlog.String("channel", t.channelName),
		mlog.String("level", t.level.String()),
	)
}

// HandleError REPORTS a failure. It must not release anything: the write buffer
// owns this task's lifetime and may keep it for another attempt, and a released
// prepared handle is exactly what that attempt needs. Releasing is Abandon's
// job, and only the owner calls it.
func (t *SyncTask) HandleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}

	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	}
}

// Abandon releases everything this task holds: the in-memory payload and any
// prepared storage handle. It is idempotent, and it is the ONLY way to release
// them — there is deliberately no narrower "release just the payload" variant,
// because a caller reaching for one on a retryable failure would silently
// destroy what the next attempt needs.
//
// Called by the write buffer once it has decided the task will never run again
// (shutdown, drop, or a terminal failure). Never on a retryable failure.
func (t *SyncTask) Abandon() {
	t.abandonOnce.Do(func() {
		t.releasePreparedStorage()
		t.releasePayload()
		// No Commit will run, so the metadata releasePayload deliberately
		// spares is dead too — and the task itself may sit in the sync
		// manager's diagnostic LRU for another 15 minutes.
		if t.pack != nil {
			t.pack.ReleaseAll()
		}
	})
}

func (t *SyncTask) releasePreparedStorage() {
	if t.preparedV3 != nil {
		t.preparedV3.Destroy()
		t.preparedV3 = nil
	}
}

// Prepare performs the slow storage work for this batch. It is intentionally
// separate from Commit so different batches of one segment can prepare in
// parallel while their metadata publication remains ordered.
func (t *SyncTask) Prepare(ctx context.Context) (err error) {
	t.tr = timerecord.NewTimeRecorder("syncTask")

	logger := t.getLogger()

	segmentInfo, has := t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		// Every removal site runs only after this segment's LAST task has
		// committed: flush completion (finishWriteBufferSync), drop commit, or
		// DataCoord reporting it compacted away (UpdateSegmentView). A drop task
		// is no exception — dropChannel drains every outstanding task before
		// building the final drop task, writeBufferSyncBlockedLocked refuses to
		// queue behind a Sealed/Flushing/Dropped segment's final task, and
		// getSyncTask never builds a task for a segment missing from the
		// metacache. The dispatcher aborts a key's whole suffix on failure, so
		// no earlier batch can still be in flight either. Reaching here
		// therefore means a task outlived the segment with its payload
		// unwritten — and reporting success would let the write buffer advance
		// the channel checkpoint past rows that exist nowhere, losing them from
		// WAL replay too. ErrSegmentNotFound is classified SyncTerminal, which
		// escalates to the fatal handler instead of being re-driven forever;
		// the terminal path releases the payload via Abandon.
		return merr.WrapErrSegmentNotFound(t.segmentID, "segment removed while its sync task was still in flight")
	}

	columnGroups := t.preparedColumns

	if !t.dataWritten {
		if !t.columnsFrozen {
			columnGroups, err = t.resolveSharedColumnGroups(segmentInfo)
			if err != nil {
				return err
			}
		}
		// statsWriter, when set (V2 / V3), exposes this sync's prepared cumulative
		// stats. Freeze it with the logical task so a metadata retry does not write
		// the payload again or publish different statistics.
		var statsWriter interface {
			PreparedStats() *metacache.SegmentStats
			PreparedStatsDelta() (int64, bool)
		}

		switch segmentInfo.GetStorageVersion() {
		case storage.StorageV2:
			// New sync task means needs to flush data immediately, so do not need to buffer data in writer again.
			writer := NewBulkPackWriterV2(t.metacache, t.schema, t.chunkManager, t.allocator, 0,
				packed.DefaultMultiPartUploadSize, t.storageConfig, columnGroups, t.ioRetry)
			t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.manifestPath, t.flushedSize, t.stats, err = writer.Write(ctx, t.pack)
			statsWriter = writer
		case storage.StorageV3:
			writer := NewBulkPackWriterV3(t.metacache, t.schema, t.chunkManager, t.allocator, 0,
				packed.DefaultMultiPartUploadSize, t.storageConfig, columnGroups, segmentInfo.ManifestPath(), t.ioRetry)
			prepared, prepareErr := writer.Prepare(ctx, t.pack)
			if prepareErr != nil {
				err = prepareErr
				break
			}
			t.insertBinlogs = prepared.inserts
			t.deltaBinlog = prepared.deltas
			t.statsBinlogs = prepared.stats
			t.bm25Binlogs = prepared.bm25Stats
			t.flushedSize = prepared.size
			t.preparedStatsBlobSize = prepared.statsBlobSize
			t.preparedStatsDigested = prepared.digested
			t.preparedStats = segmentInfo.Statistics()
			t.preparedV3 = prepared.commit
			t.v3Prepared = true
		default:
			writer := NewBulkPackWriter(t.metacache, t.schema, t.chunkManager, t.allocator, t.ioRetry)
			t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.flushedSize, err = writer.Write(ctx, t.pack)
		}

		if err != nil {
			logger.Warn(ctx, "failed to write sync data", mlog.Err(err))
			return err
		}
		t.dataWritten = true
		t.preparedColumns = columnGroups
		if statsWriter != nil {
			t.preparedStats = statsWriter.PreparedStats()
			t.preparedStatsBlobSize, t.preparedStatsDigested = statsWriter.PreparedStatsDelta()
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

		// Metadata retry only needs the frozen binlogs, statistics and column
		// groups above. Release the row payload as soon as object storage has
		// accepted it instead of retaining a large pack across metadata retries.
		t.releasePayload()
	}

	return nil
}

// Commit publishes a prepared batch. Callers must serialize Commit by segment
// sequence; no later batch may publish metadata or advance its checkpoint
// before every earlier batch has committed.
func (t *SyncTask) Commit(ctx context.Context) (err error) {
	if !t.dataWritten {
		return merr.WrapErrServiceInternalMsg("sync task commit before prepare, segmentID=%d", t.segmentID)
	}

	logger := t.getLogger()
	// A V3 task that reached Commit without a prepared handle and without a
	// manifest has lost the only thing that can publish its data. Failing loudly
	// beats falling through and reporting an empty manifest path as success.
	if t.v3Prepared && t.preparedV3 == nil && t.manifestPath == "" {
		return merr.WrapErrServiceInternalMsg(
			"v3 sync task lost its prepared manifest handle before commit, segmentID=%d", t.segmentID)
	}
	if t.preparedV3 != nil {
		// The manifest commit shares the task's inner retry budget. This is
		// what makes ImportMaxWriteRetryAttempts cover it: import has no
		// write-buffer queue to re-drive a failed task, so without this inner
		// loop one transient loon error would fail the whole ImportTask.
		// Commit tolerates re-runs: it re-stages the same updates onto the same
		// base manifest, so a landed-but-unacknowledged attempt is superseded
		// rather than committed twice.
		var manifest string
		if err := retry.Do(ctx, func() error {
			var commitErr error
			manifest, commitErr = t.preparedV3.Commit(ctx)
			return commitErr
		}, ioRetryOptions(ctx, t.ioRetry)...); err != nil {
			return err
		}
		t.preparedV3.Destroy()
		t.preparedV3 = nil
		t.manifestPath = manifest
	}
	if t.preparedStats != nil {
		segment, ok := t.metacache.GetSegmentByID(t.segmentID)
		if !ok {
			return merr.WrapErrSegmentNotFound(t.segmentID)
		}
		prepared := segment.Statistics().Clone()
		if t.preparedStatsDigested {
			prepared.Digest(t.insertBinlogs, t.deltaBinlog, t.preparedStatsBlobSize, t.batchRows, t.tsFrom, t.tsTo)
		}
		t.preparedStats = prepared
		t.stats = prepared.Publish()
	}
	if t.metaWriter != nil {
		// No retry wrapper here: BrokerMetaWriter owns the RPC retry budget
		// (retry.Handle, framework default 10 attempts). Wrapping it again
		// multiplied the budgets (~30 RPCs, minutes of wall time) while the
		// task held its segment's Commit FIFO slot — hiding the outage from
		// the write buffer, which owns re-drive and backpressure. One layer,
		// one budget: the meta writer retries the RPC, the write buffer
		// retries the task.
		if err := t.writeMeta(ctx); err != nil {
			logger.Warn(ctx, "failed to save serialized data into storage", mlog.Err(err))
			return err
		}
	}

	actions := make([]metacache.SegmentAction, 0, 7)
	if t.pack.preparedPKStats != nil {
		actions = append(actions, metacache.RollStats(t.pack.preparedPKStats))
	}
	// Consumed above; drop the references so a finished task parked in the sync
	// manager's diagnostic LRU does not hold them for another 15 minutes.
	defer func() {
		t.pack.preparedPKStats = nil
		t.pack.bm25Stats = nil
	}()
	if len(t.pack.bm25Stats) > 0 {
		actions = append(actions, metacache.MergeBm25Stats(t.pack.bm25Stats))
	}
	actions = append(actions, metacache.FinishSyncing(t.batchRows), metacache.UpdateManifestPath(t.manifestPath))
	if t.preparedColumns != nil {
		actions = append(actions, metacache.UpdateCurrentSplit(t.preparedColumns))
	}
	if t.pack.isFlush {
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}
	// Install the prepared cumulative stats directly in the commit transaction:
	// no digest work, the exact object whose Publish() DataCoord just persisted.
	if t.preparedStats != nil {
		actions = append(actions, metacache.SetStatistics(t.preparedStats))
	}
	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segmentID))
	// MergeBm25Stats is additive; clear the source so no later path can merge
	// the same batch twice. This is also where the memory finally goes away —
	// ReleaseData keeps bm25Stats alive precisely for the merge above.
	t.pack.bm25Stats = nil

	if t.pack.isDrop {
		t.metacache.RemoveSegments(metacache.WithSegmentIDs(t.segmentID))
		logger.Info(ctx, "segment removed", mlog.FieldSegmentID(t.segmentID), mlog.String("channel", t.channelName))
	}

	t.execTime = t.tr.ElapseSpan()
	logger.Info(ctx, "task done", mlog.Int64("flushedSize", t.flushedSize), mlog.Duration("timeTaken", t.execTime))

	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()

	// Publish filesystem metrics after sync task completion
	storagev2.PublishFilesystemMetricsWithConfig(t.storageConfig)

	return nil
}

func (t *SyncTask) getColumnGroups(segmentInfo *metacache.SegmentInfo) []storagecommon.ColumnGroup {
	return resolveColumnGroups(segmentInfo, t.schema, t.segmentID, t.calcColumnStats)
}

// resolveSharedColumnGroups derives a layout and agrees with every other task of
// this segment on ONE of them, before any file is written.
//
// The layout depends on the batch's own column statistics, so two tasks that
// both observe an empty currentSplit will compute different ones. Writing files
// under both and publishing one is an unrecoverable column-group mismatch that
// surfaces far from its cause. SetCurrentSplitIfNil is a compare-and-set under
// the metacache lock, so the first task to reach it wins and everyone else
// reads that winner back and uses it instead of their own.
//
// The write buffer avoids this path entirely by resolving while task
// construction is still serialized (see ResolveColumnGroups); this keeps the
// callers that do not — and any future one — correct rather than merely
// forbidden.
func (t *SyncTask) resolveSharedColumnGroups(segmentInfo *metacache.SegmentInfo) ([]storagecommon.ColumnGroup, error) {
	mine := t.getColumnGroups(segmentInfo)
	if mine == nil {
		return nil, nil
	}
	t.metacache.UpdateSegments(metacache.SetCurrentSplitIfNil(mine), metacache.WithSegmentIDs(t.segmentID))
	current, ok := t.metacache.GetSegmentByID(t.segmentID)
	if !ok {
		// The segment vanished between Prepare's fetch and this read-back. Falling
		// back to `mine` would write files under a layout nobody agreed on, which
		// is the exact mismatch this function exists to prevent — and Commit is
		// going to fail on the same missing segment anyway. Fail before writing.
		return nil, merr.WrapErrSegmentNotFound(t.segmentID, "segment removed while resolving its column groups")
	}
	if winner := cloneColumnGroups(current.GetCurrentSplit()); winner != nil {
		return winner, nil
	}
	return mine, nil
}

// ResolveColumnGroups derives the physical layout for this batch. The write
// buffer invokes it while task construction is still serialized, then stores
// the first split together with StartSyncing before parallel Prepare begins.
//
// The result is always freshly built — resolveColumnGroups clones the one
// branch that reads metacache state — so it needs no defensive copy of its own.
// WithFrozenColumnGroups still copies, because it is a public setter that must
// not alias whatever its caller hands it.
func (t *SyncTask) ResolveColumnGroups(segmentInfo *metacache.SegmentInfo) []storagecommon.ColumnGroup {
	return t.getColumnGroups(segmentInfo)
}

// ResolveAndFreezeColumnGroups derives this batch's layout, agrees on one with
// any concurrent task of the same segment, and freezes the winner on this task.
//
// It exists for callers that build tasks outside the write buffer — import,
// which submits per file and can have two files targeting one segment. They used
// to open-code the derive / compare-and-set / read-back-the-winner sequence,
// which meant the "first task decides the layout" rule had two implementations
// to keep in step. This is the one.
func (t *SyncTask) ResolveAndFreezeColumnGroups(segmentInfo *metacache.SegmentInfo) error {
	columnGroups, err := t.resolveSharedColumnGroups(segmentInfo)
	if err != nil {
		return err
	}
	t.WithFrozenColumnGroups(columnGroups)
	return nil
}

func (t *SyncTask) WithFrozenColumnGroups(columnGroups []storagecommon.ColumnGroup) *SyncTask {
	t.preparedColumns = cloneColumnGroups(columnGroups)
	t.columnsFrozen = true
	return t
}

func cloneColumnGroups(columnGroups []storagecommon.ColumnGroup) []storagecommon.ColumnGroup {
	if columnGroups == nil {
		return nil
	}
	cloned := make([]storagecommon.ColumnGroup, len(columnGroups))
	for i, group := range columnGroups {
		cloned[i] = group
		cloned[i].Columns = append([]int(nil), group.Columns...)
		cloned[i].Fields = append([]int64(nil), group.Fields...)
	}
	return cloned
}

func resolveColumnGroups(segmentInfo *metacache.SegmentInfo, schema *schemapb.CollectionSchema, segmentID int64, calcColumnStats func() map[int64]storagecommon.ColumnStats) []storagecommon.ColumnGroup {
	// column group only needed for storage v2/v3 segments
	if segmentInfo.GetStorageVersion() != storage.StorageV2 && segmentInfo.GetStorageVersion() != storage.StorageV3 {
		return nil
	}

	// empty pack
	if schema == nil {
		return nil
	}

	allFields := typeutil.GetAllFieldSchemas(schema)

	// use previous split if already exists
	if currentSplit := cloneColumnGroups(segmentInfo.GetCurrentSplit()); currentSplit != nil {
		for _, cg := range currentSplit {
			// legacy split found, use legacy policy
			if len(cg.Fields) == 0 {
				result := storagecommon.SplitColumns(allFields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewLocalFormatPolicy(), storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
				result = storagecommon.FillColumnGroupFormats(result, paramtable.Get().DataNodeCfg.StorageFormat.GetValue())
				mlog.Info(context.TODO(), "use legacy split policy", mlog.FieldSegmentID(segmentID), mlog.Stringers("columnGroups", result))
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
		if segmentInfo.GetStorageVersion() == storage.StorageV3 && segmentInfo.ManifestPath() != "" {
			return currentSplit
		}
		return storagecommon.FillColumnGroupFormats(currentSplit, paramtable.Get().DataNodeCfg.StorageFormat.GetValue())
	}

	policies := storagecommon.DefaultPolicies()
	stats := map[int64]storagecommon.ColumnStats{}
	if calcColumnStats != nil {
		stats = calcColumnStats()
	}
	result := storagecommon.SplitColumns(allFields, stats, policies...)
	result = storagecommon.FillColumnGroupFormats(result, paramtable.Get().DataNodeCfg.StorageFormat.GetValue())
	mlog.Info(context.TODO(), "sync new split columns", mlog.FieldSegmentID(segmentID), mlog.Stringers("columnGroups", result))
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

// BatchRows is the row count this attempt carries. The write buffer needs it
// to pair StartSyncing with Abort/FinishSyncing across a re-submission.
func (t *SyncTask) BatchRows() int64 {
	return t.batchRows
}

func (t *SyncTask) releasePayload() {
	if t.payload == nil {
		if t.pack != nil {
			t.pack.ReleaseData()
		}
		return
	}
	t.payload.releaseOnce.Do(func() {
		if t.pack != nil {
			t.pack.ReleaseData()
		}
		released := t.payload.insertBytes.Swap(0) + t.payload.deleteBytes.Swap(0)
		if t.payload.onRelease != nil {
			t.payload.onRelease(released)
		}
	})
}

// PayloadBytes is the write-buffer row payload still retained by this task.
// Metadata-only retries report zero after the object data has been written.
func (t *SyncTask) PayloadBytes() int64 {
	if t.payload == nil {
		return 0
	}
	return t.payload.insertBytes.Load() + t.payload.deleteBytes.Load()
}

func (t *SyncTask) InsertPayloadBytes() int64 {
	if t.payload == nil {
		return 0
	}
	return t.payload.insertBytes.Load()
}

func (t *SyncTask) DeletePayloadBytes() int64 {
	if t.payload == nil {
		return 0
	}
	return t.payload.deleteBytes.Load()
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

func (t *SyncTask) IsDrop() bool {
	return t.pack.isDrop
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
