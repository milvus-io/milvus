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

package compactor

import (
	"context"
	sio "io"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// hashSplitCompactionTask rewrites one flushed segment of a shard split's
// source into the split's two targets.
//
// A collection placed by primary key hashes its rows all over the key space,
// so the split boundary cuts through every segment of the source. Each input
// segment is therefore read once and its rows are written out to one of two
// per-target writers, chosen by the residue of the row's primary key against
// the post-split routing modulus (hashSplitPartitioner).
//
// Each writer is bound to its target's vchannel, so every output segment
// belongs to exactly one shard and satisfies the single-InsertChannel
// constraint that made sharing a segment impossible in the first place.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §6.3.
type hashSplitCompactionTask struct {
	binlogIO io.BinlogIO
	// allocator hands out the output segment and log ids, pre-allocated by
	// datacoord so the rewrite needs no callback mid-flight.
	plan             *datapb.CompactionPlan
	compactionParams compaction.Params

	ctx    context.Context
	cancel context.CancelFunc
	tr     *timerecord.TimeRecorder

	collectionID int64
	partitionID  int64
	maxRows      int64
	currentTime  time.Time
	// input is the one data segment this plan rewrites, resolved by preCompact.
	input *datapb.CompactionSegmentBinlogs
	// ttlFieldID is the collection's row-level TTL field, or -1 when it has
	// none. Resolved once, as a mix compaction does.
	ttlFieldID int64
}

var _ Compactor = (*hashSplitCompactionTask)(nil)

// NewHashSplitCompactionTask builds the rewrite task of one shard-split plan.
func NewHashSplitCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
) *hashSplitCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &hashSplitCompactionTask{
		ctx:              ctx,
		cancel:           cancel,
		binlogIO:         binlogIO,
		plan:             plan,
		compactionParams: compactionParams,
		tr:               timerecord.NewTimeRecorder("hash split compaction"),
		currentTime:      time.Now(),
		ttlFieldID:       getTTLFieldID(plan.GetSchema()),
	}
}

func (t *hashSplitCompactionTask) Complete()                    { t.cancel() }
func (t *hashSplitCompactionTask) Stop()                        { t.cancel() }
func (t *hashSplitCompactionTask) GetPlanID() typeutil.UniqueID { return t.plan.GetPlanID() }
func (t *hashSplitCompactionTask) GetChannelName() string       { return t.plan.GetChannel() }
func (t *hashSplitCompactionTask) GetCollection() typeutil.UniqueID {
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *hashSplitCompactionTask) GetCompactionType() datapb.CompactionType {
	return datapb.CompactionType_HashSplitCompaction
}

func (t *hashSplitCompactionTask) GetSlotUsage() int64 { return t.plan.GetSlotUsage() }

func (t *hashSplitCompactionTask) GetStorageConfig() *indexpb.StorageConfig {
	return t.compactionParams.StorageConfig
}

// preCompact validates the plan and caches the collection/partition scope.
//
// A hash split rewrite is strictly one data segment per plan: datacoord
// dispatches one plan per source segment so that a lost plan retries exactly
// that segment, and so the pre-allocated output ids are unambiguous.
func (t *hashSplitCompactionTask) preCompact() error {
	t.input = nil
	if len(t.plan.GetSegmentBinlogs()) != 1 {
		return merr.WrapErrServiceInternalMsg(
			"a hash split rewrite takes exactly one input segment, got %d", len(t.plan.GetSegmentBinlogs()))
	}
	t.input = t.plan.GetSegmentBinlogs()[0]
	// A shard split has exactly two targets. That they tile the input's
	// residues with no overlap is not a count and is checked by the partitioner.
	if len(t.plan.GetHashSplitTargets()) != 2 {
		return merr.WrapErrServiceInternalMsg(
			"a hash split rewrite takes exactly two targets, got %d", len(t.plan.GetHashSplitTargets()))
	}
	if t.plan.GetPreAllocatedSegmentIDs() == nil || t.plan.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
		return merr.WrapErrServiceInternalMsg("invalid pre-allocated segment id range")
	}
	t.collectionID = t.input.GetCollectionID()
	t.partitionID = t.input.GetPartitionID()
	t.maxRows = t.plan.GetTotalRows()
	return nil
}

// Compact rewrites the input segment into one output segment per target.
func (t *hashSplitCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, "HashSplitCompact")
	defer span.End()

	if err := t.preCompact(); err != nil {
		return nil, err
	}
	// datacoord stores binlogs with the path stripped down to a log id
	// (CompressSaveBinlogPaths), so the plan arrives carrying ids, not paths,
	// and every compactor that reads binlogs rebuilds them first.
	if err := binlog.DecompressCompactionBinlogsWithRootPath(
		t.compactionParams.StorageConfig.GetRootPath(), t.plan.GetSegmentBinlogs()); err != nil {
		return nil, err
	}
	logger := mlog.With(
		mlog.Int64("planID", t.plan.GetPlanID()),
		mlog.Int64("collectionID", t.collectionID),
		mlog.String("sourceChannel", t.plan.GetChannel()))

	partitioner, err := newHashSplitPartitioner(t.plan.GetHashSplitModulus(), t.plan.GetHashSplitTargets())
	if err != nil {
		return nil, err
	}
	writers, err := t.newTargetWriters(ctx, partitioner)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, w := range writers {
			w.Close()
		}
	}()

	rowsPerTarget, err := t.rewriteSegment(ctx, t.input, partitioner, writers)
	if err != nil {
		return nil, err
	}

	segments := make([]*datapb.CompactionSegment, 0, len(writers))
	for i, w := range writers {
		if err := w.Close(); err != nil {
			return nil, err
		}
		out := w.GetCompactionSegments()
		// A target with no rows produces no segment; that is legal (an empty
		// half), and datacoord drops the input all the same.
		segments = append(segments, out...)
		logger.Info(ctx, "hash split rewrite target done",
			mlog.String("targetVChannel", partitioner.TargetVChannel(i)),
			mlog.Int64("rows", rowsPerTarget[i]),
			mlog.Int("segments", len(out)))
	}

	logger.Info(ctx, "hash split rewrite finished",
		mlog.Int64("sourceSegmentID", t.input.GetSegmentID()),
		mlog.Duration("elapse", t.tr.RecordSpan()))

	return &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Channel:  t.GetChannelName(),
		Segments: segments,
		Type:     t.GetCompactionType(),
	}, nil
}

// newTargetWriters builds one writer per target, each bound to that target's
// vchannel so its output segments belong to that shard.
//
// The pre-allocated segment id range is split evenly across the targets, so no
// two writers ever hand out the same segment id.
func (t *hashSplitCompactionTask) newTargetWriters(
	ctx context.Context,
	partitioner *hashSplitPartitioner,
) ([]*MultiSegmentWriter, error) {
	idRange := t.plan.GetPreAllocatedSegmentIDs()
	logIDRange := t.plan.GetPreAllocatedLogIDs()
	n := int64(partitioner.NumTargets())
	perTarget := (idRange.GetEnd() - idRange.GetBegin()) / n
	if perTarget < 1 {
		return nil, merr.WrapErrServiceInternalMsg(
			"pre-allocated segment id range [%d, %d) is too small for %d targets",
			idRange.GetBegin(), idRange.GetEnd(), n)
	}

	writers := make([]*MultiSegmentWriter, 0, partitioner.NumTargets())
	for i := range partitioner.NumTargets() {
		begin := idRange.GetBegin() + int64(i)*perTarget
		end := begin + perTarget
		segIDAlloc := allocator.NewLocalAllocator(begin, end)
		// Every writer draws log ids from the whole range. They may hand out the
		// same log id, which is harmless: a binlog path is namespaced by segment
		// id (metautil.BuildInsertLogPath), and the writers' segment id ranges
		// above are disjoint, so two identical log ids cannot name one object.
		// Carving the log range per target instead would be wrong — the number
		// of log ids a target needs has nothing to do with its share of the
		// segment ids, and a target that outgrew its slice would fail mid-write.
		logIDAlloc := allocator.NewLocalAllocator(logIDRange.GetBegin(), logIDRange.GetEnd())
		w, err := NewMultiSegmentWriter(ctx,
			t.binlogIO, NewCompactionAllocator(segIDAlloc, logIDAlloc),
			t.plan.GetMaxSize(), t.plan.GetSchema(), t.compactionParams,
			t.maxRows, t.partitionID, t.collectionID,
			// Each writer is bound to its target's vchannel: this is what
			// attributes the output segments to the right shard.
			partitioner.TargetVChannel(i), 4096,
			// Storage v2/v3 refuse a writer without a storage config, so these
			// are not optional — a rewrite that omits them fails at the first
			// output. The reader half already passes the same config.
			storage.WithStorageConfig(t.compactionParams.StorageConfig),
			storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI),
			storage.WithWriterFormat(t.compactionParams.GetStorageFormat()))
		if err != nil {
			for _, prev := range writers {
				prev.Close()
			}
			return nil, err
		}
		writers = append(writers, w)
	}
	return writers, nil
}

// rewriteSegment streams one source segment and writes each surviving row to
// the writer of the target that owns its primary key.
//
// Deleted and expired rows are dropped exactly as an ordinary compaction drops
// them, so the rewrite folds the input's deltalogs instead of carrying them
// over: the commit drops the input, and a delete missing from the outputs here
// is a delete lost for good.
func (t *hashSplitCompactionTask) rewriteSegment(
	ctx context.Context,
	seg *datapb.CompactionSegmentBinlogs,
	partitioner *hashSplitPartitioner,
	writers []*MultiSegmentWriter,
) ([]int64, error) {
	rowsPerTarget := make([]int64, len(writers))
	sinks := make([]hashSplitSink, len(writers))
	for i, w := range writers {
		sinks[i] = w
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		return nil, err
	}

	delta, err := compaction.ComposeDeleteFromDeltalogs(ctx, pkField.DataType, seg,
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		return nil, err
	}
	entityFilter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime, seg.GetCommitTimestamp())

	reader, existingFields, err := newCompactionSegmentRecordReader(ctx, seg, t.plan.GetSchema(), t.compactionParams.StorageConfig,
		storage.WithCollectionID(t.collectionID),
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithVersion(seg.GetStorageVersion()),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	materializer, err := NewRecordMaterializer(t.plan.GetSchema(), t.plan.GetSchema().GetFunctions(), existingFields)
	if err != nil {
		return nil, err
	}
	defer materializer.Close()

	for {
		r, err := reader.Next()
		if err != nil {
			if err == sio.EOF {
				break
			}
			return nil, err
		}
		r, err = materializer.Wrap(r)
		if err != nil {
			return nil, err
		}
		err = t.routeRecord(r, pkField, seg.GetCommitTimestamp(), entityFilter, partitioner, sinks, rowsPerTarget)
		// The arrays the materializer derived for this batch are released on
		// every path; the base record stays owned by the reader.
		cleanupMaterializedRecord(r)
		if err != nil {
			return nil, err
		}
	}
	return rowsPerTarget, nil
}

// routeRecord splits one record's rows between the target sinks.
//
// The target is the owner of the row's primary key residue. The delete and TTL
// filter is keyed on the primary key, the row's timestamp and the collection's
// TTL field, exactly as in a mix compaction.
//
// Rows are appended per target in contiguous runs and written in one batch per
// target per record, so the rewrite keeps the columnar batching an ordinary
// compaction relies on. Each batch carries the input's commit timestamp as its
// row timestamps: datacoord publishes the outputs with commit_timestamp 0, so a
// row imported before its commit must not keep the older binlog timestamp.
func (t *hashSplitCompactionTask) routeRecord(
	r storage.Record,
	pkField *schemapb.FieldSchema,
	commitTs uint64,
	entityFilter compaction.EntityFilter,
	partitioner *hashSplitPartitioner,
	sinks []hashSplitSink,
	rowsPerTarget []int64,
) error {
	pkArray := r.Column(pkField.FieldID)
	tsArray := r.Column(common.TimeStampField).(*array.Int64)
	var ttlArray *array.Int64
	if t.ttlFieldID >= common.StartOfUserFieldID {
		col, ok := r.Column(t.ttlFieldID).(*array.Int64)
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"record carries no int64 column for the TTL field %d", t.ttlFieldID)
		}
		ttlArray = col
	}

	builders := make([]*storage.RecordBuilder, len(sinks))
	// Released on every path. A builder whose Append failed may hold a partial
	// row, so it is only ever released, never built.
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	// The current run is rows [runStart, i) of one target.
	runTarget, runStart := -1, 0
	flushRun := func(end int) error {
		if runTarget < 0 || end <= runStart {
			return nil
		}
		if builders[runTarget] == nil {
			builders[runTarget] = storage.NewRecordBuilder(t.plan.GetSchema())
		}
		return builders[runTarget].Append(r, runStart, end)
	}

	for i := range r.Len() {
		ts := typeutil.Timestamp(tsArray.Value(i))

		var pk any
		switch pkField.DataType {
		case schemapb.DataType_Int64:
			pk = pkArray.(*array.Int64).Value(i)
		case schemapb.DataType_VarChar:
			pk = pkArray.(*array.String).Value(i)
		default:
			return merr.WrapErrServiceInternalMsg(
				"unsupported primary key type %v for a hash split rewrite", pkField.DataType)
		}
		targetIdx, err := partitioner.Route(pk)
		if err != nil {
			return err
		}

		expireTs := int64(-1)
		if ttlArray != nil && ttlArray.IsValid(i) {
			expireTs = ttlArray.Value(i)
		}
		// Deleted and expired rows are dropped, exactly as an ordinary
		// compaction drops them: the rewrite folds the source's deltalog rather
		// than carrying it to the targets.
		if entityFilter.Filtered(pk, ts, expireTs) {
			if err := flushRun(i); err != nil {
				return err
			}
			runTarget = -1
			continue
		}
		if targetIdx != runTarget {
			if err := flushRun(i); err != nil {
				return err
			}
			runTarget, runStart = targetIdx, i
		}
	}
	if err := flushRun(r.Len()); err != nil {
		return err
	}

	for idx, b := range builders {
		if b == nil || b.GetRowNum() == 0 {
			continue
		}
		rows := b.GetRowNum()
		if err := writeHashSplitBatch(sinks[idx], b.Build(), commitTs); err != nil {
			return err
		}
		rowsPerTarget[idx] += int64(rows)
	}
	return nil
}

// writeHashSplitBatch writes one built batch with its row timestamps set to
// commitTs (unchanged when commitTs is 0), and releases what it built.
func writeHashSplitBatch(sink hashSplitSink, rec storage.Record, commitTs uint64) error {
	defer rec.Release()
	out := overwriteRecordTimestamps(rec, commitTs)
	if out != rec {
		defer out.Release()
	}
	return sink.Write(out)
}
