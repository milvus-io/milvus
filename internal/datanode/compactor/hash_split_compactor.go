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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// hashSplitCompactionTask rewrites the sealed segments of a hash-routed shard
// into its two shard-split targets.
//
// A hash-routed shard's rows hash all over the key space, so unlike the
// namespace split — where a namespace's data is confined to one shard and a
// segment can be relabeled whole — the split boundary cuts through every
// segment. Each input segment is therefore read once and its rows are written
// out to one of two per-target writers, chosen by the targets' routing
// predicates (a new hash bit for a doubling, pk equality for a hot-key carve).
//
// Each writer is bound to its target's vchannel, so every output segment
// belongs to exactly one shard and satisfies the single-InsertChannel
// constraint that made sharing a segment impossible in the first place.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §6.5.
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
// A hash split rewrite is strictly one input segment per plan: datacoord
// dispatches one plan per source segment so that a lost plan retries exactly
// that segment, and so the pre-allocated output ids (one per target) are
// unambiguous.
func (t *hashSplitCompactionTask) preCompact() error {
	if len(t.plan.GetSegmentBinlogs()) != 1 {
		return merr.WrapErrParameterInvalidMsg(
			"a hash split rewrite takes exactly one input segment, got %d",
			len(t.plan.GetSegmentBinlogs()))
	}
	// Two for a doubling, M for a rehash to M shards. The real constraint —
	// that the targets tile the input's key space with no gap or overlap — is
	// not a count and is checked by the partitioner, which derives the same
	// routing table the write path uses.
	if len(t.plan.GetHashSplitTargets()) < 2 {
		return merr.WrapErrParameterInvalidMsg(
			"a hash split rewrite needs at least two targets, got %d",
			len(t.plan.GetHashSplitTargets()))
	}
	if t.plan.GetPreAllocatedSegmentIDs() == nil || t.plan.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
		return merr.WrapErrParameterInvalidMsg("invalid pre-allocated segment id range")
	}
	seg := t.plan.GetSegmentBinlogs()[0]
	t.collectionID = seg.GetCollectionID()
	t.partitionID = seg.GetPartitionID()
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

	rowsPerTarget, err := t.rewriteSegment(ctx, t.plan.GetSegmentBinlogs()[0], partitioner, writers)
	if err != nil {
		return nil, err
	}

	segments := make([]*datapb.CompactionSegment, 0, len(writers))
	for i, w := range writers {
		if err := w.Close(); err != nil {
			return nil, err
		}
		out := w.GetCompactionSegments()
		// A target with no rows still produces no segment; that is legal (an
		// empty half), and the split's completion check counts committed
		// outputs, not rows.
		segments = append(segments, out...)
		logger.Info(ctx, "hash split rewrite target done",
			mlog.String("targetVChannel", partitioner.TargetVChannel(i)),
			mlog.Int64("rows", rowsPerTarget[i]),
			mlog.Int("segments", len(out)))
	}

	logger.Info(ctx, "hash split rewrite finished",
		mlog.Int64("sourceSegmentID", t.plan.GetSegmentBinlogs()[0].GetSegmentID()),
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
// vchannel so its output segments belong to that shard. Two writers for a
// doubling, M for a rehash to M shards.
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
		return nil, merr.WrapErrParameterInvalidMsg(
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
// them, so the rewrite also folds the source's deltalog instead of carrying it
// over.
func (t *hashSplitCompactionTask) rewriteSegment(
	ctx context.Context,
	seg *datapb.CompactionSegmentBinlogs,
	partitioner *hashSplitPartitioner,
	writers []*MultiSegmentWriter,
) ([]int64, error) {
	rowsPerTarget := make([]int64, len(writers))

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
		if err := t.routeRecord(r, pkField, entityFilter, partitioner, writers, rowsPerTarget); err != nil {
			return nil, err
		}
	}
	return rowsPerTarget, nil
}

// routeRecord splits one record's rows between the target writers.
//
// Rows are accumulated per target and written in one Append per target per
// record, rather than row by row, so the rewrite keeps the columnar batching
// an ordinary compaction relies on.
func (t *hashSplitCompactionTask) routeRecord(
	r storage.Record,
	pkField *schemapb.FieldSchema,
	entityFilter compaction.EntityFilter,
	partitioner *hashSplitPartitioner,
	writers []*MultiSegmentWriter,
	rowsPerTarget []int64,
) error {
	pkArray := r.Column(pkField.FieldID)
	tsArray := r.Column(common.TimeStampField).(*array.Int64)

	builders := make([]*storage.RecordBuilder, len(writers))
	for i := range r.Len() {
		ts := typeutil.Timestamp(tsArray.Value(i))

		var (
			targetIdx int
			err       error
			pk        any
		)
		switch pkField.DataType {
		case schemapb.DataType_Int64:
			v := pkArray.(*array.Int64).Value(i)
			pk = v
			targetIdx, err = partitioner.RouteInt64(v)
		case schemapb.DataType_VarChar:
			v := pkArray.(*array.String).Value(i)
			pk = v
			targetIdx, err = partitioner.RouteVarChar(v)
		default:
			return merr.WrapErrParameterInvalidMsg(
				"unsupported primary key type %v for a hash split rewrite", pkField.DataType)
		}
		if err != nil {
			return err
		}

		// Deleted and expired rows are dropped, exactly as an ordinary
		// compaction drops them: the rewrite folds the source's deltalog rather
		// than carrying it to the targets.
		if entityFilter.Filtered(pk, ts, -1) {
			continue
		}

		if builders[targetIdx] == nil {
			builders[targetIdx] = storage.NewRecordBuilder(t.plan.GetSchema())
		}
		builders[targetIdx].Append(r, i, i+1)
		rowsPerTarget[targetIdx]++
	}

	for idx, b := range builders {
		if b == nil {
			continue
		}
		rec := b.Build()
		if rec.Len() == 0 {
			continue
		}
		if err := writers[idx].Write(rec); err != nil {
			return err
		}
	}
	return nil
}
