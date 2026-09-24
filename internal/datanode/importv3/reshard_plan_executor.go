// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the execution of one ReshardTask run. Three objects split
// the work, each hiding its own complexity:
//
//   - reshardPlanExecutor: the orchestration. Reads sources through the prepare
//     stage, routes batches into buckets, applies the flush/spill triggers,
//     and publishes the manifest.
//   - reshardFlushPool: the detached fragment writes. Owns the concurrency
//     limit, the write goroutines' lifecycle, first-error propagation (which
//     cancels the read side), and the seq-ordered outcomes.
//   - reshardPrepareStage: one source's read/normalize/function pipeline,
//     running one batch ahead of the routing side.
//
// Everything they share at run scope lives in reshardRunConfig (immutable)
// plus the runner's mutable fields (buckets, resident counter, manifest,
// counters, fragment sequence).

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// importV3SpillRootDir is the node-local root of Import V3 reshard spill
// files. It must live outside the import_v3 prefix: with
// common.storageType=local the chunk-manager root and the local storage path
// are the same directory, and reshard's durable output (fragments and
// manifests) is written under <root>/import_v3/<job>/... — a startup cleanup
// of that prefix would delete finished work.
const importV3SpillRootDir = "import_v3_spill"

// CleanImportV3Prefixes removes the local Import V3 spill root left by previous
// process runs. DataNode V3 task state is memory-only and is never recovered
// from local spill files, so after restart every file under the spill root is
// garbage by definition. Only the spill root is touched: the import_v3 prefix
// is shared with durable reshard output under local storage (see
// importV3SpillRootDir).
func CleanImportV3Prefixes() {
	root := path.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir)
	if err := os.RemoveAll(root); err != nil {
		mlog.Warn(context.TODO(), "failed to clean import v3 local root", mlog.String("root", root), mlog.Err(err))
		return
	}
	mlog.Info(context.TODO(), "cleaned import v3 local root", mlog.String("root", root))
}

// executeReshardPlan is the entry point of one ReshardTask run: it validates
// the plan, derives the run configuration and drives the reshardPlanExecutor.
// The pipeline itself lives below.
func executeReshardPlan(ctx context.Context, cm storage.ChunkManager, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, pluginContext *indexcgopb.StoragePluginContext, metrics *Metrics, progress *ReshardProgress) error {
	executor, err := newReshardPlanExecutor(cm, req, plan, pluginContext, metrics, progress)
	if err != nil {
		return err
	}
	return executor.execute(ctx)
}

// reshardRunConfig is the immutable configuration of one reshard run, derived
// once from the request, the plan and the live parameters.
type reshardRunConfig struct {
	req             *datapb.ReshardTaskRequest
	plan            *datapb.ReshardTaskPlan
	cm              storage.ChunkManager
	pluginContext   *indexcgopb.StoragePluginContext
	temporarySchema *schemapb.CollectionSchema
	sortFields      []int64
	bufferSize      int64
	fragmentTarget  int64
	slot            int64
	memoryBudget    int64
	// Backup sources already carry every function output column; ordinary
	// sources get theirs computed during the run so fragments are uniform
	// either way.
	runFunctions           bool
	memModel               reshardmem.Model
	effectiveFragmentInput int64
	residentBudget         int64
	fragmentOverhead       int64
	numPartitions          int64
	spillRoot              string
	spillStreams           int
}

// reshardPlanExecutor executes one ReshardTask run end to end.
type reshardPlanExecutor struct {
	reshardRunConfig

	ctx          context.Context // run ctx, set by run(); bounds every stage
	spillManager *SpillManager
	flushPool    *reshardFlushPool
	buckets      map[reshardBucketKey]*reshardBucket
	manifest     *datapb.ReshardManifest
	counters     reshardRunCounters
	// stageTimings is the prepare-stage phase breakdown, written by the stage
	// goroutine and read by the summary once every stage is joined.
	stageTimings reshardStageTimings
	// Accounted live bytes: every routed batch still sitting in a bucket plus
	// every fragment input a detached write has not released yet. The routing
	// side adds and spills, the write goroutines release, hence atomic.
	resident atomic.Int64
	// bucketResident is the part of resident still held by the buckets (not yet
	// flushed or spilled). It is the quantity the whole-task ceiling bounds:
	// bytes owned by a detached fragment write are bounded by the flush pool
	// concurrency the model already charges, and are released by the write
	// itself, so counting them here would spill live bucket tails to reclaim
	// memory the flush path did not hold.
	bucketResident atomic.Int64
	fragmentSeq    int64
	metrics        *Metrics
	progress       *ReshardProgress
}

// newReshardPlanExecutor validates the plan and derives the run configuration. It
// performs no IO: spill setup and the flush pool are created by run.
func newReshardPlanExecutor(cm storage.ChunkManager, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, pluginContext *indexcgopb.StoragePluginContext, metrics *Metrics, progress *ReshardProgress) (*reshardPlanExecutor, error) {
	temporarySchema := plan.GetTempSchema()
	sortFields, err := SortFields(plan.GetSort(), temporarySchema)
	if err != nil {
		return nil, err
	}
	bufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	fragmentTarget := plan.GetFragmentSize()
	// Contract: DataCoord validates a positive fragment size at plan build time.
	// A non-positive target would flush every non-empty bucket after every batch
	// (fragment count = batches x buckets), so fail loudly at the boundary too.
	if fragmentTarget <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("invalid ReshardTask fragment size %d", fragmentTarget)
	}
	slot := req.GetSlot()
	if slot <= 0 {
		slot = 1
	}
	// The task's slot budget is charged in the node's own slot unit, the same
	// one DataCoord estimated the slot with (reshardmem.MemoryPerSlot), not the
	// import-specific memoryLimitPerSlot.
	memoryBudget := slot * reshardmem.MemoryPerSlot(paramtable.Get().DataNodeCfg.WorkerSlotUnit.GetAsInt64())
	// Fixed bucket-to-shard mapping: every range of one bucket lands in the
	// same spill file, so reading a bucket back never touches the others and
	// the file/fd count stays at min(buckets, reshardSpillMaxStreams).
	bucketCount := int64(len(plan.GetVchannels()) * len(plan.GetPartitions()))
	memModel := reshardmem.Model{
		ReadBuffer:       bufferSize,
		FragmentTarget:   fragmentTarget,
		FlushConcurrency: paramtable.Get().DataCoordCfg.ReshardFlushConcurrency.GetAsInt64(),
		ExpansionFactor:  paramtable.Get().DataCoordCfg.ReshardMemoryExpansionFactor.GetAsFloat(),
	}
	// The task's bucket-resident ceiling: the same total the shared model
	// charges DataCoord (min(buckets, bucketCap) x fragmentTarget), applied to
	// the bytes still held by the buckets. One extra bucket per overflow is
	// spilled (the largest first) instead of forcing every bucket under an
	// equal per-bucket share, so a skewed partition hash spends the spill
	// budget on the hot buckets and keeps cold buckets resident instead of
	// spilling them regardless of the free budget. Detached fragment writes
	// are bounded by the flush pool concurrency and released by the writes, so
	// they are outside this ceiling and never trigger a spill.
	bucketCap := paramtable.Get().DataCoordCfg.ReshardResidentBucketCap.GetAsInt64()
	residentBudget := min(bucketCount, bucketCap) * fragmentTarget
	// Every routed fragment carries structural live-heap overhead on top of
	// its decoded bytes (FieldData wrappers, map entries, slice capacity and
	// allocator rounding -- see reshardmem.FragmentFieldOverhead). Accounting
	// only GetMemorySize systematically undercharges the real resident set,
	// worst when a high bucket count shreds every source batch into tiny
	// fragments; the same term is charged by DataCoord's WorkingSet.
	nFields := int64(len(typeutil.GetAllFieldSchemas(temporarySchema)))
	fragmentOverhead := reshardmem.FragmentOverhead(nFields)
	return &reshardPlanExecutor{
		reshardRunConfig: reshardRunConfig{
			req:                    req,
			plan:                   plan,
			cm:                     cm,
			pluginContext:          pluginContext,
			temporarySchema:        temporarySchema,
			sortFields:             sortFields,
			bufferSize:             bufferSize,
			fragmentTarget:         fragmentTarget,
			slot:                   slot,
			memoryBudget:           memoryBudget,
			runFunctions:           !plan.GetBackup(),
			memModel:               memModel,
			effectiveFragmentInput: memModel.SortInput(memoryBudget),
			residentBudget:         residentBudget,
			fragmentOverhead:       fragmentOverhead,
			numPartitions:          int64(len(plan.GetPartitions())),
			spillRoot: path.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir,
				strconv.FormatInt(req.GetJobId(), 10), strconv.FormatInt(req.GetTaskId(), 10), strconv.FormatInt(req.GetRunId(), 10)),
			spillStreams: int(min(bucketCount, int64(paramtable.Get().DataNodeCfg.ReshardSpillMaxStreams.GetAsInt()))),
		},
		buckets:  make(map[reshardBucketKey]*reshardBucket),
		manifest: &datapb.ReshardManifest{},
		metrics:  metrics,
		progress: progress,
	}, nil
}

// run executes the reshard pipeline: read every source through the prepare
// stage, route batches into buckets, flush full buckets as detached writes,
// then publish the manifest once every write has landed.
func (r *reshardPlanExecutor) execute(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "ImportV3-Reshard",
		trace.WithAttributes(
			attribute.Int64("job_id", r.req.GetJobId()),
			attribute.Int64("task_id", r.req.GetTaskId()),
			attribute.Int64("run_id", r.req.GetRunId()),
		))
	defer span.End()
	r.ctx = ctx
	runStart := time.Now()
	spillManager, err := NewSpillManager(r.spillRoot, r.temporarySchema, r.spillStreams)
	if err != nil {
		return err
	}
	r.spillManager = spillManager
	defer func() { _ = r.spillManager.Close() }()
	r.flushPool = newReshardFlushPool(ctx, &r.reshardRunConfig, r.spillManager, &r.resident)
	// Registered after the spillManager.Close defer, so the run cancels, joins
	// every write, and only then closes the spill manager (which removes its
	// directory).
	defer r.flushPool.shutdown()

	for _, source := range r.plan.GetSources() {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A detached write that failed has already canceled the flush ctx; stop
		// instead of reading the next source for nothing.
		if err := r.flushPool.Err(); err != nil {
			return err
		}
		if err := r.readSource(source); err != nil {
			return err
		}
		debug.FreeOSMemory()
	}
	if err := r.flushAll(); err != nil {
		return err
	}
	// Every fragment write is detached, so the run joins them before it
	// publishes: the manifest is complete only once the last fragment is on
	// object storage, and the spill manager has to outlive its last reader.
	if err := r.flushPool.wait(); err != nil {
		return err
	}
	r.collectOutcomes()
	r.counters.runWall = time.Since(runStart)
	r.observeRunMetrics()
	r.logSummary()
	return publishReshardManifest(ctx, r.cm, r.req, r.manifest)
}

// observeRunMetrics reports one finished run's volume and phase latencies.
func (r *reshardPlanExecutor) observeRunMetrics() {
	r.metrics.ObserveReshardRun(ReshardRunObservation{
		Rows:         r.counters.rows,
		LogicalBytes: r.counters.logicalBytes,
		WrittenBytes: r.counters.writtenBytes,
		SpillBytes:   r.counters.spillBytes,
		ReadWait:     r.counters.readWait,
		RouteCost:    r.counters.routeCost,
		FlushBlock:   r.counters.flushBlock,
		FlushWall:    r.flushPool.flushWallDuration(),

		PrepareRead:      time.Duration(r.stageTimings.read.Load()),
		PrepareNormalize: time.Duration(r.stageTimings.normalize.Load()),
		PrepareFunctions: time.Duration(r.stageTimings.functions.Load()),
		PrepareSend:      time.Duration(r.stageTimings.send.Load()),

		SortRead:  r.counters.sortReadCost,
		Sort:      r.counters.sortCost,
		SortWrite: r.counters.sortWriteCost,
	})
}

// readSource prepares one source through the prepare stage, routing every
// prepared batch and detaching a write per full bucket.
func (r *reshardPlanExecutor) readSource(source *datapb.SourceFileSpec) error {
	if source == nil || source.GetFile() == nil {
		return merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
	}
	// The source context bounds this reader's lifetime: canceling it
	// interrupts an in-flight Read (the reader's IO is bound to its
	// construction ctx), so any failure return below stops the prepare
	// stage without waiting out a slow or hung read. It derives from
	// the flush ctx, not the run ctx, so a failed fragment write also
	// cuts the read side short instead of letting it read the rest of
	// the source for a run that is already failed.
	sourceCtx, cancelSource := context.WithCancel(r.flushPool.ctx)
	defer cancelSource()
	reader, err := newReshardSourceReader(sourceCtx, r.cm, r.plan.GetSchema(), source, r.bufferSize, r.req.GetStorageConfig(), r.pluginContext)
	if err != nil {
		return err
	}
	defer reader.Close()
	stage := newReshardPrepareStage(sourceCtx, cancelSource, r.ctx, reader, source, r.plan.GetSchema(), r.runFunctions, &r.stageTimings)
	// Registered after reader.Close, so the stage cancels and joins first and
	// the goroutine never observes a closed reader.
	defer stage.close()

	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		// A detached write that failed has already canceled the flush ctx; stop
		// routing instead of reading the rest of the source for nothing.
		if err := r.flushPool.Err(); err != nil {
			return err
		}
		receiveStart := time.Now()
		batch, ok, err := stage.next()
		r.counters.readWait += time.Since(receiveStart)
		// A failed write cancels the flush ctx, and that cancel is what
		// unblocked this receive: report the write's error rather than the
		// source-side cancel it caused. The error is stored before the cancel,
		// so once the cancel is observable the write error is too.
		if err := r.flushPool.Err(); err != nil {
			return err
		}
		if !ok {
			// Clean EOF: the prepare stage closes the channel after io.EOF.
			// A canceled run can land here too, but the ctx checks before
			// the next source and before each final flush catch it long
			// before anything is published.
			return nil
		}
		if err != nil {
			return merr.Wrapf(err, "prepare import source %d", source.GetFile().GetId())
		}
		routeStart := time.Now()
		err = r.routeBatch(batch)
		r.counters.routeCost += time.Since(routeStart)
		if err != nil {
			return err
		}
		// The batch is hashed into buckets now; count its rows as imported for
		// this source file before the flush/spill triggers run.
		r.progress.AddHashed(source.GetFile().GetId(), int64(batch.GetRowNum()))
		if err := r.spillByMemory(); err != nil {
			return err
		}
	}
}

// routeBatch hashes one prepared batch into buckets and applies the flush and
// spill triggers per destination bucket.
func (r *reshardPlanExecutor) routeBatch(batch *storage.InsertData) error {
	hashed, err := importv2.HashDataBySchema(r.temporarySchema, r.plan.GetVchannels(), r.plan.GetPartitions(), batch)
	if err != nil {
		return err
	}
	for channelOrdinal := range hashed {
		for partitionOrdinal, bucketData := range hashed[channelOrdinal] {
			if bucketData.GetRowNum() == 0 {
				continue
			}
			key := reshardBucketKey{channelOrdinal: channelOrdinal, partitionOrdinal: partitionOrdinal}
			b := r.buckets[key]
			if b == nil {
				b = &reshardBucket{vchannelOrdinal: channelOrdinal, partitionOrdinal: partitionOrdinal}
				r.buckets[key] = b
			}
			// Measure the routed batch once; the bucket entry carries both
			// sizes to resident accounting and the sort-input split so the
			// O(rows) string/JSON walk is not repeated per batch.
			logical := int64(bucketData.GetMemorySize())
			mem := logical + r.fragmentOverhead
			// Flush the accumulated segment BEFORE this crossing batch joins:
			// the flushed input is then strictly below fragmentTarget, so
			// splitReshardBucketForSort packs it into a single group and no
			// tiny overshoot fragment is cut for the batch that crossed the
			// threshold; that batch starts the next segment instead. Appending
			// first would leave the crossing batch as its own group.
			if b.logicalBytes > 0 && b.logicalBytes+logical >= r.fragmentTarget {
				// The bucket's accounted bytes travel with the detached writes
				// and are released there, not here: the memory stays live
				// until the fragment is on object storage.
				if err := r.flushBucket(b); err != nil {
					return err
				}
			}
			appendReshardBatch(b, bucketData, mem, logical)
			r.resident.Add(mem)
			r.bucketResident.Add(mem)
			r.counters.peakResident = max(r.counters.peakResident, r.resident.Load())
			// Whole-task bucket-resident ceiling: on overflow spill the largest
			// buckets back under the budget. Only the bytes still held by the
			// buckets count here -- a detached fragment write's input is
			// bounded by the flush pool concurrency the model already charges
			// and is released by the write, so charging it would spill live
			// bucket tails to reclaim memory the flush path never held.
			if r.bucketResident.Load() > r.residentBudget {
				if err := r.spill(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// flushBucket cuts one full bucket into sort groups, detaches a write per
// group and resets the bucket: its data now belongs to the writes, which
// release the accounted bytes as they finish. The spill ranges travel with
// their group, so a shard file is retired by the write that drained it.
func (r *reshardPlanExecutor) flushBucket(b *reshardBucket) error {
	groups := splitReshardBucketForSort(b, r.effectiveFragmentInput)
	for _, group := range groups {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		blocked, err := r.flushPool.dispatch(b.vchannelOrdinal, b.partitionOrdinal, group, r.fragmentSeq)
		r.counters.flushBlock += blocked
		if err != nil {
			return err
		}
		r.fragmentSeq++
	}
	// The bucket's accounted bytes move to the detached writes, which release
	// them on completion; they leave the bucket-resident ceiling here.
	r.bucketResident.Add(-b.bytes)
	*b = reshardBucket{vchannelOrdinal: b.vchannelOrdinal, partitionOrdinal: b.partitionOrdinal}
	return nil
}

// spillBucket appends one bucket's whole in-memory tail to the shared spill
// manager as a single indexed range and returns the accounted bytes freed. The
// bucket keeps its rows/logicalBytes: the fragment a bucket eventually flushes
// covers everything it ever received, in memory or on disk.
func (r *reshardPlanExecutor) spillBucket(b *reshardBucket) (int64, error) {
	items := make([]SpillBatch, 0, len(b.batches))
	for _, batch := range b.batches {
		items = append(items, SpillBatch{Data: batch.data, Bytes: batch.logical})
	}
	bucketOrdinal := int64(b.vchannelOrdinal)*r.numPartitions + int64(b.partitionOrdinal)
	spillRange, err := r.spillManager.Append(bucketOrdinal, items)
	if err != nil {
		return 0, err
	}
	b.ranges = append(b.ranges, spillRange)
	freed := b.bytes
	b.batches = nil
	b.bytes = 0
	r.bucketResident.Add(-freed)
	r.counters.spillRanges++
	r.counters.spillBytes += freed
	return freed, nil
}

// spillLargest is the dynamic-checkpoint spill: one bucket per call, the
// largest first, converging real free memory back above the floor.
func (r *reshardPlanExecutor) spillLargest() (int64, error) {
	var target *reshardBucket
	for _, b := range r.buckets {
		if b.bytes == 0 {
			continue
		}
		if target == nil || b.bytes > target.bytes ||
			(b.bytes == target.bytes && (b.vchannelOrdinal < target.vchannelOrdinal ||
				(b.vchannelOrdinal == target.vchannelOrdinal && b.partitionOrdinal < target.partitionOrdinal))) {
			target = b
		}
	}
	if target == nil {
		return 0, nil
	}
	return r.spillBucket(target)
}

// spillByMemory runs once per source batch -- the batch cadence itself
// rate-limits the check, so no sampling timer is needed. It converges on
// REMAINING memory: while free is below the model's checkpoint floor (the
// in-flight writes' flush spike + the system-memory reserve), drain the
// largest bucket's tail into the spill manager, repeating within the same batch
// until free is back above the floor or the buckets are empty. Free memory
// sees what the static resident budget misses (concurrent tasks, V2 import,
// compaction, GC churn, cgo buffers); it can only push usage below what the
// budget accounts, never above it. See importutilv2/reshardmem.
func (r *reshardPlanExecutor) spillByMemory() error {
	if r.resident.Load() <= 0 {
		return nil
	}
	total := int64(hardware.GetMemoryCount())
	floor := r.memModel.CheckpointFloor(r.memoryBudget, total)
	for r.resident.Load() > 0 {
		if total-int64(hardware.GetUsedMemoryCount()) >= floor {
			break
		}
		spilled, err := r.spillLargest()
		if err != nil {
			return err
		}
		if spilled == 0 {
			// The buckets are empty: whatever resident bytes remain belong to
			// in-flight fragment inputs, which the writes release on their own.
			break
		}
		r.resident.Add(-spilled)
	}
	return nil
}

// spill converges the bucket-resident set back under the task's whole resident
// budget by spilling the largest buckets, one at a time. It bounds only the
// bytes the buckets hold: a detached fragment write's input is bounded by the
// flush pool concurrency and released by the write, so it is not part of the
// ceiling and needs no spill.
func (r *reshardPlanExecutor) spill() error {
	for r.bucketResident.Load() > r.residentBudget {
		spilled, err := r.spillLargest()
		if err != nil {
			return err
		}
		if spilled == 0 {
			// Every bucket is empty: the ceiling is satisfied by construction.
			break
		}
		r.resident.Add(-spilled)
	}
	return nil
}

// flushAll flushes every non-empty bucket in bucket order, the same
// tail the serial pipeline produced.
func (r *reshardPlanExecutor) flushAll() error {
	keys := make([]reshardBucketKey, 0, len(r.buckets))
	for key := range r.buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].channelOrdinal != keys[j].channelOrdinal {
			return keys[i].channelOrdinal < keys[j].channelOrdinal
		}
		return keys[i].partitionOrdinal < keys[j].partitionOrdinal
	})
	for _, key := range keys {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		b := r.buckets[key]
		if b.rows == 0 && len(b.ranges) == 0 {
			continue
		}
		if err := r.flushBucket(b); err != nil {
			return err
		}
	}
	return nil
}

// collectOutcomes folds the seq-ordered fragment outcomes into the manifest
// and the run counters. Collected in sequence order, which is the routing
// order the synchronous pipeline appended to the manifest in.
func (r *reshardPlanExecutor) collectOutcomes() {
	for _, outcome := range r.flushPool.outcomes() {
		r.manifest.Fragments = append(r.manifest.Fragments, outcome.descriptor)
		r.counters.fragments++
		r.counters.rows += outcome.descriptor.GetRows()
		r.counters.logicalBytes += outcome.descriptor.GetLogicalBytes()
		r.counters.writtenBytes += int64(outcome.writtenUncompressed)
		if outcome.timings != nil {
			r.counters.sortReadCost += outcome.timings.ReadCost
			r.counters.sortCost += outcome.timings.SortCost
			r.counters.sortWriteCost += outcome.timings.WriteCost
		}
	}
}

func (r *reshardPlanExecutor) logSummary() {
	mlog.Info(r.ctx, "import v3 reshard run finished",
		mlog.FieldJobID(r.req.GetJobId()),
		mlog.Int64("taskID", r.req.GetTaskId()),
		mlog.Int64("runID", r.req.GetRunId()),
		mlog.Int64("slot", r.slot),
		mlog.Int64("memoryBudget", r.memoryBudget),
		mlog.Int64("fragmentTarget", r.fragmentTarget),
		mlog.Int64("effectiveFragmentInput", r.effectiveFragmentInput),
		mlog.Int64("residentBudget", r.residentBudget),
		mlog.Int64("flushConcurrency", r.memModel.Flushes()),
		mlog.Int("buckets", len(r.buckets)),
		mlog.Int("sources", len(r.plan.GetSources())),
		mlog.Int("fragments", r.counters.fragments),
		mlog.Int64("rows", r.counters.rows),
		mlog.Int64("logicalBytes", r.counters.logicalBytes),
		mlog.Int64("writtenUncompressedBytes", r.counters.writtenBytes),
		mlog.Int("spillRanges", r.counters.spillRanges),
		mlog.Int("spillStreams", r.spillManager.Streams()),
		mlog.Int("spillFiles", r.spillManager.Files()),
		mlog.Int64("spillBytes", r.counters.spillBytes),
		mlog.Int64("peakResidentBytes", r.counters.peakResident),
		mlog.Duration("runWall", r.counters.runWall),
		// Routing side: readWait is time starved by the prepare stage,
		// routeCost is hashing + flush/spill triggers, flushBlock is waiting
		// for a write slot.
		mlog.Duration("readWait", r.counters.readWait),
		mlog.Duration("routeCost", r.counters.routeCost),
		mlog.Duration("flushWall", r.flushPool.flushWallDuration()),
		mlog.Duration("flushBlock", r.counters.flushBlock),
		// Prepare stage split: stageRead is the source reader (IO + decode),
		// stageNormalize and stageFunctions are the prepare steps, stageSend
		// is the stage blocked handing off to the routing side.
		mlog.Duration("stageRead", time.Duration(r.stageTimings.read.Load())),
		mlog.Duration("stageNormalize", time.Duration(r.stageTimings.normalize.Load())),
		mlog.Duration("stageFunctions", time.Duration(r.stageTimings.functions.Load())),
		mlog.Duration("stageSend", time.Duration(r.stageTimings.send.Load())),
		mlog.Duration("sortReadCost", r.counters.sortReadCost),
		mlog.Duration("sortCost", r.counters.sortCost),
		mlog.Duration("sortWriteCost", r.counters.sortWriteCost),
	)
}

// reshardFlushPool owns the detached fragment writes of one reshard run: the
// concurrency limit, the write futures' lifecycle, first-error propagation
// (whose cancel stops the read side), and the seq-ordered outcomes. Each sort
// group is submitted to a bounded pool so the sort, encode and upload of one
// fragment overlap reading and hashing the next batches instead of stalling
// them. The pool is capped at the same N DataCoord charged the slot for
// (reshardmem.WorkingSet), so the live set stays inside the model; a full pool
// blocks the routing side exactly like the old synchronous flush did.
type reshardFlushPool struct {
	ctx     context.Context
	cancel  context.CancelFunc
	pool    *conc.Pool[any]
	futures []*conc.Future[any]
	// mu guards err, flushed and flushWall: the routing side reads Err while
	// write goroutines store their results.
	mu        sync.Mutex
	err       error
	flushed   []*reshardFragmentOutcome // indexed by sequence number
	flushWall time.Duration

	cfg          *reshardRunConfig
	spillManager *SpillManager
	resident     *atomic.Int64
}

func newReshardFlushPool(ctx context.Context, cfg *reshardRunConfig, spillManager *SpillManager, resident *atomic.Int64) *reshardFlushPool {
	flushCtx, cancel := context.WithCancel(ctx)
	return &reshardFlushPool{
		ctx:          flushCtx,
		cancel:       cancel,
		pool:         conc.NewPool[any](int(cfg.memModel.Flushes())),
		cfg:          cfg,
		spillManager: spillManager,
		resident:     resident,
	}
}

// dispatch hands one sort group to the write pool. The group takes over the
// accounted bytes of the fragment input it carries and releases them when the
// write is done, so resident covers that memory for exactly as long as it is
// live. Sequence numbers are assigned by the caller in routing order. The
// returned duration is how long the routing side blocked waiting for a free
// write slot (Submit blocks when the pool is full, the semaphore semantics).
func (p *reshardFlushPool) dispatch(vchannelOrdinal, partitionOrdinal int, group reshardFragmentGroup, seq int64) (time.Duration, error) {
	if err := p.Err(); err != nil {
		return 0, err
	}
	blockStart := time.Now()
	future := p.pool.Submit(func() (any, error) {
		writeStart := time.Now()
		outcome, err := p.write(vchannelOrdinal, partitionOrdinal, group, seq)
		wall := time.Since(writeStart)
		// This write was the last reader of the group's spill ranges, so it
		// owns releasing them, which is what retires a drained shard file.
		p.spillManager.Release(group.ranges)
		p.resident.Add(-group.accounted)
		p.mu.Lock()
		defer p.mu.Unlock()
		p.flushWall += wall
		if err != nil {
			if p.err == nil {
				p.err = err
				// Stop routing and the read side: the task is failed, so the
				// rest of the source is wasted work. A write already in
				// flight still runs to completion -- the writer takes no
				// ctx -- and the run joins it before returning.
				p.cancel()
			}
			return nil, nil
		}
		// Outcomes are indexed by sequence number, so the published manifest
		// keeps the exact fragment order the synchronous pipeline produced no
		// matter which write finishes first.
		for int64(len(p.flushed)) <= seq {
			p.flushed = append(p.flushed, nil)
		}
		p.flushed[seq] = outcome
		return nil, nil
	})
	blocked := time.Since(blockStart)
	// The write's own error travels through the pool's first-error record and
	// surfaces in wait()/Err(); a write failure cancels the read side, which
	// the read loop's per-batch Err checks report long before publish.
	p.futures = append(p.futures, future)
	return blocked, nil
}

// Err returns the first detached write error, if any.
func (p *reshardFlushPool) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// wait joins every detached write and returns the first write error, if any.
func (p *reshardFlushPool) wait() error {
	if err := conc.AwaitAll(p.futures...); err != nil {
		return err
	}
	return p.Err()
}

// shutdown is the deferred cleanup: cancel the read/write side, then join
// every write before the spill manager is closed and its directory wiped.
func (p *reshardFlushPool) shutdown() {
	p.cancel()
	_ = p.wait()
}

// outcomes returns the seq-ordered fragment outcomes. It must only be called
// after wait, when no write goroutine can still be running.
func (p *reshardFlushPool) outcomes() []*reshardFragmentOutcome {
	return p.flushed
}

func (p *reshardFlushPool) flushWallDuration() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushWall
}

// write sorts one group into a single fragment parquet and returns its
// manifest descriptor plus the run observations.
func (p *reshardFlushPool) write(vchannelOrdinal, partitionOrdinal int, group reshardFragmentGroup, seq int64) (*reshardFragmentOutcome, error) {
	req, plan := p.cfg.req, p.cfg.plan
	fragmentPath := path.Join(req.GetStorageConfig().GetRootPath(), metautil.BuildImportReshardOutputPath(req.GetJobId(), req.GetTaskId()), "fragments", strconv.Itoa(vchannelOrdinal), strconv.FormatInt(plan.GetPartitions()[partitionOrdinal], 10), fmt.Sprintf("%d_%d.parquet", req.GetRunId(), seq))
	writer, err := newImportV3PackedRecordWriter(req.GetStorageConfig().GetBucketName(), []string{fragmentPath}, p.cfg.temporarySchema, p.cfg.bufferSize, req.GetStorageConfig(), p.cfg.pluginContext)
	if err != nil {
		return nil, err
	}

	readers := make([]storage.RecordReader, 0, len(group.ranges)+len(group.batches))
	for _, spillRange := range group.ranges {
		reader, err := p.spillManager.RangeReader(spillRange)
		if err != nil {
			_ = writer.Close()
			// Readers opened so far borrow the ipc.Reader arrow buffers; the
			// deferred close is registered after the loops, so the failure
			// branch must release them explicitly. The shared spill fds are
			// owned by the log, not by these readers.
			closeReshardReaders(readers)
			return nil, err
		}
		readers = append(readers, reader)
	}
	for _, batch := range group.batches {
		reader, err := storage.NewInsertDataRecordReader(batch, p.cfg.temporarySchema)
		if err != nil {
			_ = writer.Close()
			closeReshardReaders(readers)
			return nil, err
		}
		readers = append(readers, reader)
	}
	defer closeReshardReaders(readers)

	rows, timings, err := storage.Sort(uint64(p.cfg.bufferSize), p.cfg.temporarySchema, readers, writer, func(storage.Record, int, int) bool { return true }, p.cfg.sortFields)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if int64(rows) != group.rows || writer.GetWrittenRowNum() != int64(rows) {
		return nil, merr.WrapErrDataIntegrityMsg("fragment row count mismatch: input=%d sorted=%d written=%d", group.rows, rows, writer.GetWrittenRowNum())
	}
	// LogicalBytes publishes the fragment's normalized decoded bytes, the same
	// metric that cut this fragment out of its bucket (see appendReshardBatch
	// and splitReshardBucketForSort). Planning packs dataCoord.segment.maxSize
	// against it; the packed writer's uncompressed output runs below it by a
	// schema-dependent gap.
	return &reshardFragmentOutcome{
		descriptor: &datapb.FragmentDescriptor{
			ChannelIndex: int32(vchannelOrdinal), PartitionId: plan.GetPartitions()[partitionOrdinal], Seq: seq,
			Path: writer.GetWrittenPaths(0), Rows: int64(rows), LogicalBytes: group.logicalBytes,
		},
		timings:             timings,
		writtenUncompressed: writer.GetWrittenUncompressed(),
	}, nil
}

// reshardStageTimings breaks the prepare stage's per-batch work into the
// phases that decide whether the stage is the bottleneck and, if so, which
// phase: stageRead is the source reader (object-store IO plus decode),
// stageNormalize and stageFunctions are the two prepare steps, and stageSend
// is the time the stage blocked handing a batch to the routing side
// (downstream backpressure). The routing side separately records readWait
// (starved by this stage), routeCost and flushBlock. The stage is the limiter
// when readWait > 0 with stageSend ~= 0; it is reader-bound when stageRead
// dominates that stage cycle.
type reshardStageTimings struct {
	read      atomic.Int64
	normalize atomic.Int64
	functions atomic.Int64
	send      atomic.Int64
}

// reshardPrepareStage is one source's read/normalize/function pipeline. A
// goroutine replays the reader strictly sequentially -- row counting,
// normalization (including the sequential preallocated-ID offset) and function
// execution all keep read order without any sharing with the routing side --
// streaming prepared batches into a buffered channel so source-side work
// overlaps fragment sort/write.
type reshardPrepareStage struct {
	source       *datapb.SourceFileSpec
	schema       *schemapb.CollectionSchema
	runFunctions bool
	// runCtx drives function execution: it is the run ctx, not the source ctx,
	// exactly as before the stage existed. Only the source-side cancel (flush
	// failure, source switch) stops the stage.
	runCtx       context.Context
	idOffset     int64
	results      <-chan reshardPrepareResult
	stop         func()
	cancelSource context.CancelFunc
	timings      *reshardStageTimings
}

// newReshardPrepareStage wires one source's prepare stage. It deliberately
// takes two contexts: sourceCtx bounds the source reader and is canceled on a
// source switch or flush failure, while runCtx drives function execution and is
// the run context, not the source context. revive's context-as-argument rule
// cannot express that split.
//
//nolint:revive // context-as-argument: two contexts by design (source cancel vs run functions)
func newReshardPrepareStage(sourceCtx context.Context, cancelSource context.CancelFunc, runCtx context.Context, reader importutilv2.Reader, source *datapb.SourceFileSpec, schema *schemapb.CollectionSchema, runFunctions bool, timings *reshardStageTimings) *reshardPrepareStage {
	s := &reshardPrepareStage{
		source:       source,
		schema:       schema,
		runFunctions: runFunctions,
		runCtx:       runCtx,
		cancelSource: cancelSource,
		timings:      timings,
	}
	s.results, s.stop = startReshardPrepare(sourceCtx, reader, s.prepare, timings)
	return s
}

// prepare runs inside the stage goroutine: count, normalize, then execute the
// schema functions. A zero-row batch returns nil and is never sent.
func (s *reshardPrepareStage) prepare(batch *storage.InsertData) (*storage.InsertData, error) {
	rowNum, _ := importv2.GetInsertDataRowCount(batch, s.schema)
	normalizeStart := time.Now()
	err := normalizeReshardBatch(s.source, s.schema, batch, rowNum, &s.idOffset)
	s.timings.normalize.Add(int64(time.Since(normalizeStart)))
	if err != nil {
		return nil, err
	}
	if rowNum == 0 {
		return nil, nil
	}
	if s.runFunctions {
		functionsStart := time.Now()
		err := runReshardFunctions(s.runCtx, s.schema, batch)
		s.timings.functions.Add(int64(time.Since(functionsStart)))
		if err != nil {
			return nil, err
		}
	}
	return batch, nil
}

// next returns the next prepared batch. ok=false means the source is drained
// (clean EOF or cancellation, which the run-level ctx checks catch).
func (s *reshardPrepareStage) next() (*storage.InsertData, bool, error) {
	result, ok := <-s.results
	if !ok {
		return nil, false, nil
	}
	return result.batch, true, result.err
}

// close cancels the source ctx first (aborting an in-flight Read), then joins
// the stage goroutine. It must run before reader.Close so the goroutine never
// observes a closed reader.
func (s *reshardPrepareStage) close() {
	s.cancelSource()
	s.stop()
}

// reshardPrepareDepth bounds how far the source prepare stage runs ahead of
// the routing side: one batch buffered in the channel plus one in-flight
// batch being read, normalized, or function-processed inside the stage.
// The run-ahead exists so source reading, normalization, and function
// execution overlap fragment sort/write; deeper buffering only costs memory
// without helping steady state, which is bounded by the slower stage either
// way.
const reshardPrepareDepth = 1

// reshardPrepareResult is one prepared source batch (or the terminal prepare
// error) traveling from the prepare stage to the routing side. The batch is
// normalized, function-processed, and non-empty: empty batches are skipped
// inside the stage and never sent.
type reshardPrepareResult struct {
	batch *storage.InsertData
	err   error
}

// startReshardPrepare launches one goroutine replaying reader and preparing
// every batch with prepare (row count, normalization, function execution),
// streaming the prepared batches into a buffered channel so source-side work
// overlaps fragment sort/write. A nil batch returned by prepare is skipped
// without sending. The channel closes at io.EOF, or after an error result is
// delivered. The returned stop func cancels the send side and joins the
// goroutine, so a closed reader is never touched again; it must run before
// reader.Close. timings records the read and hand-off phases; prepare records
// its own normalization and function phases.
func startReshardPrepare(ctx context.Context, reader importutilv2.Reader, prepare func(*storage.InsertData) (*storage.InsertData, error), timings *reshardStageTimings) (<-chan reshardPrepareResult, func()) {
	results := make(chan reshardPrepareResult, reshardPrepareDepth)
	ctx, cancel := context.WithCancel(ctx)
	// The prepare stage runs one batch ahead on its own goroutine. The future
	// joins it: the stop func cancels the shared ctx and waits for the
	// goroutine to close results.
	future := conc.Go(func() (struct{}, error) {
		defer close(results)
		for {
			if err := ctx.Err(); err != nil {
				return struct{}{}, nil
			}
			readStart := time.Now()
			batch, err := reader.Read()
			timings.read.Add(int64(time.Since(readStart)))
			if err == io.EOF {
				return struct{}{}, nil
			}
			if err == nil {
				batch, err = prepare(batch)
			}
			if err != nil {
				select {
				case results <- reshardPrepareResult{err: err}:
				case <-ctx.Done():
				}
				return struct{}{}, nil
			}
			if batch == nil {
				continue
			}
			sendStart := time.Now()
			select {
			case results <- reshardPrepareResult{batch: batch}:
				timings.send.Add(int64(time.Since(sendStart)))
			case <-ctx.Done():
				timings.send.Add(int64(time.Since(sendStart)))
				return struct{}{}, nil
			}
		}
	})
	return results, func() {
		cancel()
		_, _ = future.Await()
	}
}

type reshardBucketKey struct {
	channelOrdinal   int
	partitionOrdinal int
}

type reshardBucket struct {
	vchannelOrdinal  int
	partitionOrdinal int
	batches          []reshardBatch
	ranges           []SpillRange
	bytes            int64 // accounted resident bytes (decoded + structural overhead)
	logicalBytes     int64 // decoded bytes; the flush trigger and descriptor metric
	rows             int64
}

// reshardBatch is one routed batch held in a bucket together with two sizes
// measured once at routing time: bytes is the memory-accounted size (decoded
// plus the calibrated per-fragment structural overhead, see
// reshardmem.FragmentOverhead) that drives resident accounting and the spill
// tail cap, and logical is the decoded size (InsertData.GetMemorySize, which
// walks every string/JSON value) that drives the fragment flush trigger, the
// sort-group packing and the published descriptor. Measuring both per batch
// once here replaces the repeated measurements that routing, resident
// accounting, and the sort-input split used to perform on the same batch.
type reshardBatch struct {
	data    *storage.InsertData
	bytes   int64
	logical int64
}

// reshardRunCounters aggregates per-run observations for the reshard summary
// log. It is log-only: no counter feeds back into planning, packing, or quota,
// so miscounting here can mislead an operator but never corrupts data.
type reshardRunCounters struct {
	fragments    int
	rows         int64
	logicalBytes int64 // sum of published descriptor.LogicalBytes (normalized decoded bytes)
	writtenBytes int64 // sum of the packed writer's uncompressed output, reported
	// alongside logicalBytes so the run log keeps both sides of the packing metric
	spillRanges   int
	spillBytes    int64
	sortReadCost  time.Duration
	sortCost      time.Duration
	sortWriteCost time.Duration
	// readWait is the time the routing side blocked waiting for the next
	// prepared source batch (read + normalize + function execution); flushBlock
	// is the time the routing side spent waiting for a free write slot: zero
	// means every write hid behind reading and hashing, anything above zero
	// means the run is write-bound and dataCoord.import.reshardFlushConcurrency
	// is the limiter. (The summed wall time of the detached writes themselves
	// is owned by the flush pool, not by these counters.)
	readWait   time.Duration
	flushBlock time.Duration
	// routeCost is the time the routing side spent hashing a batch into
	// buckets and applying the flush/spill triggers; runWall is the whole run's
	// wall time, the denominator for the phase breakdown. readWait + routeCost
	// + flushBlock make up the routing side; the prepare stage's split
	// (read/normalize/functions/send) is in stageTimings.
	routeCost    time.Duration
	runWall      time.Duration
	peakResident int64
}

func appendReshardBatch(b *reshardBucket, data *storage.InsertData, mem, logical int64) {
	b.batches = append(b.batches, reshardBatch{data: data, bytes: mem, logical: logical})
	b.bytes += mem
	b.logicalBytes += logical
	b.rows += int64(data.GetRowNum())
}

// reshardFragmentGroup is the bounded input of one storage.Sort call. Splitting
// a bucket into groups is what turns "the bucket is on disk" into a real memory
// bound for the sort: storage.Sort materializes its whole input. Group sizes
// are the items' decoded logical bytes -- the metric Sort actually
// materializes once the fragments become compact arrow records.
type reshardFragmentGroup struct {
	ranges       []SpillRange
	batches      []*storage.InsertData
	rows         int64
	logicalBytes int64
	// accounted is the memory-accounted size of the group's in-memory batches
	// (decoded bytes plus the per-fragment structural overhead). Spilled ranges
	// contribute nothing: their bytes were released when they went to disk. A
	// detached write releases exactly this much once its fragment is written.
	accounted int64
}

// splitReshardBucketForSort packs spill ranges and the in-memory tail into
// contiguous groups that each fit inside one Sort's input budget. A single item
// larger than the budget stays alone; the existing slot estimate makes that a
// single-record pathological case.
func splitReshardBucketForSort(b *reshardBucket, limit int64) []reshardFragmentGroup {
	type item struct {
		rangeRef  *SpillRange
		batch     *storage.InsertData
		bytes     int64
		accounted int64
		rows      int64
	}
	items := make([]item, 0, len(b.ranges)+len(b.batches))
	for i := range b.ranges {
		items = append(items, item{rangeRef: &b.ranges[i], bytes: b.ranges[i].Logical, rows: b.ranges[i].Rows})
	}
	for _, batch := range b.batches {
		items = append(items, item{batch: batch.data, bytes: batch.logical, accounted: batch.bytes, rows: int64(batch.data.GetRowNum())})
	}
	groups := make([]reshardFragmentGroup, 0, 1)
	if limit <= 0 {
		for _, it := range items {
			groups = append(groups, reshardFragmentGroup{rows: it.rows, logicalBytes: it.bytes, accounted: it.accounted})
			g := &groups[len(groups)-1]
			if it.rangeRef != nil {
				g.ranges = append(g.ranges, *it.rangeRef)
			} else {
				g.batches = append(g.batches, it.batch)
			}
		}
		return groups
	}
	appendItem := func(g *reshardFragmentGroup, it item) {
		if it.rangeRef != nil {
			g.ranges = append(g.ranges, *it.rangeRef)
		} else {
			g.batches = append(g.batches, it.batch)
		}
		g.rows += it.rows
		g.logicalBytes += it.bytes
		g.accounted += it.accounted
	}
	for _, it := range items {
		if len(groups) == 0 || (groups[len(groups)-1].logicalBytes > 0 && groups[len(groups)-1].logicalBytes+it.bytes > limit) {
			groups = append(groups, reshardFragmentGroup{})
		}
		appendItem(&groups[len(groups)-1], it)
	}
	return groups
}

// reshardFragmentOutcome carries what one fragment write produced: the
// manifest descriptor plus the observations the reshard summary log needs.
// The writer's uncompressed byte count travels alongside the descriptor so
// the run summary can report the packing metric next to the Sort timings.
type reshardFragmentOutcome struct {
	descriptor          *datapb.FragmentDescriptor
	timings             *storage.SortTimings
	writtenUncompressed uint64
}

func closeReshardReaders(readers []storage.RecordReader) {
	for _, reader := range readers {
		if reader != nil {
			_ = reader.Close()
		}
	}
}

func publishReshardManifest(ctx context.Context, cm storage.ChunkManager, req *datapb.ReshardTaskRequest, manifest *datapb.ReshardManifest) error {
	payload, err := proto.Marshal(manifest)
	if err != nil {
		return merr.WrapErrSerializationFailed(err, "marshal ReshardManifest")
	}
	resultPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(req.GetJobId(), req.GetTaskId(), req.GetRunId()))
	if err := cm.Write(ctx, resultPath, payload); err != nil {
		return merr.Wrap(err, "write ReshardManifest")
	}
	return nil
}

func newReshardSourceReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, source *datapb.SourceFileSpec, bufferSize int64, storageConfig *indexpb.StorageConfig, pluginContext *indexcgopb.StoragePluginContext) (importutilv2.Reader, error) {
	if source == nil || source.GetFile() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
	}
	options := make(importutilv2.Options, 0, 6)
	appendOption := func(key, value string) {
		options = append(options, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	readerOptions := source.GetOptions()
	switch source.GetFileType() {
	case datapb.ImportFileType_Csv:
		if readerOptions.GetSeparator() != "" {
			appendOption(importutilv2.CSVSep, readerOptions.GetSeparator())
		}
		if readerOptions.GetNullKey() != "" {
			appendOption(importutilv2.CSVNullKey, readerOptions.GetNullKey())
		}
		return importutilv2.NewReader(ctx, cm, schema, source.GetFile(), options, int(bufferSize), storageConfig)
	case datapb.ImportFileType_BackupBinlog:
		return binlog.NewReader(ctx, cm, schema, storageConfig, readerOptions.GetStorageVersion(), source.GetFile().GetPaths(), readerOptions.GetStartTs(), readerOptions.GetEndTs(), int(bufferSize), "", pluginContext)
	default:
		return importutilv2.NewReader(ctx, cm, schema, source.GetFile(), options, int(bufferSize), storageConfig)
	}
}

// runReshardFunctions executes every schema function (TextEmbedding, BM25,
// MinHash) once per batch, at the same pipeline position as Import V2:
// after normalization and before hash routing. User-provided outputs that the
// property allows are preserved (TextEmbedding skips the provider) or
// deterministically recomputed (BM25/MinHash overwrite with identical values),
// matching V2 semantics exactly.
func runReshardFunctions(ctx context.Context, schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(schema.GetFunctions()) == 0 {
		return nil
	}
	return embedding.RunAll(ctx, schema, data, embedding.RunOptions{
		ClusterID:           paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		DBName:              schema.GetDbName(),
		AllowNonBM25Outputs: common.GetCollectionAllowInsertNonBM25FunctionOutputs(schema.GetProperties()),
	})
}

func normalizeReshardBatch(source *datapb.SourceFileSpec, schema *schemapb.CollectionSchema, data *storage.InsertData, rowNum int, idOffset *int64) error {
	if data == nil {
		return merr.WrapErrDataIntegrityMsg("source reader returned nil data")
	}
	if err := importv2.CheckRowsEqual(schema, data); err != nil {
		return err
	}
	if err := importv2.CheckStructArrayConsistency(schema, data); err != nil {
		return err
	}
	if err := importv2.AppendNullableDefaultFieldsData(schema, data, rowNum); err != nil {
		return err
	}
	if err := importv2.FillDynamicData(schema, data, rowNum); err != nil {
		return err
	}
	if source.GetFileType() == datapb.ImportFileType_BackupBinlog {
		return nil
	}
	return importv2.AppendPreallocatedSystemFields(schema, data, rowNum, source.GetFile().GetIdRange(), idOffset)
}
