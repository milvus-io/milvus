// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the execution of one ReshardTask run. The executor is pure
// orchestration: per source batch it routes into the bucket table, reports
// progress, and spills under memory pressure, then cuts every remaining bucket
// and lets the writer publish. The work it drives is split across three objects,
// each hiding its own complexity:
//
//   - sourceStream (reshard_source.go): every file as one stream of prepared
//     batches -- the prepare goroutine, the file switch, and the read-side stop
//     checks in one place.
//   - bucketTable (reshard_bucket.go): add / cutIfCrossing / cutAll / spillWhile,
//     owning the accounted bytes of the buckets.
//   - fragmentWriter (reshard_fragment_writer.go): the detached writes -- seq,
//     first error, in-flight bytes -- around a plain writeFragment.
//
// Everything they share is immutable and derived once by deriveReshardPolicies
// (reshard_policy.go): a fragmentPolicy for cut, a memoryPolicy for spill, and
// the write/source specs. The run's live set is computed, never a shared counter:
// resident = bucketTable.memBytes + fragmentWriter.inflight.

import (
	"context"
	"os"
	"path"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// importV3SpillRootDir is the node-local root of Import V3 reshard spill files.
// It must live outside the import_v3 prefix: with common.storageType=local the
// chunk-manager root and the local storage path are the same directory, and
// reshard's durable output (fragments and manifests) is written under
// <root>/import_v3/<job>/... — a startup cleanup of that prefix would delete
// finished work.
const importV3SpillRootDir = "import_v3_spill"

// CleanImportV3Prefixes removes the local Import V3 spill root left by previous
// process runs. DataNode V3 task state is memory-only and is never recovered
// from local spill files, so after restart every file under the spill root is
// garbage by definition. Only the spill root is touched: the import_v3 prefix is
// shared with durable reshard output under local storage (see
// importV3SpillRootDir).
func CleanImportV3Prefixes() {
	root := path.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir)
	if err := os.RemoveAll(root); err != nil {
		mlog.Warn(context.TODO(), "failed to clean import v3 local root", mlog.String("root", root), mlog.Err(err))
		return
	}
	mlog.Info(context.TODO(), "cleaned import v3 local root", mlog.String("root", root))
}

// executeReshardPlan is the entry point of one ReshardTask run: it validates the
// plan, derives the run configuration and drives the reshardPlanExecutor. The
// pipeline itself lives below.
func executeReshardPlan(
	ctx context.Context,
	cm storage.ChunkManager,
	req *datapb.ReshardTaskRequest,
	plan *importv3pb.ReshardTaskPlan,
	pluginContext *indexcgopb.StoragePluginContext,
	metrics *Metrics,
	progress *ReshardProgress,
) error {
	executor, err := newReshardPlanExecutor(cm, req, plan, pluginContext, metrics, progress)
	if err != nil {
		return err
	}
	return executor.execute(ctx)
}

// reshardPlanExecutor executes one ReshardTask run end to end.
type reshardPlanExecutor struct {
	req  *datapb.ReshardTaskRequest
	plan *importv3pb.ReshardTaskPlan
	cm   storage.ChunkManager

	// Immutable per-run configuration, derived once from the request, the plan
	// and the live parameters.
	slot     int64
	fragment fragmentPolicy
	memory   memoryPolicy
	source   sourceSpec
	write    writeSpec

	ctx      context.Context // run ctx, set by execute(); bounds every stage
	spillMgr *SpillManager
	writer   *fragmentWriter
	buckets  *bucketTable
	manifest *importv3pb.ReshardManifest
	counters reshardRunCounters
	// stageTimings is the prepare-stage phase breakdown, written by the stage
	// goroutine and read by the summary once every stage is joined.
	stageTimings reshardStageTimings
	fragmentSeq  int64
	metrics      *Metrics
	progress     *ReshardProgress
}

// newReshardPlanExecutor derives the run configuration. It performs no IO: spill
// setup and the write pool are created by execute.
func newReshardPlanExecutor(
	cm storage.ChunkManager,
	req *datapb.ReshardTaskRequest,
	plan *importv3pb.ReshardTaskPlan,
	pluginContext *indexcgopb.StoragePluginContext,
	metrics *Metrics,
	progress *ReshardProgress,
) (*reshardPlanExecutor, error) {
	policy, err := getReshardPolicy(req, plan)
	if err != nil {
		return nil, err
	}
	// The chunk manager and the storage plugin context are run handles the
	// caller supplies, not values derived from the request and the plan.
	policy.source.cm = cm
	policy.source.pluginContext = pluginContext
	policy.write.pluginContext = pluginContext
	return &reshardPlanExecutor{
		req:      req,
		plan:     plan,
		cm:       cm,
		slot:     policy.slot,
		fragment: policy.fragment,
		memory:   policy.memory,
		source:   policy.source,
		write:    policy.write,
		manifest: &importv3pb.ReshardManifest{},
		metrics:  metrics,
		progress: progress,
	}, nil
}

// execute runs the reshard pipeline: read every source through the prepare
// stage, route batches into buckets, cut full buckets as detached writes, then
// publish the manifest once every write has landed.
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
	spillMgr, err := NewSpillManager(r.write.spillRoot, r.write.fragmentSchema, r.write.spillStreams)
	if err != nil {
		return err
	}
	r.spillMgr = spillMgr
	defer func() { _ = r.spillMgr.Close() }()
	r.writer = newFragmentWriter(ctx, &r.write, r.spillMgr, r.memory.model.Flushes())
	// Registered after the spillManager.Close defer, so the run cancels, joins
	// every write, and only then closes the spill manager (which removes its
	// directory).
	defer r.writer.close()
	r.buckets = newBucketTable(&r.fragment, int64(len(r.plan.GetPartitionIds())))
	source := newReshardSourceStream(r.ctx, &r.source, r.plan.GetFiles(), r.writer, &r.stageTimings)
	// Registered last, so a stopped run tears the source down before the writes
	// it may have starved and before the spill manager its ranges live in.
	defer source.close()

	for {
		batch, file, ok, err := source.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		routeStart := time.Now()
		err = r.route(batch)
		r.counters.routeCost += time.Since(routeStart)
		if err != nil {
			return err
		}
		// The batch is hashed into buckets now; count its rows as imported for
		// this source file before the spill triggers run.
		r.progress.AddHashed(file.GetId(), int64(batch.GetRowNum()))
		if err := r.spillByMemory(); err != nil {
			return err
		}
	}
	r.counters.readWait = source.readWait
	if err := r.cutAll(); err != nil {
		return err
	}
	// Every fragment write is detached, so the run joins them before it
	// publishes: the manifest is complete only once the last fragment is on
	// object storage, and the spill manager has to outlive its last reader.
	if err := r.writer.wait(); err != nil {
		return err
	}
	r.collectOutcomes()
	r.counters.runWall = time.Since(runStart)
	r.observeRunMetrics()
	r.logSummary()
	return publishReshardManifest(ctx, r.cm, r.req, r.manifest)
}

// resident is the run's live accounted set: the bytes the buckets still hold plus
// the bytes their detached writes have not released. It is computed, not a shared
// counter, so a spill can only reclaim memory the buckets actually hold.
func (r *reshardPlanExecutor) resident() int64 {
	return r.buckets.memBytes() + r.writer.inflightBytes()
}

// route hashes one prepared batch into buckets and applies the per-destination
// cut and the run's whole-task spill ceiling.
func (r *reshardPlanExecutor) route(batch *storage.InsertData) error {
	hashed, err := importv2.HashDataBySchema(r.write.fragmentSchema, r.plan.GetVchannels(), r.plan.GetPartitionIds(), batch)
	if err != nil {
		return err
	}
	for vchannelOrdinal := range hashed {
		for partitionOrdinal, bucketData := range hashed[vchannelOrdinal] {
			if bucketData.GetRowNum() == 0 {
				continue
			}
			key := bucketKey{vchannelOrdinal: vchannelOrdinal, partitionOrdinal: partitionOrdinal}
			// Measure the routed chunk once; the piece carries both sizes to
			// resident accounting and the sort-input split so the O(rows)
			// string/JSON walk is not repeated per batch.
			logical := int64(bucketData.GetMemorySize())
			mem := logical + r.fragment.overhead
			if inputs := r.buckets.add(key, bucketData, mem, logical); len(inputs) > 0 {
				if err := r.dispatch(vchannelOrdinal, partitionOrdinal, inputs); err != nil {
					return err
				}
			}
			r.counters.peakResident = max(r.counters.peakResident, r.resident())
			// Whole-task bucket-resident ceiling: on overflow spill the largest
			// buckets back under the budget. Only the bytes still held by the
			// buckets count here -- a detached fragment write's input is bounded
			// by the writer's concurrency the model already charges and is
			// released by the write, so charging it would spill live bucket tails
			// to reclaim memory the write path never held.
			if err := r.spill(); err != nil {
				return err
			}
		}
	}
	return nil
}

// dispatch submits a cut's fragment inputs to the detached writer in order,
// assigning sequence numbers as it goes and accumulating the time the routing
// side blocked for a free write slot.
func (r *reshardPlanExecutor) dispatch(vchannelOrdinal, partitionOrdinal int, inputs []fragmentInput) error {
	for _, input := range inputs {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		blocked, err := r.writer.dispatch(vchannelOrdinal, partitionOrdinal, input, r.fragmentSeq)
		r.counters.flushBlock += blocked
		if err != nil {
			return err
		}
		r.fragmentSeq++
	}
	return nil
}

// cutAll cuts every non-empty bucket in bucket order and submits each cut to the
// writer, the same tail the serial pipeline produced.
func (r *reshardPlanExecutor) cutAll() error {
	for _, keyed := range r.buckets.cutAll() {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if err := r.dispatch(keyed.key.vchannelOrdinal, keyed.key.partitionOrdinal, keyed.inputs); err != nil {
			return err
		}
	}
	return nil
}

// spill converges the bucket-resident set back under the task's whole resident
// budget by spilling the largest buckets, one at a time. It bounds only the bytes
// the buckets hold: a detached fragment write's input is bounded by the writer's
// concurrency and released by the write, so it is not part of the ceiling and
// needs no spill.
func (r *reshardPlanExecutor) spill() error {
	if r.buckets.memBytes() <= r.memory.residentBudget {
		return nil
	}
	spilled, ranges, err := r.buckets.spillWhile(r.spillMgr, func() bool {
		return r.buckets.memBytes() > r.memory.residentBudget
	})
	r.counters.spillBytes += spilled
	r.counters.spillRanges += ranges
	return err
}

// spillByMemory runs once per source batch -- the batch cadence itself
// rate-limits the check, so no sampling timer is needed. It converges on
// REMAINING memory: while free is below the model's checkpoint floor (the
// in-flight writes' flush spike + the system-memory reserve), drain the largest
// bucket's tail into the spill manager, repeating within the same batch until
// free is back above the floor or the buckets are empty. Free memory sees what
// the static resident budget misses (concurrent tasks, V2 import, compaction, GC
// churn, cgo buffers); it can only push usage below what the budget accounts,
// never above it. See importutilv2/reshardmem.
func (r *reshardPlanExecutor) spillByMemory() error {
	if r.resident() <= 0 {
		return nil
	}
	total := int64(hardware.GetMemoryCount())
	floor := r.memory.model.CheckpointFloor(r.memory.budget, total)
	spilled, ranges, err := r.buckets.spillWhile(r.spillMgr, func() bool {
		return r.resident() > 0 && total-int64(hardware.GetUsedMemoryCount()) < floor
	})
	r.counters.spillBytes += spilled
	r.counters.spillRanges += ranges
	return err
}

func publishReshardManifest(ctx context.Context, cm storage.ChunkManager, req *datapb.ReshardTaskRequest, manifest *importv3pb.ReshardManifest) error {
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
