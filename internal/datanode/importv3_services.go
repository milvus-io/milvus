// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package datanode

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
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/datanode/importv3"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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

// cleanImportV3Prefixes removes the local Import V3 spill root left by
// previous process runs. DataNode V3 task state is memory-only and is never
// recovered from local spill files, so after restart every file under the
// spill root is garbage by definition. Only the spill root is touched: the
// import_v3 prefix is shared with durable reshard output under local
// storage (see importV3SpillRootDir).
func cleanImportV3Prefixes() {
	root := path.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir)
	if err := os.RemoveAll(root); err != nil {
		mlog.Warn(context.TODO(), "failed to clean import v3 local root", mlog.String("root", root), mlog.Err(err))
		return
	}
	mlog.Info(context.TODO(), "cleaned import v3 local root", mlog.String("root", root))
}

func (node *DataNode) createImportV3WorkerTask(
	_ context.Context,
	taskID, runID, slot int64,
	execute importv3.Run,
) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized")), nil
	}
	if err := node.importV3TaskMgr.Add(taskID, runID, slot, execute); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (node *DataNode) queryImportV3WorkerTask(
	ctx context.Context,
	taskID, runID int64,
	taskType taskcommon.Type,
) (*workerpb.QueryTaskResponse, error) {
	if node.importV3TaskMgr == nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized"))}, nil
	}
	snapshot, ok := node.importV3TaskMgr.Query(taskID, runID)
	if !ok {
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrNodeNotFound(node.GetNodeID(),
			"cannot find current import V3 task run"))}, nil
	}
	properties := taskcommon.NewProperties(nil)
	properties.AppendTaskState(importV3TaskCommonState(snapshot.State))
	properties.AppendReason(snapshot.Reason)

	var payload any
	switch taskType {
	case taskcommon.Reshard:
		payload = &datapb.QueryReshardTaskResponse{
			Status: merr.Success(), State: importV3WorkerState(snapshot.State), Reason: snapshot.Reason,
		}
	case taskcommon.ImportV3:
		response := &datapb.QueryImportTaskV3Response{
			Status: merr.Success(), State: importV3WorkerState(snapshot.State), Reason: snapshot.Reason,
			Segments: snapshot.Segments,
		}
		if snapshot.State == importv3.StateCompleted {
			resultBytes := proto.Size(response)
			grpcLimitBytes := min(
				paramtable.Get().DataNodeGrpcServerCfg.ServerMaxSendSize.GetAsInt(),
				paramtable.Get().DataNodeGrpcClientCfg.ClientMaxRecvSize.GetAsInt(),
			)
			if resultBytes >= grpcLimitBytes/2 {
				mlog.Warn(ctx, "import V3 result is close to the gRPC result limit",
					mlog.Int64("taskID", taskID),
					mlog.Int64("runID", runID),
					mlog.Int("segmentCount", len(snapshot.Segments)),
					mlog.Int("resultBytes", resultBytes),
					mlog.Int("grpcResultLimitBytes", grpcLimitBytes))
			}
		}
		payload = response
	default:
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg(
			"invalid V3 task type %q", taskType))}, nil
	}
	// Keep the concrete proto types at the boundary so wrapQueryTaskResult can
	// enforce the existing GetStatus payload contract.
	switch result := payload.(type) {
	case *datapb.QueryReshardTaskResponse:
		return wrapQueryTaskResult(result, properties)
	case *datapb.QueryImportTaskV3Response:
		return wrapQueryTaskResult(result, properties)
	default:
		// Defensive: a new task type must not crash the DataNode in the
		// query path; surface it as an error so DataCoord retries.
		return &workerpb.QueryTaskResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg(
			"unsupported import V3 query payload %T", payload))}, nil
	}
}

func (node *DataNode) dropImportV3WorkerTask(taskID, runID int64) (*commonpb.Status, error) {
	if node.importV3TaskMgr == nil {
		return merr.Success(), nil
	}
	// Best effort and idempotent.  A stale run must not cancel a newer run;
	// TaskManager.Drop returns false for both stale and already-absent tasks.
	node.importV3TaskMgr.Drop(taskID, runID)
	return merr.Success(), nil
}

func (node *DataNode) executeReshardTask(ctx context.Context, req *datapb.ReshardTaskRequest, runID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req == nil || req.GetRunId() != runID || req.GetStorageConfig() == nil || req.GetSlot() <= 0 || req.GetPlan() == nil {
		return merr.WrapErrImportSysFailedMsg("invalid or incomplete ReshardTask request")
	}
	plan := req.GetPlan()
	cm, err := node.storageFactory.NewChunkManager(ctx, req.GetStorageConfig())
	if err != nil {
		return err
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), plan.GetCollectionId())
	if err != nil {
		return err
	}
	return executeReshardPlan(ctx, cm, req, plan, pluginContext)
}

type reshardBucket struct {
	vchannelOrdinal  int
	partitionOrdinal int
	batches          []reshardBatch
	spillChunks      []string
	spillChunkBytes  []int64
	spillChunkRows   []int64
	bytes            int64
	logicalBytes     int64
	rows             int64
	nextSpillSeq     int
}

// reshardBatch is one routed batch held in a bucket together with the decoded
// byte count measured once at routing time. InsertData.GetMemorySize walks
// every string/JSON value, so measuring per batch once here replaces the
// repeated measurements that routing, resident accounting, and the sort-input
// split used to perform on the same batch.
type reshardBatch struct {
	data  *storage.InsertData
	bytes int64
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
	spillChunks   int
	spillBytes    int64
	sortReadCost  time.Duration
	sortCost      time.Duration
	sortWriteCost time.Duration
	// readWait is the time the routing side blocked waiting for the next
	// prepared source batch (read + normalize + function execution); flushWall
	// is the wall time spent inside bucket flushes (sort + write + upload).
	// Whichever dominates tells the operator whether the run is
	// read/prepare-bound or flush-bound; the gap to their sum is the
	// hash/routing CPU cost.
	readWait     time.Duration
	flushWall    time.Duration
	peakResident int64
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
// reader.Close.
// readReshardSource builds the reader with a ctx derived from the one passed
// here and cancels that ctx before calling stop: a reader's IO is bound to
// its construction ctx, so that cancel also interrupts an in-flight Read
// and the join never waits out a slow or hung read.
func startReshardPrepare(ctx context.Context, reader importutilv2.Reader, prepare func(*storage.InsertData) (*storage.InsertData, error)) (<-chan reshardPrepareResult, func()) {
	results := make(chan reshardPrepareResult, reshardPrepareDepth)
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(results)
		for {
			if err := ctx.Err(); err != nil {
				return
			}
			batch, err := reader.Read()
			if err == io.EOF {
				return
			}
			if err == nil {
				batch, err = prepare(batch)
			}
			if err != nil {
				select {
				case results <- reshardPrepareResult{err: err}:
				case <-ctx.Done():
				}
				return
			}
			if batch == nil {
				continue
			}
			select {
			case results <- reshardPrepareResult{batch: batch}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return results, func() { cancel(); wg.Wait() }
}

func executeReshardPlan(ctx context.Context, cm storage.ChunkManager, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, pluginContext *indexcgopb.StoragePluginContext) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "ImportV3-Reshard",
		trace.WithAttributes(
			attribute.Int64("job_id", req.GetJobId()),
			attribute.Int64("task_id", req.GetTaskId()),
			attribute.Int64("run_id", req.GetRunId()),
		))
	defer span.End()
	temporarySchema := plan.GetTempSchema()
	sortFields, err := importv3.SortFields(plan.GetSort(), temporarySchema)
	if err != nil {
		return err
	}
	bufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	maxFileSize := int64(paramtable.Get().DataNodeCfg.MaxImportFileSizeInGB.GetAsFloat() * 1024 * 1024 * 1024)
	fragmentTarget := plan.GetFragmentSize()
	// Contract: DataCoord validates a positive fragment size at plan build time.
	// A non-positive target would flush every non-empty bucket after every batch
	// (fragment count = batches x buckets), so fail loudly at the boundary too.
	if fragmentTarget <= 0 {
		return merr.WrapErrImportSysFailedMsg("invalid ReshardTask fragment size %d", fragmentTarget)
	}
	slot := req.GetSlot()
	if slot <= 0 {
		slot = 1
	}
	memoryBudget := slot * paramtable.Get().DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt64()
	spillRoot := path.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir,
		strconv.FormatInt(req.GetJobId(), 10), strconv.FormatInt(req.GetTaskId(), 10), strconv.FormatInt(req.GetRunId(), 10))
	if err := os.MkdirAll(spillRoot, 0o755); err != nil {
		what := "create reshard spill directory"
		if errors.Is(err, syscall.ENOSPC) {
			return merr.Wrap(merr.ErrServiceResourceInsufficient, what+": "+err.Error())
		}
		return merr.Wrapf(err, "%s", what)
	}
	defer func() {
		_ = os.RemoveAll(spillRoot)
		// Best-effort: remove the task-level directory if this was its last run.
		_ = os.Remove(path.Dir(spillRoot))
	}()

	manifest := &datapb.ReshardManifest{}
	buckets := make(map[reshardBucketKey]*reshardBucket)
	var residentBytes int64
	var fragmentSeq int64
	// Backup sources already carry every function output column; ordinary
	// sources get theirs computed here so fragments are uniform either way.
	runFunctions := !plan.GetBackup()

	memModel := reshardmem.Model{ReadBuffer: bufferSize, FragmentTarget: fragmentTarget}
	effectiveFragmentInput := memModel.SortInput(memoryBudget)
	var counters reshardRunCounters
	// flushBucket sorts and writes one bucket's fragments synchronously while
	// the prepare stage keeps reading ahead, so the source side no longer stalls
	// for the whole flush. Sequence numbers are assigned in flush order and
	// spill chunks are removed before the bucket resets, so the manifest and
	// the spill files' lifetime are exactly what the pre-pipeline serial run
	// produced.
	flushBucket := func(b *reshardBucket) error {
		flushStart := time.Now()
		defer func() { counters.flushWall += time.Since(flushStart) }()
		groups := splitReshardBucketForSort(b, effectiveFragmentInput)
		for _, group := range groups {
			if err := ctx.Err(); err != nil {
				return err
			}
			outcome, err := writeReshardFragment(ctx, req, plan, temporarySchema, b.vchannelOrdinal, b.partitionOrdinal, group, fragmentSeq, bufferSize, sortFields, pluginContext)
			if err != nil {
				return err
			}
			fragmentSeq++
			manifest.Fragments = append(manifest.Fragments, outcome.descriptor)
			counters.fragments++
			counters.rows += outcome.descriptor.GetRows()
			counters.logicalBytes += outcome.descriptor.GetLogicalBytes()
			counters.writtenBytes += int64(outcome.writtenUncompressed)
			if outcome.timings != nil {
				counters.sortReadCost += outcome.timings.ReadCost
				counters.sortCost += outcome.timings.SortCost
				counters.sortWriteCost += outcome.timings.WriteCost
			}
		}
		for _, chunk := range b.spillChunks {
			_ = os.Remove(chunk)
		}
		*b = reshardBucket{vchannelOrdinal: b.vchannelOrdinal, partitionOrdinal: b.partitionOrdinal}
		debug.FreeOSMemory()
		return nil
	}

	spillLargest := func() (int64, error) {
		var target *reshardBucket
		for _, b := range buckets {
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
		spillPath := path.Join(spillRoot, fmt.Sprintf("%d_%d_%d.arrow",
			target.vchannelOrdinal, plan.GetPartitions()[target.partitionOrdinal], target.nextSpillSeq))
		writer, err := importv3.NewSpillWriter(spillPath, temporarySchema)
		if err != nil {
			what := fmt.Sprintf("create reshard spill file %s", spillPath)
			if errors.Is(err, syscall.ENOSPC) {
				return 0, merr.Wrap(merr.ErrServiceResourceInsufficient, what+": "+err.Error())
			}
			return 0, merr.Wrapf(err, "%s", what)
		}
		for _, batch := range target.batches {
			if err := writer.Write(batch.data); err != nil {
				_ = writer.Close()
				what := fmt.Sprintf("write reshard spill file %s", spillPath)
				if errors.Is(err, syscall.ENOSPC) {
					return 0, merr.Wrap(merr.ErrServiceResourceInsufficient, what+": "+err.Error())
				}
				return 0, merr.Wrapf(err, "%s", what)
			}
		}
		if err := writer.Close(); err != nil {
			what := fmt.Sprintf("close reshard spill file %s", spillPath)
			if errors.Is(err, syscall.ENOSPC) {
				return 0, merr.Wrap(merr.ErrServiceResourceInsufficient, what+": "+err.Error())
			}
			return 0, merr.Wrapf(err, "%s", what)
		}
		spillRows := int64(0)
		for _, batch := range target.batches {
			spillRows += int64(batch.data.GetRowNum())
		}
		target.spillChunks = append(target.spillChunks, spillPath)
		target.spillChunkBytes = append(target.spillChunkBytes, target.bytes)
		target.spillChunkRows = append(target.spillChunkRows, spillRows)
		target.batches = nil
		target.nextSpillSeq++
		spilled := target.bytes
		target.bytes = 0
		counters.spillChunks++
		counters.spillBytes += spilled
		return spilled, nil
	}

	// readReshardSource prepares one source through the prepare stage, routing
	// every prepared batch and flushing full buckets synchronously. Reader
	// cleanup and the stage join ride on defers, so every failure return below
	// leaves nothing behind.
	readReshardSource := func(source *datapb.SourceFileSpec) error {
		if source == nil || source.GetFile() == nil {
			return merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
		}
		// The source context bounds this reader's lifetime: canceling it
		// interrupts an in-flight Read (the reader's IO is bound to its
		// construction ctx), so any failure return below stops the prepare
		// stage without waiting out a slow or hung read.
		sourceCtx, cancelSource := context.WithCancel(ctx)
		defer cancelSource()
		reader, err := newReshardSourceReader(sourceCtx, cm, plan.GetSchema(), source, bufferSize, req.GetStorageConfig(), pluginContext)
		if err != nil {
			return err
		}
		defer reader.Close()
		size, err := reader.Size()
		if err != nil {
			return merr.Wrapf(err, "get import source %d size", source.GetFile().GetId())
		}
		if size > maxFileSize {
			return merr.WrapErrParameterInvalidMsg("import file size (%d bytes) exceeds the maximum allowed size (%d bytes)", size, maxFileSize)
		}
		// The prepare closure runs inside the stage goroutine, which replays
		// the reader strictly sequentially: row counting, normalization
		// (including the sequential preallocated-ID offset), and function
		// execution all keep read order without any sharing with the routing
		// side. Function execution keeps the run ctx rather than the source
		// ctx, exactly as before the stage existed: only the source-side
		// cancel (flush failure, source switch) stops the stage, via the
		// send-side select and the join below.
		var idOffset int64
		prepare := func(batch *storage.InsertData) (*storage.InsertData, error) {
			rowNum, _ := importv2.GetInsertDataRowCount(batch, plan.GetSchema())
			if err := normalizeReshardBatch(source, plan.GetSchema(), batch, rowNum, &idOffset); err != nil {
				return nil, err
			}
			if rowNum == 0 {
				return nil, nil
			}
			if runFunctions {
				if err := runReshardFunctions(ctx, plan.GetSchema(), batch); err != nil {
					return nil, err
				}
			}
			return batch, nil
		}
		prepared, stopPrepare := startReshardPrepare(sourceCtx, reader, prepare)
		// Cancel before joining (abort the in-flight Read first), and join
		// before the deferred reader.Close, so the goroutine never observes a
		// closed reader.
		defer func() { cancelSource(); stopPrepare() }()

		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			receiveStart := time.Now()
			result, ok := <-prepared
			counters.readWait += time.Since(receiveStart)
			if !ok {
				// Clean EOF: the prepare stage closes the channel after io.EOF.
				// A canceled run can land here too, but the ctx checks before
				// the next source and before each final flush catch it long
				// before anything is published.
				return nil
			}
			if result.err != nil {
				return merr.Wrapf(result.err, "prepare import source %d", source.GetFile().GetId())
			}
			batch := result.batch
			hashed, err := importv2.HashDataBySchema(temporarySchema, plan.GetVchannels(), plan.GetPartitions(), batch)
			if err != nil {
				return err
			}
			for channelOrdinal := range hashed {
				for partitionOrdinal, bucketData := range hashed[channelOrdinal] {
					if bucketData.GetRowNum() == 0 {
						continue
					}
					key := reshardBucketKey{channelOrdinal: channelOrdinal, partitionOrdinal: partitionOrdinal}
					b := buckets[key]
					if b == nil {
						b = &reshardBucket{vchannelOrdinal: channelOrdinal, partitionOrdinal: partitionOrdinal}
						buckets[key] = b
					}
					// Measure the routed batch once; the bucket entry carries this
					// count to resident accounting and the sort-input split so the
					// O(rows) string/JSON walk is not repeated per batch.
					mem := int64(bucketData.GetMemorySize())
					appendReshardBatch(b, bucketData, mem)
					residentBytes += mem
					counters.peakResident = max(counters.peakResident, residentBytes)
					if b.logicalBytes >= fragmentTarget {
						freed := b.bytes
						if err := flushBucket(b); err != nil {
							return err
						}
						residentBytes -= freed
					} else if residentBytes > memoryBudget {
						spilled, err := spillLargest()
						if err != nil {
							return err
						}
						residentBytes -= spilled
					}
				}
			}
			// Natural checkpoint: exactly one memory read per source batch --
			// the batch cadence itself rate-limits the check, so no sampling
			// timer is needed. Converges on REMAINING memory: while free is
			// below the model's checkpoint floor (one flush spike + the
			// system-memory reserve), spill the largest bucket once per
			// batch. Free memory sees what the per-task budget misses
			// (concurrent tasks, V2 import, compaction, GC churn, cgo
			// buffers); this can only push usage below the static ceiling,
			// never above it. See importutilv2/reshardmem.
			if residentBytes > 0 {
				total := int64(hardware.GetMemoryCount())
				if total-int64(hardware.GetUsedMemoryCount()) < memModel.CheckpointFloor(memoryBudget, total) {
					spilled, err := spillLargest()
					if err != nil {
						return err
					}
					residentBytes -= spilled
				}
			}
		}
	}

	for _, source := range plan.GetSources() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := readReshardSource(source); err != nil {
			return err
		}
		debug.FreeOSMemory()
	}

	keys := make([]reshardBucketKey, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].channelOrdinal != keys[j].channelOrdinal {
			return keys[i].channelOrdinal < keys[j].channelOrdinal
		}
		return keys[i].partitionOrdinal < keys[j].partitionOrdinal
	})
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := buckets[key]
		if b.rows == 0 && len(b.spillChunks) == 0 {
			continue
		}
		if err := flushBucket(b); err != nil {
			return err
		}
	}

	mlog.Info(ctx, "import v3 reshard run finished",
		mlog.FieldJobID(req.GetJobId()),
		mlog.Int64("taskID", req.GetTaskId()),
		mlog.Int64("runID", req.GetRunId()),
		mlog.Int64("slot", slot),
		mlog.Int64("memoryBudget", memoryBudget),
		mlog.Int64("fragmentTarget", fragmentTarget),
		mlog.Int64("effectiveFragmentInput", effectiveFragmentInput),
		mlog.Int("buckets", len(buckets)),
		mlog.Int("sources", len(plan.GetSources())),
		mlog.Int("fragments", counters.fragments),
		mlog.Int64("rows", counters.rows),
		mlog.Int64("logicalBytes", counters.logicalBytes),
		mlog.Int64("writtenUncompressedBytes", counters.writtenBytes),
		mlog.Int("spillChunks", counters.spillChunks),
		mlog.Int64("spillBytes", counters.spillBytes),
		mlog.Int64("peakResidentBytes", counters.peakResident),
		mlog.Duration("readWait", counters.readWait),
		mlog.Duration("flushWall", counters.flushWall),
		mlog.Duration("sortReadCost", counters.sortReadCost),
		mlog.Duration("sortCost", counters.sortCost),
		mlog.Duration("sortWriteCost", counters.sortWriteCost),
	)
	return publishReshardManifest(ctx, cm, req, manifest)
}

type reshardBucketKey struct {
	channelOrdinal   int
	partitionOrdinal int
}

func appendReshardBatch(b *reshardBucket, data *storage.InsertData, mem int64) {
	b.batches = append(b.batches, reshardBatch{data: data, bytes: mem})
	b.bytes += mem
	b.logicalBytes += mem
	b.rows += int64(data.GetRowNum())
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
	return importv2.AppendPreallocatedSystemFields(schema, data, rowNum, source.GetFile().GetPreAllocatedAutoIds(), idOffset)
}

// reshardFragmentGroup is the bounded input of one storage.Sort call. Splitting
// a bucket into groups is what turns "the bucket is on disk" into a real memory
// bound for the sort: storage.Sort materializes its whole input.
type reshardFragmentGroup struct {
	spillChunks  []string
	batches      []*storage.InsertData
	rows         int64
	logicalBytes int64
}

// splitReshardBucketForSort packs spill chunks and the in-memory tail into
// contiguous groups that each fit inside one Sort's input budget. A single item
// larger than the budget stays alone; the existing slot estimate makes that a
// single-record pathological case.
func splitReshardBucketForSort(b *reshardBucket, limit int64) []reshardFragmentGroup {
	type item struct {
		chunk string
		batch *storage.InsertData
		bytes int64
		rows  int64
	}
	items := make([]item, 0, len(b.spillChunks)+len(b.batches))
	for i, chunk := range b.spillChunks {
		items = append(items, item{chunk: chunk, bytes: b.spillChunkBytes[i], rows: b.spillChunkRows[i]})
	}
	for _, batch := range b.batches {
		items = append(items, item{batch: batch.data, bytes: batch.bytes, rows: int64(batch.data.GetRowNum())})
	}
	groups := make([]reshardFragmentGroup, 0, 1)
	if limit <= 0 {
		for _, it := range items {
			groups = append(groups, reshardFragmentGroup{rows: it.rows, logicalBytes: it.bytes})
			g := &groups[len(groups)-1]
			if it.chunk != "" {
				g.spillChunks = append(g.spillChunks, it.chunk)
			} else {
				g.batches = append(g.batches, it.batch)
			}
		}
		return groups
	}
	appendItem := func(g *reshardFragmentGroup, it item) {
		if it.chunk != "" {
			g.spillChunks = append(g.spillChunks, it.chunk)
		} else {
			g.batches = append(g.batches, it.batch)
		}
		g.rows += it.rows
		g.logicalBytes += it.bytes
	}
	for _, it := range items {
		if len(groups) == 0 || (groups[len(groups)-1].logicalBytes > 0 && groups[len(groups)-1].logicalBytes+it.bytes > limit) {
			groups = append(groups, reshardFragmentGroup{})
		}
		appendItem(&groups[len(groups)-1], it)
	}
	return groups
}

type importV3PackedRecordWriter interface {
	storage.RecordWriter
	GetWrittenRowNum() int64
	GetWrittenPaths(columnGroup typeutil.UniqueID) string
}

func newImportV3PackedRecordWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64, storageConfig *indexpb.StorageConfig, pluginContext *indexcgopb.StoragePluginContext) (importV3PackedRecordWriter, error) {
	fields := typeutil.GetAllFieldSchemas(schema)
	columns := make([]int, len(fields))
	fieldIDs := make([]int64, len(fields))
	for index, field := range fields {
		columns[index], fieldIDs[index] = index, field.GetFieldID()
	}
	return storage.NewPackedRecordWriterWithFieldIDNames(bucketName, paths, schema, bufferSize, packed.DefaultMultiPartUploadSize,
		[]storagecommon.ColumnGroup{{GroupID: 0, Columns: columns, Fields: fieldIDs}}, storageConfig, pluginContext)
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

func writeReshardFragment(ctx context.Context, req *datapb.ReshardTaskRequest, plan *datapb.ReshardTaskPlan, temporarySchema *schemapb.CollectionSchema, vchannelOrdinal, partitionOrdinal int, group reshardFragmentGroup, seq, bufferSize int64, sortFields []int64, pluginContext *indexcgopb.StoragePluginContext) (*reshardFragmentOutcome, error) {
	fragmentPath := path.Join(req.GetStorageConfig().GetRootPath(), metautil.BuildImportReshardOutputPath(req.GetJobId(), req.GetTaskId()), "fragments", strconv.Itoa(vchannelOrdinal), strconv.FormatInt(plan.GetPartitions()[partitionOrdinal], 10), fmt.Sprintf("%d_%d.parquet", req.GetRunId(), seq))
	writer, err := newImportV3PackedRecordWriter(req.GetStorageConfig().GetBucketName(), []string{fragmentPath}, temporarySchema, bufferSize, req.GetStorageConfig(), pluginContext)
	if err != nil {
		return nil, err
	}

	readers := make([]storage.RecordReader, 0, len(group.spillChunks)+len(group.batches))
	for _, chunk := range group.spillChunks {
		reader, err := importv3.NewSpillReader(chunk, temporarySchema)
		if err != nil {
			_ = writer.Close()
			// Readers opened so far own *os.File handles and ipc.Reader arrow
			// buffers; the deferred close is registered after the loops, so the
			// failure branch must release them explicitly.
			closeReshardReaders(readers)
			return nil, err
		}
		readers = append(readers, reader)
	}
	for _, batch := range group.batches {
		reader, err := storage.NewInsertDataRecordReader(batch, temporarySchema)
		if err != nil {
			_ = writer.Close()
			closeReshardReaders(readers)
			return nil, err
		}
		readers = append(readers, reader)
	}
	defer closeReshardReaders(readers)

	rows, timings, err := storage.Sort(uint64(bufferSize), temporarySchema, readers, writer, func(storage.Record, int, int) bool { return true }, sortFields)
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
			ChannelIndex: int32(vchannelOrdinal), PartitionIndex: int32(partitionOrdinal), Seq: seq,
			Path: writer.GetWrittenPaths(0), Rows: int64(rows), LogicalBytes: group.logicalBytes,
		},
		timings:             timings,
		writtenUncompressed: writer.GetWrittenUncompressed(),
	}, nil
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

func (node *DataNode) executeImportTaskV3(ctx context.Context, req *datapb.ImportTaskV3Request, runID int64) ([]*datapb.SegmentResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil || req.GetRunId() != runID || req.GetStorageConfig() == nil || req.GetSlot() <= 0 || req.GetPlan() == nil {
		return nil, merr.WrapErrImportSysFailedMsg("invalid or incomplete ImportTaskV3 request")
	}
	plan := req.GetPlan()
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), plan.GetCollectionId())
	if err != nil {
		return nil, err
	}
	cm, err := node.storageFactory.NewChunkManager(ctx, req.GetStorageConfig())
	if err != nil {
		return nil, err
	}
	return executeImportPlan(ctx, cm, req, plan, pluginContext)
}

func executeImportPlan(ctx context.Context, cm storage.ChunkManager, req *datapb.ImportTaskV3Request, plan *datapb.ImportTaskPlan, pluginContext *indexcgopb.StoragePluginContext) ([]*datapb.SegmentResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "ImportV3-Import",
		trace.WithAttributes(
			attribute.Int64("job_id", req.GetJobId()),
			attribute.Int64("task_id", req.GetTaskId()),
			attribute.Int64("run_id", req.GetRunId()),
			attribute.Int64("segment_id", req.GetSegmentId()),
		))
	defer span.End()
	temporarySchema := plan.GetTempSchema()
	targetSchema := plan.GetSchema()
	sortFields, err := importv3.SortFields(plan.GetSort(), temporarySchema)
	if err != nil {
		return nil, err
	}
	logAllocator := allocator.NewLocalAllocator(req.GetLogRange().GetBegin(), req.GetLogRange().GetEnd())
	writerSpec := plan.GetWriter()
	segmentID := req.GetSegmentId()
	// The final segment writer must encrypt with the target collection's zone,
	// never the read/source context (which for a backup import is the source
	// cluster's zone). Passing no plugin context lets the writer derive the
	// target zone from the schema properties, matching the V2 import path. The
	// read paths below keep pluginContext so backup fragments decrypt correctly.
	writerOptions, err := buildImportV3WriterOptions(req.GetStorageConfig(), plan.GetCollectionId(), plan.GetPartitionId(), targetSchema, writerSpec)
	if err != nil {
		return nil, err
	}
	sources := make([]importv3.Source, 0, len(plan.GetFragments()))
	for _, ref := range plan.GetFragments() {
		source, err := importv3.SourceFromFragment(ref, temporarySchema, paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64(), req.GetStorageConfig(), pluginContext)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	var writer storage.BinlogRecordWriter
	finalWriter := func(_ context.Context) (storage.RecordWriter, error) {
		if writer != nil {
			return writer, nil
		}
		writer, err = storage.NewBinlogRecordWriter(ctx, plan.GetCollectionId(), plan.GetPartitionId(), segmentID, targetSchema, logAllocator, uint64(paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsInt64()), plan.GetRows(), writerOptions...)
		return writer, err
	}
	predicate := importv3.NewTTLOnlyPredicate(temporarySchema, writerSpec.GetTtlNanos(), plan.GetDataTs())
	executor := &importv3.MergeExecutor{
		FanIn: int(plan.GetFanIn()), BatchSize: uint64(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()),
		Schema: temporarySchema, SortFields: sortFields, Predicate: predicate,
		FinalWriter: func(output storage.RecordWriter) storage.RecordWriter {
			return newImportV3FinalWriter(ctx, output, temporarySchema, targetSchema, plan.GetDataTs(), plan.GetBackup())
		},
		Intermediate: newImportV3IntermediateFactory(req, 0, temporarySchema, pluginContext),
	}
	rows, err := executor.Execute(ctx, sources, finalWriter)
	if err != nil {
		return nil, err
	}
	segmentResult := &datapb.SegmentResult{Rows: rows}
	if writer != nil {
		fieldBinlogs, statsLog, bm25Logs, manifestPath, expiration := writer.GetLogs()
		insertLogs := storage.SortFieldBinlogs(fieldBinlogs)
		sortedBM25Logs := storage.SortFieldBinlogs(bm25Logs)
		statistics := storage.BuildStatsFromFieldBinlogs(insertLogs, nil, sortedBM25Logs, nil)
		statistics.StatsBinlogSize = writer.GetStatsBlobSize()
		segmentResult.Statistics = statistics
		segmentResult.ExpirationQuantiles = expiration
		if manifestPath != "" {
			// Storage V3: only the manifest path crosses the RPC boundary. The
			// coordinator persists SegmentInfo from the manifest/stats, not from
			// FieldBinlog arrays.
			segmentResult.ManifestPath = manifestPath
		} else {
			segmentResult.InsertLogs = insertLogs
			segmentResult.PkLog = statsLog
			segmentResult.Bm25Logs = sortedBM25Logs
		}
		if writer.GetRowNum() != rows {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 writer/result row mismatch: writer=%d result=%d", writer.GetRowNum(), rows)
		}
	} else if rows != 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 kept rows without materializing a writer: %d", rows)
	}
	return []*datapb.SegmentResult{segmentResult}, nil
}

// buildImportV3WriterOptions builds the options for the final segment writer.
// It deliberately takes no plugin context: the writer must encrypt with the
// target collection's encryption zone, which the writer derives from the schema
// properties (see storage.NewBinlogRecordWriter). Feeding the read/source
// context here would encrypt target data with a foreign zone's key.
func buildImportV3WriterOptions(storageConfig *indexpb.StorageConfig, collectionID, partitionID int64, targetSchema *schemapb.CollectionSchema, spec *datapb.WriterSpec) ([]storage.RwOption, error) {
	bfType := bloomfilter.BFTypeFromString(spec.GetBloomType())
	if (bfType != bloomfilter.BasicBF && bfType != bloomfilter.BlockedBF) || spec.GetBloomFpp() <= 0 || spec.GetBloomFpp() >= 1 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec Bloom filter config is invalid")
	}
	if spec.GetFormat() == "" {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec writer format is empty")
	}
	groups, err := importV3Groups(targetSchema, spec.GetGroups())
	if err != nil {
		return nil, err
	}
	bufferSize := int64(packed.DefaultWriteBufferSize)
	multipartSize := int64(packed.DefaultMultiPartUploadSize)
	if spec.GetV2() != nil {
		if spec.GetV2().GetBufferSize() <= 0 || spec.GetV2().GetMultipartSize() <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec V2 IO sizes must be positive")
		}
		bufferSize = spec.GetV2().GetBufferSize()
		multipartSize = spec.GetV2().GetMultipartSize()
	}
	options := []storage.RwOption{
		storage.WithVersion(spec.GetStorageVersion()),
		storage.WithBufferSize(bufferSize),
		storage.WithMultiPartUploadSize(multipartSize),
		storage.WithColumnGroups(groups),
		storage.WithStorageConfig(storageConfig),
		storage.WithWriterFormat(spec.GetFormat()),
		storage.WithPkStatsConfig(storage.PkStatsConfig{
			Capacity: spec.GetPkCapacity(), BloomFilterType: spec.GetBloomType(), MaxBloomFalsePositive: spec.GetBloomFpp(),
		}),
	}
	if len(spec.GetText()) > 0 {
		partitionBase := path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10))
		textConfigs := make([]packed.TextColumnConfig, 0, len(spec.GetText()))
		for _, text := range spec.GetText() {
			if text == nil || text.GetFieldId() < common.StartOfUserFieldID || text.GetInlineLimit() < 0 || text.GetLobLimit() <= 0 || text.GetFlushLimit() <= 0 {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec TEXT config is invalid")
			}
			textConfigs = append(textConfigs, packed.TextColumnConfig{
				FieldID: text.GetFieldId(), LobBasePath: path.Join(partitionBase, "lobs", strconv.FormatInt(text.GetFieldId(), 10)),
				InlineThreshold: text.GetInlineLimit(), MaxLobFileBytes: text.GetLobLimit(), FlushThresholdBytes: text.GetFlushLimit(),
			})
		}
		options = append(options, storage.WithTextColumnConfigs(textConfigs))
	}
	return options, nil
}

func importV3Groups(schema *schemapb.CollectionSchema, specs []*datapb.ColumnGroupSpec) ([]storagecommon.ColumnGroup, error) {
	fields := typeutil.GetAllFieldSchemas(schema)
	fieldColumns := make(map[int64]int, len(fields))
	for column, field := range fields {
		fieldColumns[field.GetFieldID()] = column
	}
	if len(specs) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column groups are empty")
	}
	groups := make([]storagecommon.ColumnGroup, 0, len(specs))
	covered := make(map[int64]struct{}, len(fields))
	for _, spec := range specs {
		if spec == nil || len(spec.GetFields()) == 0 || spec.GetFormat() == "" {
			return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column group is incomplete")
		}
		group := storagecommon.ColumnGroup{GroupID: spec.GetId(), Format: spec.GetFormat()}
		for _, fieldID := range spec.GetFields() {
			column, ok := fieldColumns[fieldID]
			if !ok {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column group references unknown field %d", fieldID)
			}
			if _, ok := covered[fieldID]; ok {
				return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column groups overlap at field %d", fieldID)
			}
			covered[fieldID] = struct{}{}
			group.Fields = append(group.Fields, fieldID)
			group.Columns = append(group.Columns, column)
		}
		groups = append(groups, group)
	}
	if len(covered) != len(fields) {
		return nil, merr.WrapErrDataIntegrityMsg("ImportTaskV3 WriterSpec column groups do not cover target schema: covered=%d fields=%d", len(covered), len(fields))
	}
	return groups, nil
}

type importV3FinalWriter struct {
	ctx             context.Context
	output          storage.RecordWriter
	temporarySchema *schemapb.CollectionSchema
	targetSchema    *schemapb.CollectionSchema
	dataTS          uint64
	backup          bool
	// requiredFields is the immutable set of temporary-schema field IDs that
	// RecordToInsertData must find in every merged record. The temporary schema
	// never changes for the lifetime of the writer, so building the set per
	// batch only repeated an allocation and a map insert loop on the hottest
	// path of the import stage.
	requiredFields typeutil.Set[int64]
}

func newImportV3FinalWriter(ctx context.Context, output storage.RecordWriter, temporarySchema, targetSchema *schemapb.CollectionSchema, dataTS uint64, backup bool) storage.RecordWriter {
	required := typeutil.NewSet[int64]()
	for _, field := range typeutil.GetAllFieldSchemas(temporarySchema) {
		required.Insert(field.GetFieldID())
	}
	return &importV3FinalWriter{ctx: ctx, output: output, temporarySchema: temporarySchema, targetSchema: targetSchema, dataTS: dataTS, backup: backup, requiredFields: required}
}

// Write performs the final transform: every function output column is already
// physically present in the fragments (Reshard computed it for ordinary
// imports, the source binlog carries it for backups), so the only remaining
// work is materializing the timestamp column for ordinary imports and handing
// the record to the formal writer.
func (w *importV3FinalWriter) Write(record storage.Record) error {
	data, err := storage.RecordToInsertData(record, w.targetSchema, w.requiredFields)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "convert ImportTaskV3 merged record")
	}
	rows := data.GetRowNum()
	if rows == 0 {
		return nil
	}
	ts := data.Data[common.TimeStampField]
	if w.backup {
		// Backup fragments carry the source binlog timestamp; the final merge must
		// not overwrite it with the import data timestamp.
		if ts == nil {
			return merr.WrapErrDataIntegrityMsg("ImportTaskV3 backup timestamp is missing")
		}
		if ts.RowNum() != rows {
			return merr.WrapErrDataIntegrityMsg("ImportTaskV3 backup timestamp rows mismatch: timestamps=%d rows=%d", ts.RowNum(), rows)
		}
	} else if ts == nil || ts.RowNum() == 0 {
		timestamps := make([]int64, rows)
		for index := range timestamps {
			timestamps[index] = int64(w.dataTS)
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: timestamps}
	} else if ts.RowNum() != rows {
		return merr.WrapErrDataIntegrityMsg("ImportTaskV3 source timestamp rows mismatch: timestamps=%d rows=%d", ts.RowNum(), rows)
	}
	reader, err := storage.NewInsertDataRecordReader(data, w.targetSchema)
	if err != nil {
		return err
	}
	defer reader.Close()
	finalRecord, err := reader.Next()
	if err != nil {
		return merr.Wrap(err, "build ImportTaskV3 final record")
	}
	return w.output.Write(finalRecord)
}

func (w *importV3FinalWriter) GetWrittenUncompressed() uint64 {
	return w.output.GetWrittenUncompressed()
}

func (w *importV3FinalWriter) Close() error {
	return w.output.Close()
}

func newImportV3IntermediateFactory(req *datapb.ImportTaskV3Request, segmentIndex int, schema *schemapb.CollectionSchema, pluginContext *indexcgopb.StoragePluginContext) importv3.IntermediateWriterFactory {
	return func(_ context.Context, round, group int) (storage.RecordWriter, func(int64) (importv3.Source, error), error) {
		intermediatePath := path.Join(req.GetStorageConfig().GetRootPath(), metautil.BuildImportV3ImportOutputPath(req.GetJobId(), req.GetTaskId()), "merge", fmt.Sprintf("%d_%d_%d_%d.parquet", req.GetRunId(), segmentIndex, round, group))
		writer, err := newImportV3PackedRecordWriter(req.GetStorageConfig().GetBucketName(), []string{intermediatePath}, schema,
			paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64(), req.GetStorageConfig(), pluginContext)
		if err != nil {
			return nil, nil, err
		}
		commit := func(rows int64) (importv3.Source, error) {
			if rows <= 0 || writer.GetWrittenRowNum() != rows {
				return importv3.Source{}, merr.WrapErrDataIntegrityMsg("ImportTaskV3 intermediate rows mismatch: writer=%d merge=%d", writer.GetWrittenRowNum(), rows)
			}
			return importv3.Source{
				ID: intermediatePath, Rows: rows,
				Open: func(ctx context.Context) (storage.RecordReader, error) {
					return storage.NewImportFragmentRecordReader(ctx, storage.ImportFragmentReaderSpec{Path: intermediatePath, Rows: rows}, schema,
						storage.WithBufferSize(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()), storage.WithStorageConfig(req.GetStorageConfig()), storage.WithPluginContext(pluginContext))
				},
			}, nil
		}
		return writer, commit, nil
	}
}

func importV3TaskCommonState(state importv3.State) taskcommon.State {
	switch state {
	case importv3.StatePending:
		return taskcommon.Init
	case importv3.StateRunning:
		return taskcommon.InProgress
	case importv3.StateRetry:
		return taskcommon.Retry
	case importv3.StateCompleted:
		return taskcommon.Finished
	case importv3.StateFailed:
		return taskcommon.Failed
	default:
		return taskcommon.None
	}
}

func importV3WorkerState(state importv3.State) datapb.ImportTaskStateV2 {
	switch state {
	case importv3.StatePending:
		return datapb.ImportTaskStateV2_Pending
	case importv3.StateRunning:
		return datapb.ImportTaskStateV2_InProgress
	case importv3.StateRetry:
		return datapb.ImportTaskStateV2_Retry
	case importv3.StateCompleted:
		return datapb.ImportTaskStateV2_Completed
	case importv3.StateFailed:
		return datapb.ImportTaskStateV2_Failed
	default:
		return datapb.ImportTaskStateV2_None
	}
}
