// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the source side of one reshard run: the per-file reader, the
// prepare stage that normalizes and function-processes each batch one read
// ahead, and the source stream that presents every file as one stream of
// prepared batches with the read-side stop checks in one place.

import (
	"context"
	"io"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// sourceStream presents a run's sources as one stream of prepared batches: it
// opens each file in turn, runs the shared prepare stage over it, applies the
// read-side stop checks in one place, and closes each file before the next one
// opens. Callers see only prepared, non-empty batches.
type sourceStream struct {
	spec    *sourceSpec
	files   []*internalpb.ImportFile
	runCtx  context.Context // run ctx: bounds function execution, checked between batches
	writer  *fragmentWriter // the source ctx derives from its ctx; its error stops the read side
	timings *reshardStageTimings

	// readWait is the time the routing side was starved of the next prepared
	// batch (read + normalize + function execution). Routing-side single
	// goroutine, folded into the run counters when the stream drains.
	readWait time.Duration

	index        int
	file         *internalpb.ImportFile
	reader       importutilv2.Reader
	stage        *reshardPrepareStage
	cancelSource context.CancelFunc
}

func newReshardSourceStream(
	runCtx context.Context,
	spec *sourceSpec,
	files []*internalpb.ImportFile,
	writer *fragmentWriter,
	timings *reshardStageTimings,
) *sourceStream {
	return &sourceStream{
		spec:    spec,
		files:   files,
		runCtx:  runCtx,
		writer:  writer,
		timings: timings,
	}
}

// next returns the next prepared batch and the source file it belongs to.
// ok=false means every source is drained. The two read-side stop checks (run ctx
// cancellation and a detached write's first error) live here, and the current
// file is torn down before the next one opens.
func (s *sourceStream) next() (*storage.InsertData, *internalpb.ImportFile, bool, error) {
	for {
		if s.stage == nil {
			if s.index >= len(s.files) {
				return nil, nil, false, nil
			}
			if err := s.runCtx.Err(); err != nil {
				return nil, nil, false, err
			}
			// A detached write that failed has already canceled the write ctx;
			// stop instead of reading the next source for nothing.
			if err := s.writer.Err(); err != nil {
				return nil, nil, false, err
			}
			if err := s.open(s.files[s.index]); err != nil {
				return nil, nil, false, err
			}
			s.index++
		}
		if err := s.runCtx.Err(); err != nil {
			return nil, nil, false, err
		}
		if err := s.writer.Err(); err != nil {
			return nil, nil, false, err
		}
		receiveStart := time.Now()
		batch, ok, err := s.stage.next()
		s.readWait += time.Since(receiveStart)
		// A failed write cancels the write ctx, and that cancel is what unblocked
		// this receive: report the write's error rather than the source-side
		// cancel it caused. The error is stored before the cancel, so once the
		// cancel is observable the write error is too.
		if err := s.writer.Err(); err != nil {
			return nil, nil, false, err
		}
		if !ok {
			// Clean EOF: the prepare stage closes the channel after io.EOF. Tear
			// the file down before the next one opens, then let memory settle the
			// same way the source boundary always did. A canceled run can land
			// here too, but the ctx checks before the next source and before each
			// final cut catch it long before anything is published.
			s.close()
			debug.FreeOSMemory()
			continue
		}
		if err != nil {
			return nil, nil, false, merr.Wrapf(err, "prepare import source %d", s.file.GetId())
		}
		return batch, s.file, true, nil
	}
}

// open starts the next source's reader and prepare stage.
func (s *sourceStream) open(file *internalpb.ImportFile) error {
	if file == nil {
		return merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
	}
	// The source context bounds this reader's lifetime: canceling it interrupts
	// an in-flight Read (the reader's IO is bound to its construction ctx), so
	// any failure return stops the prepare stage without waiting out a slow or
	// hung read. It derives from the write ctx, not the run ctx, so a failed
	// fragment write also cuts the read side short instead of letting it read
	// the rest of the source for a run that is already failed.
	sourceCtx, cancelSource := context.WithCancel(s.writer.ctx)
	s.cancelSource = cancelSource
	reader, err := newReshardSourceReader(
		sourceCtx,
		s.spec.cm,
		s.spec.collectionSchema,
		file,
		s.spec.options,
		s.spec.bufferSize,
		s.spec.storageConfig,
		s.spec.pluginContext,
	)
	if err != nil {
		cancelSource()
		return err
	}
	s.file = file
	s.reader = reader
	s.stage = newReshardPrepareStage(
		sourceCtx,
		cancelSource,
		s.runCtx,
		reader,
		file,
		s.spec.backup,
		s.spec.collectionSchema,
		s.spec.runFunctions,
		s.spec.rlsPredicate,
		s.timings,
	)
	return nil
}

// close tears the current file's stage and reader down in the same order
// the per-source defers used to: the stage cancels and joins first, so the
// goroutine never observes a closed reader.
func (s *sourceStream) close() {
	if s.stage == nil {
		return
	}
	s.stage.close()
	s.reader.Close()
	s.cancelSource()
	s.file = nil
	s.reader = nil
	s.stage = nil
}

// reshardStageTimings breaks the prepare stage's per-batch work into the phases
// that decide whether the stage is the bottleneck and, if so, which phase:
// stageRead is the source reader (object-store IO plus decode), stageNormalize
// and stageFunctions are the two prepare steps, and stageSend is the time the
// stage blocked handing a batch to the routing side (downstream backpressure).
// The routing side separately records readWait (starved by this stage),
// routeCost and flushBlock. The stage is the limiter when readWait > 0 with
// stageSend ~= 0; it is reader-bound when stageRead dominates that stage cycle.
type reshardStageTimings struct {
	read      atomic.Int64
	normalize atomic.Int64
	functions atomic.Int64
	send      atomic.Int64
}

// reshardPrepareStage is one source's read/normalize/validate/function pipeline.
// A goroutine replays the reader strictly sequentially -- row counting,
// normalization (including the sequential preallocated-ID offset), RLS
// validation and function execution all keep read order without any sharing with
// the routing side -- streaming prepared batches into a buffered channel so
// source-side work overlaps fragment sort/write.
type reshardPrepareStage struct {
	file         *internalpb.ImportFile
	backup       bool
	schema       *schemapb.CollectionSchema
	runFunctions bool
	// rlsPredicate is enforced on every prepared batch, exactly where Import V2
	// checks: after normalization and before functions add their output columns.
	// Nil when the job skips RLS.
	rlsPredicate *planpb.Expr
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

// newReshardPrepareStage wires one source's prepare stage. It deliberately takes
// two contexts: sourceCtx bounds the source reader and is canceled on a source
// switch or flush failure, while runCtx drives function execution and is the run
// context, not the source context. revive's context-as-argument rule cannot
// express that split.
//
//nolint:revive // context-as-argument: two contexts by design (source cancel vs run functions)
func newReshardPrepareStage(
	sourceCtx context.Context,
	cancelSource context.CancelFunc,
	runCtx context.Context,
	reader importutilv2.Reader,
	file *internalpb.ImportFile,
	backup bool,
	schema *schemapb.CollectionSchema,
	runFunctions bool,
	rlsPredicate *planpb.Expr,
	timings *reshardStageTimings,
) *reshardPrepareStage {
	s := &reshardPrepareStage{
		file:         file,
		backup:       backup,
		schema:       schema,
		runFunctions: runFunctions,
		rlsPredicate: rlsPredicate,
		runCtx:       runCtx,
		cancelSource: cancelSource,
		timings:      timings,
	}
	s.results, s.stop = startReshardPrepare(sourceCtx, reader, s.prepare, timings)
	return s
}

// prepare runs inside the stage goroutine: count, normalize, validate the RLS
// write predicate, then execute the schema functions. A zero-row batch returns
// nil and is never sent.
func (s *reshardPrepareStage) prepare(batch *storage.InsertData) (*storage.InsertData, error) {
	rowNum, _ := importv2.GetInsertDataRowCount(batch, s.schema)
	normalizeStart := time.Now()
	err := normalizeReshardBatch(s.file, s.backup, s.schema, batch, rowNum, &s.idOffset)
	if err == nil && rowNum > 0 && s.rlsPredicate != nil {
		// Same RLS write-predicate check Import V2 runs on every batch, at the
		// same point: the batch is normalized (defaults, dynamic data) and the
		// functions have not yet added their output columns. A violating row
		// fails this reshard task, exactly as it fails the V2 import task.
		err = rlsutil.ValidateInsertDataByPredicate(s.runCtx, batch.Data, rowNum, s.rlsPredicate, "import", "check")
	}
	s.timings.normalize.Add(int64(time.Since(normalizeStart)))
	if err != nil {
		return nil, err
	}
	if rowNum == 0 {
		return nil, nil
	}
	if s.runFunctions {
		functionsStart := time.Now()
		err := runFieldFunctions(s.runCtx, s.schema, batch)
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

// close cancels the source ctx first (aborting an in-flight Read), then joins the
// stage goroutine. It must run before reader.Close so the goroutine never
// observes a closed reader.
func (s *reshardPrepareStage) close() {
	s.cancelSource()
	s.stop()
}

// reshardPrepareDepth bounds how far the source prepare stage runs ahead of the
// routing side: one batch buffered in the channel plus one in-flight batch being
// read, normalized, or function-processed inside the stage. The run-ahead exists
// so source reading, normalization, and function execution overlap fragment
// sort/write; deeper buffering only costs memory without helping steady state,
// which is bounded by the slower stage either way.
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
// reader.Close. timings records the read and hand-off phases; prepare records its
// own normalization and function phases.
func startReshardPrepare(
	ctx context.Context,
	reader importutilv2.Reader,
	prepare func(*storage.InsertData) (*storage.InsertData, error),
	timings *reshardStageTimings,
) (<-chan reshardPrepareResult, func()) {
	results := make(chan reshardPrepareResult, reshardPrepareDepth)
	ctx, cancel := context.WithCancel(ctx)
	// The prepare stage runs one batch ahead on its own goroutine. The future
	// joins it: the stop func cancels the shared ctx and waits for the goroutine
	// to close results.
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

func newReshardSourceReader(
	ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	file *internalpb.ImportFile,
	options importutilv2.Options,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	pluginContext *indexcgopb.StoragePluginContext,
) (importutilv2.Reader, error) {
	if file == nil {
		return nil, merr.WrapErrDataIntegrityMsg("nil ReshardTask source")
	}
	if importutilv2.IsBackup(options) {
		tsStart, tsEnd, err := importutilv2.ParseTimeRange(options)
		if err != nil {
			return nil, err
		}
		storageVersion, err := importutilv2.GetStorageVersion(options)
		if err != nil {
			return nil, err
		}
		return binlog.NewReader(ctx, cm, schema, storageConfig, storageVersion, file.GetPaths(), tsStart, tsEnd, int(bufferSize), "", pluginContext)
	}
	return importutilv2.NewReader(ctx, cm, schema, file, options, int(bufferSize), storageConfig)
}

// runFieldFunctions executes every schema function (TextEmbedding, BM25,
// MinHash) once per batch, at the same pipeline position as Import V2: after
// normalization and before hash routing. User-provided outputs that the property
// allows are preserved (TextEmbedding skips the provider) or deterministically
// recomputed (BM25/MinHash overwrite with identical values), matching V2
// semantics exactly.
func runFieldFunctions(ctx context.Context, schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(schema.GetFunctions()) == 0 {
		return nil
	}
	return embedding.RunAll(ctx, schema, data, embedding.RunOptions{
		ClusterID:           paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		DBName:              schema.GetDbName(),
		AllowNonBM25Outputs: common.GetCollectionAllowInsertNonBM25FunctionOutputs(schema.GetProperties()),
	})
}

func normalizeReshardBatch(
	file *internalpb.ImportFile,
	backup bool,
	schema *schemapb.CollectionSchema,
	data *storage.InsertData,
	rowNum int,
	idOffset *int64,
) error {
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
	if backup {
		return nil
	}
	return importv2.AppendPreallocatedSystemFields(schema, data, rowNum, file.GetIdRange(), idOffset)
}
