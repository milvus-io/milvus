// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the detached fragment writes of one reshard run. Each fragment
// input is submitted to a bounded pool so the sort, encode and upload of one
// fragment overlap reading and hashing the next batches instead of stalling
// them. The writer is the only place that knows a write's lifecycle: the
// concurrency limit, the write goroutines, first-error propagation (whose cancel
// stops the read side), the seq-ordered outcomes and the in-flight bytes.

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// fragmentOutcome carries what one fragment write produced: the manifest
// descriptor plus the observations the reshard summary log needs. The writer's
// uncompressed byte count travels alongside the descriptor so the run summary
// can report the packing metric next to the Sort timings.
type fragmentOutcome struct {
	descriptor          *importv3pb.FragmentDescriptor
	timings             *storage.SortTimings
	writtenUncompressed uint64
}

// fragmentWriter owns the detached fragment writes of one reshard run: the
// concurrency limit, the write futures' lifecycle, first-error propagation
// (whose cancel stops the read side), the seq-ordered outcomes, and the
// in-flight accounted bytes of the fragment inputs currently being written. The
// pool is capped at the same N DataCoord charged the slot for
// (reshardmem.WorkingSet), so the live set stays inside the model; a full pool
// blocks the routing side exactly like the old synchronous flush did.
type fragmentWriter struct {
	ctx     context.Context
	cancel  context.CancelFunc
	pool    *conc.Pool[any]
	futures []*conc.Future[any]
	// mu guards err, flushed and flushWall: the routing side reads Err while
	// write goroutines store their results.
	mu        sync.Mutex
	err       error
	flushed   []*fragmentOutcome // indexed by sequence number
	flushWall time.Duration

	spec  *writeSpec
	spill *SpillManager
	// inflight is the accounted bytes of the fragment inputs dispatched but not
	// yet released by their write. Together with the bucket table's accounted
	// bytes it is the run's live set (see reshardPlanExecutor.resident); hence
	// atomic, since the write goroutines release it.
	inflight atomic.Int64
}

func newFragmentWriter(ctx context.Context, spec *writeSpec, spill *SpillManager, poolSize int64) *fragmentWriter {
	writeCtx, cancel := context.WithCancel(ctx)
	return &fragmentWriter{
		ctx:    writeCtx,
		cancel: cancel,
		pool:   conc.NewPool[any](int(poolSize)),
		spec:   spec,
		spill:  spill,
	}
}

// dispatch hands one fragment input to the write pool. The input takes over the
// accounted bytes of the pieces it carries and releases them when the write is
// done, so the run's resident set covers that memory for exactly as long as it is
// live. Sequence numbers are assigned by the caller in routing order. The
// returned duration is how long the routing side blocked waiting for a free write
// slot (Submit blocks when the pool is full, the semaphore semantics).
func (w *fragmentWriter) dispatch(vchannelOrdinal, partitionOrdinal int, input fragmentInput, seq int64) (time.Duration, error) {
	if err := w.Err(); err != nil {
		// Never submitted: release the input's spill ranges so spill accounting
		// stays balanced on the abort path. Its accounted bytes left the bucket
		// table when it was cut and were never added to inflight.
		w.spill.Release(input.ranges())
		return 0, err
	}
	blockStart := time.Now()
	w.inflight.Add(input.accounted)
	future := w.pool.Submit(func() (any, error) {
		// This write is the last reader of the input's spill ranges, so it owns
		// releasing them (which retires a drained shard file) and returning the
		// accounted bytes. Deferred so a panic in writeFragment -- which the pool
		// re-panics -- still balances the run's accounting.
		defer w.spill.Release(input.ranges())
		defer w.inflight.Add(-input.accounted)
		writeStart := time.Now()
		outcome, err := writeFragment(w.spec, w.spill, vchannelOrdinal, partitionOrdinal, input, seq)
		wall := time.Since(writeStart)
		w.mu.Lock()
		defer w.mu.Unlock()
		w.flushWall += wall
		if err != nil {
			if w.err == nil {
				w.err = err
				// Stop routing and the read side: the task is failed, so the
				// rest of the source is wasted work. A write already in flight
				// still runs to completion -- the writer takes no ctx -- and the
				// run joins it before returning.
				w.cancel()
			}
			return nil, nil
		}
		// Outcomes are indexed by sequence number, so the published manifest
		// keeps the exact fragment order the synchronous pipeline produced no
		// matter which write finishes first.
		for int64(len(w.flushed)) <= seq {
			w.flushed = append(w.flushed, nil)
		}
		w.flushed[seq] = outcome
		return nil, nil
	})
	blocked := time.Since(blockStart)
	// The write's own error travels through the pool's first-error record and
	// surfaces in wait()/Err(); a write failure cancels the read side, which
	// the read loop's per-batch Err checks report long before publish.
	w.futures = append(w.futures, future)
	return blocked, nil
}

// Err returns the first detached write error, if any.
func (w *fragmentWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// wait joins every detached write and returns the first write error, if any.
func (w *fragmentWriter) wait() error {
	if err := conc.AwaitAll(w.futures...); err != nil {
		return err
	}
	return w.Err()
}

// close is the deferred cleanup: cancel the read/write side, then join every
// write before the spill manager is closed and its directory wiped.
func (w *fragmentWriter) close() {
	w.cancel()
	_ = w.wait()
	// Release the underlying worker pool: it is created per run and never
	// reused, and ants only stops its purge/ticktock background goroutines on
	// Release. Without this every run leaks those goroutines and their tickers
	// for the lifetime of the process.
	w.pool.Release()
}

// outcomes returns the seq-ordered fragment outcomes. It must only be called
// after wait, when no write goroutine can still be running.
func (w *fragmentWriter) outcomes() []*fragmentOutcome {
	return w.flushed
}

func (w *fragmentWriter) flushWallDuration() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushWall
}

// inflightBytes is the accounted bytes of the fragment inputs dispatched but not
// yet released by their write.
func (w *fragmentWriter) inflightBytes() int64 {
	return w.inflight.Load()
}

// writeFragment sorts one fragment input into a single fragment parquet and
// returns its manifest descriptor plus the run observations.
func writeFragment(spec *writeSpec, spill *SpillManager, vchannelOrdinal, partitionOrdinal int, input fragmentInput, seq int64) (*fragmentOutcome, error) {
	req, plan := spec.req, spec.plan
	fragmentPath := path.Join(
		req.GetStorageConfig().GetRootPath(),
		metautil.BuildImportReshardOutputPath(req.GetJobId(), req.GetTaskId()),
		"fragments", strconv.Itoa(vchannelOrdinal), strconv.FormatInt(plan.GetPartitionIds()[partitionOrdinal], 10),
		fmt.Sprintf("%d_%d.parquet", req.GetRunId(), seq),
	)
	writer, err := newImportV3PackedRecordWriter(
		req.GetStorageConfig().GetBucketName(),
		[]string{fragmentPath},
		spec.fragmentSchema,
		spec.bufferSize,
		req.GetStorageConfig(),
		spec.pluginContext,
	)
	if err != nil {
		return nil, err
	}

	readers := make([]storage.RecordReader, 0, len(input.pieces))
	for i := range input.pieces {
		p := &input.pieces[i]
		var (
			reader storage.RecordReader
			err    error
		)
		if p.disk != nil {
			reader, err = spill.RangeReader(*p.disk)
		} else {
			reader, err = storage.NewInsertDataRecordReader(p.mem, spec.fragmentSchema)
		}
		if err != nil {
			_ = writer.Close()
			// Readers opened so far borrow the ipc.Reader arrow buffers; the
			// deferred close is registered after the loop, so the failure
			// branch must release them explicitly. The shared spill fds are
			// owned by the log, not by these readers.
			closeReshardReaders(readers)
			return nil, err
		}
		readers = append(readers, reader)
	}
	defer closeReshardReaders(readers)

	rows, timings, err := storage.Sort(
		uint64(spec.bufferSize),
		spec.fragmentSchema,
		readers,
		writer,
		func(storage.Record, int, int) bool { return true },
		spec.sortFields,
	)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if int64(rows) != input.rows || writer.GetWrittenRowNum() != int64(rows) {
		return nil, merr.WrapErrDataIntegrityMsg("fragment row count mismatch: input=%d sorted=%d written=%d", input.rows, rows, writer.GetWrittenRowNum())
	}
	// LogicalBytes publishes the fragment's normalized decoded bytes, the same
	// metric that cut this fragment out of its bucket (see bucket.append and
	// packFragments). Planning packs dataCoord.segment.maxSize against it; the
	// packed writer's uncompressed output runs below it by a schema-dependent
	// gap.
	return &fragmentOutcome{
		descriptor: &importv3pb.FragmentDescriptor{
			VchannelIndex: int32(vchannelOrdinal), PartitionId: plan.GetPartitionIds()[partitionOrdinal], Seq: seq,
			Path: writer.GetWrittenPaths(0), Rows: int64(rows), LogicalBytes: input.logicalBytes,
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
