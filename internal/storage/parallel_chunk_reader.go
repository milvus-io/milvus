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

package storage

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// chunkResult is everything one chunk yields. A chunk is read to its end
// before it is handed over, so it is either complete or failed, never partial.
type chunkResult struct {
	// records are retained by the worker; whoever takes the result owns them.
	records []Record
	err     error
	// canceled marks a chunk given up because the reader was closed or another
	// chunk had already failed. It carries no error of its own.
	canceled bool
}

var _ RecordReader = (*parallelChunkRecordReader)(nil)

// parallelChunkRecordReader reads a chain of chunks with several of them in
// flight, and still delivers their records in chunk order.
//
// Each worker owns one chunk from start to finish: it opens the chunk, reads it
// to EOF and closes it. Nothing about a chunk stays on the consumer's critical
// path, which is the difference from IterativeRecordReader, where the consumer
// opens every chunk and triggers every read itself.
//
// Workers do not wait for the consumer. Up to the whole input can be decoded
// before the first record is consumed, so this reader is only for callers that
// materialize their input anyway (a sort); callers that stream must keep using
// IterativeRecordReader. What the reader adds on top of the decoded input is
// the state of the chunk readers still open, at most concurrency of them, and
// it is open's job to bound each one.
type parallelChunkRecordReader struct {
	ctx         context.Context
	numChunks   int
	concurrency int
	// open opens chunk i. It is called from several goroutines at once, each
	// with a different i.
	open func(chunk int) (RecordReader, error)

	// results has one slot per chunk. Every slot is filled exactly once, also
	// after a stop, so Close can drain them all without blocking.
	results []chan chunkResult
	claimed atomic.Int64
	stop    chan struct{}
	halt    sync.Once
	wg      sync.WaitGroup

	failMu  sync.Mutex
	failure error

	// Consumer state, touched only by Next and Close.
	started bool
	closed  bool
	err     error
	chunk   int
	pending []Record
	lent    Record
}

func newParallelChunkRecordReader(
	ctx context.Context,
	numChunks int,
	concurrency int,
	open func(chunk int) (RecordReader, error),
) *parallelChunkRecordReader {
	return &parallelChunkRecordReader{
		ctx:         ctx,
		numChunks:   numChunks,
		concurrency: min(concurrency, numChunks),
		open:        open,
		stop:        make(chan struct{}),
	}
}

func (r *parallelChunkRecordReader) start() {
	if r.started {
		return
	}
	r.started = true
	r.results = make([]chan chunkResult, r.numChunks)
	for i := range r.results {
		r.results[i] = make(chan chunkResult, 1)
	}
	r.wg.Add(r.concurrency)
	for i := 0; i < r.concurrency; i++ {
		go r.work()
	}
}

func (r *parallelChunkRecordReader) work() {
	defer r.wg.Done()
	for {
		chunk := int(r.claimed.Add(1)) - 1
		if chunk >= r.numChunks {
			return
		}
		r.results[chunk] <- r.readChunk(chunk)
	}
}

func (r *parallelChunkRecordReader) stopped() bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

// fail records the first failure and stops the remaining reads: once one chunk
// has failed the consumer cannot finish, so further downloads are wasted.
func (r *parallelChunkRecordReader) fail(err error) {
	r.failMu.Lock()
	if r.failure == nil {
		r.failure = err
	}
	r.failMu.Unlock()
	r.halt.Do(func() { close(r.stop) })
}

func (r *parallelChunkRecordReader) firstFailure() error {
	r.failMu.Lock()
	defer r.failMu.Unlock()
	return r.failure
}

func (r *parallelChunkRecordReader) readChunk(chunk int) (res chunkResult) {
	defer func() {
		if x := recover(); x != nil {
			res.err = merr.WrapErrServiceInternalMsg("internal error recovered: %v", x)
		}
		if res.err != nil || res.canceled {
			releaseRecords(res.records)
			res.records = nil
		}
		if res.err != nil {
			r.fail(res.err)
		}
	}()

	if r.stopped() {
		return chunkResult{canceled: true}
	}
	if err := r.ctx.Err(); err != nil {
		return chunkResult{err: err}
	}
	reader, err := r.open(chunk)
	if err != nil {
		// Do not touch reader: open may hand back a typed-nil alongside err.
		return chunkResult{err: err}
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && res.err == nil && !res.canceled {
			res.err = closeErr
		}
	}()

	for {
		if r.stopped() {
			res.canceled = true
			return res
		}
		if err := r.ctx.Err(); err != nil {
			res.err = err
			return res
		}
		rec, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return res
		}
		if err != nil {
			res.err = err
			return res
		}
		// rec is only borrowed until the next Next on the chunk reader.
		rec.Retain()
		res.records = append(res.records, rec)
	}
}

// Next implements RecordReader.
func (r *parallelChunkRecordReader) Next() (Record, error) {
	r.releaseLent()
	if r.closed {
		return nil, io.EOF
	}
	if r.err != nil {
		return nil, r.err
	}
	r.start()
	for len(r.pending) == 0 {
		if r.chunk >= r.numChunks {
			return nil, io.EOF
		}
		res := <-r.results[r.chunk]
		r.chunk++
		if res.err != nil {
			r.err = res.err
			return nil, r.err
		}
		if res.canceled {
			// Only a failure elsewhere cancels a chunk while the reader is
			// still open; report that failure, not the cancellation.
			r.err = r.firstFailure()
			return nil, r.err
		}
		r.pending = res.records
	}
	rec := r.pending[0]
	r.pending[0] = nil
	r.pending = r.pending[1:]
	r.lent = rec
	return rec, nil
}

// releaseLent drops the reader's reference to the record handed out by the
// previous Next, which per the RecordReader contract is only borrowed.
func (r *parallelChunkRecordReader) releaseLent() {
	if r.lent != nil {
		r.lent.Release()
		r.lent = nil
	}
}

// Close implements RecordReader. It stops the workers, waits for them, and
// releases every record that was read but never handed out.
func (r *parallelChunkRecordReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.releaseLent()
	releaseRecords(r.pending)
	r.pending = nil
	if !r.started {
		return nil
	}
	r.halt.Do(func() { close(r.stop) })
	r.wg.Wait()
	for ; r.chunk < r.numChunks; r.chunk++ {
		res := <-r.results[r.chunk]
		releaseRecords(res.records)
	}
	return nil
}

func releaseRecords(records []Record) {
	for _, rec := range records {
		rec.Release()
	}
}
