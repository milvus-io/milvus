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
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refTrackedRecord tracks its reference count so tests can prove that every
// record a reader touched ends up released.
type refTrackedRecord struct {
	chunk, seq int
	refs       atomic.Int32
}

func (r *refTrackedRecord) Column(FieldID) arrow.Array { return nil }
func (r *refTrackedRecord) Len() int                   { return 1 }
func (r *refTrackedRecord) Retain()                    { r.refs.Add(1) }
func (r *refTrackedRecord) Release()                   { r.refs.Add(-1) }

// fakeChunk describes one chunk and records what happened to it.
type fakeChunk struct {
	rows    int
	openErr error
	// failAt makes the failAt-th Next (0-based) return nextErr.
	failAt   int
	nextErr  error
	panicAt  int
	closeErr error
	// gate, when set, blocks the first Next until it is closed.
	gate chan struct{}

	mu      sync.Mutex
	records []*refTrackedRecord
	opened  bool
	closed  bool
}

func (c *fakeChunk) isOpened() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opened
}

// fakeChunkReader borrows records the way PackedReader does: the record
// returned by Next is released by the following Next or by Close.
type fakeChunkReader struct {
	id    int
	chunk *fakeChunk
	n     int
	cur   *refTrackedRecord
}

func (r *fakeChunkReader) Next() (Record, error) {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.n == 0 && r.chunk.gate != nil {
		<-r.chunk.gate
	}
	if r.chunk.panicAt == r.n+1 {
		panic("boom")
	}
	if r.chunk.nextErr != nil && r.chunk.failAt == r.n {
		return nil, r.chunk.nextErr
	}
	if r.n >= r.chunk.rows {
		return nil, io.EOF
	}
	rec := &refTrackedRecord{chunk: r.id, seq: r.n}
	rec.refs.Store(1)
	r.chunk.mu.Lock()
	r.chunk.records = append(r.chunk.records, rec)
	r.chunk.mu.Unlock()
	r.cur = rec
	r.n++
	return rec, nil
}

func (r *fakeChunkReader) Close() error {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	r.chunk.mu.Lock()
	r.chunk.closed = true
	r.chunk.mu.Unlock()
	return r.chunk.closeErr
}

type fakeChunks struct {
	chunks      []*fakeChunk
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	// hold keeps every open blocked until released, to observe concurrency.
	hold chan struct{}
}

func (f *fakeChunks) open(i int) (RecordReader, error) {
	cur := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		seen := f.maxInFlight.Load()
		if cur <= seen || f.maxInFlight.CompareAndSwap(seen, cur) {
			break
		}
	}
	if f.hold != nil {
		<-f.hold
	}
	c := f.chunks[i]
	if c.openErr != nil {
		// Mirror newPackedRecordReader callers: a typed-nil next to the error.
		var r *fakeChunkReader
		return r, c.openErr
	}
	c.mu.Lock()
	c.opened = true
	c.mu.Unlock()
	return &fakeChunkReader{id: i, chunk: c}, nil
}

func (f *fakeChunks) reader(ctx context.Context, concurrency int) *parallelChunkRecordReader {
	return newParallelChunkRecordReader(ctx, len(f.chunks), concurrency, f.open)
}

// assertSettled checks that every opened chunk was closed and that no record
// is still referenced, apart from the ones the test retained itself.
func (f *fakeChunks) assertSettled(t *testing.T, retained int32) {
	t.Helper()
	var refs int32
	for i, c := range f.chunks {
		c.mu.Lock()
		if c.opened {
			assert.True(t, c.closed, "chunk %d was opened but never closed", i)
		}
		for _, rec := range c.records {
			refs += rec.refs.Load()
		}
		c.mu.Unlock()
	}
	assert.Equal(t, retained, refs, "leaked or over-released record references")
}

func rowsOf(rows ...int) *fakeChunks {
	f := &fakeChunks{}
	for _, n := range rows {
		f.chunks = append(f.chunks, &fakeChunk{rows: n})
	}
	return f
}

func drain(t *testing.T, r RecordReader) ([][2]int, error) {
	t.Helper()
	var got [][2]int
	for {
		rec, err := r.Next()
		if err != nil {
			return got, err
		}
		cr := rec.(*refTrackedRecord)
		got = append(got, [2]int{cr.chunk, cr.seq})
	}
}

func TestParallelChunkReader_DeliversInOrderAndSkipsEmptyChunks(t *testing.T) {
	f := rowsOf(3, 0, 2, 0, 0, 4)
	// Let later chunks finish first: the first chunk is released last.
	f.chunks[0].gate = make(chan struct{})
	r := f.reader(context.Background(), 4)

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(f.chunks[0].gate)
	}()
	got, err := drain(t, r)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, [][2]int{{0, 0}, {0, 1}, {0, 2}, {2, 0}, {2, 1}, {5, 0}, {5, 1}, {5, 2}, {5, 3}}, got)

	_, err = r.Next()
	assert.ErrorIs(t, err, io.EOF, "EOF must be repeatable")
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_ReadsChunksConcurrently(t *testing.T) {
	f := rowsOf(1, 1, 1, 1, 1, 1, 1, 1)
	f.hold = make(chan struct{})
	r := f.reader(context.Background(), 3)

	done := make(chan error, 1)
	go func() {
		_, err := drain(t, r)
		done <- err
	}()
	require.Eventually(t, func() bool { return f.inFlight.Load() == 3 }, time.Second, time.Millisecond,
		"all workers must be opening a chunk at the same time")
	close(f.hold)
	assert.ErrorIs(t, <-done, io.EOF)
	assert.EqualValues(t, 3, f.maxInFlight.Load(), "never more chunks in flight than the configured concurrency")
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_ConcurrencyAboveChunkCount(t *testing.T) {
	f := rowsOf(2, 2)
	r := f.reader(context.Background(), 64)
	got, err := drain(t, r)
	assert.ErrorIs(t, err, io.EOF)
	assert.Len(t, got, 4)
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_NoChunks(t *testing.T) {
	r := rowsOf().reader(context.Background(), 4)
	_, err := r.Next()
	assert.ErrorIs(t, err, io.EOF)
	assert.NoError(t, r.Close())
}

func TestParallelChunkReader_RecordIsBorrowedUntilNextCall(t *testing.T) {
	f := rowsOf(2)
	r := f.reader(context.Background(), 2)

	first, err := r.Next()
	require.NoError(t, err)
	assert.EqualValues(t, 1, first.(*refTrackedRecord).refs.Load())
	first.Retain() // what Sort does to keep a record

	second, err := r.Next()
	require.NoError(t, err)
	assert.EqualValues(t, 1, first.(*refTrackedRecord).refs.Load(), "only the caller's reference is left")
	assert.EqualValues(t, 1, second.(*refTrackedRecord).refs.Load())

	assert.NoError(t, r.Close())
	f.assertSettled(t, 1)
	first.Release()
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_OpenErrorIsStickyAndReleasesEverything(t *testing.T) {
	openErr := errors.New("binlog object missing")
	f := rowsOf(2, 2, 2, 2, 2, 2)
	f.chunks[2].openErr = openErr
	r := f.reader(context.Background(), 3)

	_, err := drain(t, r)
	assert.ErrorIs(t, err, openErr)
	_, err = r.Next()
	assert.ErrorIs(t, err, openErr, "a failed reader keeps reporting the failure, not EOF")

	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_NextErrorMidChunk(t *testing.T) {
	readErr := errors.New("s3 throttled")
	f := rowsOf(3, 3, 3)
	f.chunks[1].failAt, f.chunks[1].nextErr = 1, readErr
	r := f.reader(context.Background(), 2)

	got, err := drain(t, r)
	assert.ErrorIs(t, err, readErr)
	for _, rec := range got {
		assert.Zero(t, rec[0], "a failed chunk must not deliver the records it read before failing")
	}
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_CanceledChunkReportsTheRealFailure(t *testing.T) {
	readErr := errors.New("corrupt file")
	f := rowsOf(1, 1, 1)
	// Chunk 0 is still reading when chunk 2 fails, so chunk 0 is canceled and
	// the consumer meets the canceled chunk before the failed one.
	f.chunks[0].gate = make(chan struct{})
	f.chunks[0].rows = 5
	f.chunks[2].failAt, f.chunks[2].nextErr = 0, readErr
	r := f.reader(context.Background(), 3)

	go func() {
		for r.firstFailure() == nil {
			time.Sleep(time.Millisecond)
		}
		close(f.chunks[0].gate)
	}()
	_, err := drain(t, r)
	assert.ErrorIs(t, err, readErr)
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_CanceledChunkCloseErrorKeepsTheRealFailure(t *testing.T) {
	// A canceled chunk's own close error must not mask the real failure that
	// stopped the reader, even when the consumer meets the canceled chunk
	// first.
	readErr := errors.New("corrupt file")
	closeErr := errors.New("close failed")
	f := rowsOf(1, 1, 1)
	f.chunks[0].gate = make(chan struct{})
	f.chunks[0].rows = 5
	f.chunks[0].closeErr = closeErr
	f.chunks[2].failAt, f.chunks[2].nextErr = 0, readErr
	r := f.reader(context.Background(), 3)

	go func() {
		for r.firstFailure() == nil {
			time.Sleep(time.Millisecond)
		}
		close(f.chunks[0].gate)
	}()
	_, err := drain(t, r)
	assert.ErrorIs(t, err, readErr, "a canceled chunk's close error must not mask the real failure")
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

// TestParallelChunkReader_CanceledChunkWithoutFailure returns a real error, not
// a nil record with a nil error, when a stop with no recorded failure (the
// shape Close gives the workers) cancels a chunk. It stops the workers the way
// Close does and lets the consumer observe the canceled chunk, which the
// single-consumer contract would otherwise leave to a Close/Next race.
func TestParallelChunkReader_CanceledChunkWithoutFailure(t *testing.T) {
	f := rowsOf(5)
	f.chunks[0].gate = make(chan struct{})
	r := f.reader(context.Background(), 1)

	done := make(chan error, 1)
	go func() {
		_, err := drain(t, r)
		done <- err
	}()
	require.Eventually(t, func() bool { return f.chunks[0].isOpened() }, time.Second, time.Millisecond)
	r.halt.Do(func() { close(r.stop) })
	close(f.chunks[0].gate)
	assert.ErrorIs(t, <-done, io.EOF, "a canceled chunk without a failure or close error reads as EOF")
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_CanceledChunkSurfacesItsCloseError(t *testing.T) {
	// A stop with no recorded failure must still surface the canceled chunk's
	// own close error instead of a nil record with a nil error.
	closeErr := errors.New("close failed")
	f := rowsOf(5)
	f.chunks[0].gate = make(chan struct{})
	f.chunks[0].closeErr = closeErr
	r := f.reader(context.Background(), 1)

	done := make(chan error, 1)
	go func() {
		_, err := drain(t, r)
		done <- err
	}()
	require.Eventually(t, func() bool { return f.chunks[0].isOpened() }, time.Second, time.Millisecond)
	r.halt.Do(func() { close(r.stop) })
	close(f.chunks[0].gate)
	assert.ErrorIs(t, <-done, closeErr)
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_CloseErrorFailsTheChunk(t *testing.T) {
	closeErr := errors.New("close failed")
	f := rowsOf(2, 2)
	f.chunks[0].closeErr = closeErr
	r := f.reader(context.Background(), 2)

	_, err := drain(t, r)
	assert.ErrorIs(t, err, closeErr)
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_PanicIsRecovered(t *testing.T) {
	f := rowsOf(3, 3)
	f.chunks[1].panicAt = 2
	r := f.reader(context.Background(), 2)

	_, err := drain(t, r)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_CloseMidwayStopsWorkersAndReleasesReadAhead(t *testing.T) {
	f := rowsOf(4, 4, 4, 4, 4, 4, 4, 4)
	r := f.reader(context.Background(), 4)

	_, err := r.Next()
	require.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.NoError(t, r.Close(), "Close is idempotent")
	_, err = r.Next()
	assert.ErrorIs(t, err, io.EOF, "a closed reader stays at EOF")
	f.assertSettled(t, 0)
}

func TestParallelChunkReader_CloseWithoutNext(t *testing.T) {
	f := rowsOf(1, 1)
	r := f.reader(context.Background(), 2)
	assert.NoError(t, r.Close())
	for _, c := range f.chunks {
		assert.False(t, c.opened, "nothing is opened before the first Next")
	}
}

func TestParallelChunkReader_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := rowsOf(2, 2, 2)
	f.chunks[0].gate = make(chan struct{})
	r := f.reader(ctx, 1)

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
		close(f.chunks[0].gate)
	}()
	_, err := drain(t, r)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NoError(t, r.Close())
	f.assertSettled(t, 0)
}
