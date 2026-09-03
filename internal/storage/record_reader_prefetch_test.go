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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

// newInt64Record builds a one-row record carrying v, so a test can read back
// which chunk and position a record came from.
func newInt64Record(alloc memory.Allocator, v int64) Record {
	b := array.NewInt64Builder(alloc)
	b.Append(v)
	col := b.NewArray()
	b.Release()
	return &compositeRecord{index: map[FieldID]int16{100: 0}, recs: []arrow.Array{col}}
}

func int64Of(rec Record) int64 {
	return rec.Column(100).(*array.Int64).Value(0)
}

// ownedChunkReader hands out records under the borrowed contract (the previous
// one is released on the next Next/Close) and, unlike borrowingRecordReader,
// also releases whatever it never handed out when closed. With a
// CheckedAllocator that turns "every reader the wrapper opened got closed" into
// an assertion instead of a leak nobody sees.
type ownedChunkReader struct {
	recs     []Record
	pos      int
	held     Record
	closed   bool
	closeErr error
}

func (r *ownedChunkReader) Next() (Record, error) {
	if r.held != nil {
		r.held.Release()
		r.held = nil
	}
	if r.pos >= len(r.recs) {
		return nil, io.EOF
	}
	r.held = r.recs[r.pos]
	r.pos++
	return r.held, nil
}

func (r *ownedChunkReader) Close() error {
	r.closed = true
	if r.held != nil {
		r.held.Release()
		r.held = nil
	}
	for ; r.pos < len(r.recs); r.pos++ {
		r.recs[r.pos].Release()
	}
	return r.closeErr
}

// chunkOf builds a reader over `rows` records whose values encode (chunk, row).
func chunkOf(alloc memory.Allocator, chunk, rows int) *ownedChunkReader {
	recs := make([]Record, 0, rows)
	for i := 0; i < rows; i++ {
		recs = append(recs, newInt64Record(alloc, int64(chunk*100+i)))
	}
	return &ownedChunkReader{recs: recs}
}

// drain reads until a non-nil error and returns the values seen and that error.
func drain(ir *IterativeRecordReader) ([]int64, error) {
	var got []int64
	for {
		rec, err := ir.Next()
		if err != nil {
			return got, err
		}
		got = append(got, int64Of(rec))
	}
}

func TestIterativeRecordReader_PrefetchPreservesOrder(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	chunks := []*ownedChunkReader{chunkOf(alloc, 0, 2), chunkOf(alloc, 1, 3), chunkOf(alloc, 2, 1)}
	call, invocations := 0, 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			invocations++
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	got, err := drain(ir)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []int64{0, 1, 100, 101, 102, 200}, got, "chunks must still come out in order")
	for i, c := range chunks {
		assert.True(t, c.closed, "chunk %d must be closed once drained", i)
	}
	assert.Equal(t, len(chunks)+1, invocations, "iterate is called once per chunk plus once to learn there are no more")

	// EOF is sticky: further Next calls do not go back to iterate().
	_, err = ir.Next()
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, len(chunks)+1, invocations)

	require.NoError(t, ir.Close())
	alloc.AssertSize(t, 0)
}

func TestIterativeRecordReader_PrefetchSkipsEmptyChunks(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	chunks := []*ownedChunkReader{
		chunkOf(alloc, 0, 0), // empty
		chunkOf(alloc, 1, 1),
		chunkOf(alloc, 2, 0), // empty
		chunkOf(alloc, 3, 0), // empty
		chunkOf(alloc, 4, 2),
	}
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	got, err := drain(ir)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []int64{100, 400, 401}, got)
	for i, c := range chunks {
		assert.True(t, c.closed, "chunk %d (including the empty ones) must be closed", i)
	}
	require.NoError(t, ir.Close())
	alloc.AssertSize(t, 0)
}

// TestIterativeRecordReader_PrefetchSurfacesOpenError is #50927 on the prefetch
// path: iterate() hands back a typed-nil reader together with the error. The
// error must reach the caller and Close() must not dereference the nil.
func TestIterativeRecordReader_PrefetchSurfacesOpenError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	missing := errors.New("IOError: Path does not exist")
	first := chunkOf(alloc, 0, 1)
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			call++
			if call == 1 {
				return first, nil
			}
			var pr *packedRecordReader
			return pr, missing
		},
	}

	got, err := drain(ir)
	assert.ErrorIs(t, err, missing)
	assert.Equal(t, []int64{0}, got, "records before the broken chunk are still delivered")
	assert.True(t, first.closed)
	assert.NotPanics(t, func() { assert.NoError(t, ir.Close()) })
	alloc.AssertSize(t, 0)
}

func TestIterativeRecordReader_PrefetchSurfacesFirstNextError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	broken := &failingFirstNextReader{err: errors.New("decode failed")}
	chunks := []RecordReader{chunkOf(alloc, 0, 1), broken}
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	_, err := drain(ir)
	assert.ErrorIs(t, err, broken.err)
	assert.True(t, broken.closed, "a chunk whose first read fails must still be closed")
	require.NoError(t, ir.Close())
	alloc.AssertSize(t, 0)
}

type failingFirstNextReader struct {
	err    error
	closed bool
}

func (r *failingFirstNextReader) Next() (Record, error) { return nil, r.err }
func (r *failingFirstNextReader) Close() error          { r.closed = true; return nil }

// TestIterativeRecordReader_PrefetchOverlapsNextOpen pins the point of the
// feature: the next chunk is being opened while the caller still holds the
// current one. iterate() for chunk 1 must be entered before chunk 0 is drained.
func TestIterativeRecordReader_PrefetchOverlapsNextOpen(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	chunks := []*ownedChunkReader{chunkOf(alloc, 0, 3), chunkOf(alloc, 1, 1)}
	secondOpened := make(chan struct{})
	var once sync.Once
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call == 1 {
				once.Do(func() { close(secondOpened) })
			}
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	rec, err := ir.Next() // installs chunk 0 and kicks off chunk 1
	require.NoError(t, err)
	assert.Equal(t, int64(0), int64Of(rec))

	select {
	case <-secondOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("chunk 1 was not opened while chunk 0 was still being consumed")
	}
	assert.False(t, chunks[0].closed, "chunk 0 is still live while chunk 1 is prefetched")

	got, err := drain(ir)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []int64{1, 2, 100}, got)
	require.NoError(t, ir.Close())
	alloc.AssertSize(t, 0)
}

// TestIterativeRecordReader_PrefetchCloseDrainsInFlight closes the wrapper while
// the next chunk is still being opened. Close must wait for that open, close the
// reader it produced (no leak, no goroutine left running iterate()), and never
// open anything further.
func TestIterativeRecordReader_PrefetchCloseDrainsInFlight(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	chunks := []*ownedChunkReader{chunkOf(alloc, 0, 2), chunkOf(alloc, 1, 2), chunkOf(alloc, 2, 2)}
	gate := make(chan struct{})
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call == 1 {
				<-gate // hold the open of chunk 1 until the test says so
			}
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	_, err := ir.Next() // chunk 0 live, chunk 1 open blocked on the gate
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() { closed <- ir.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned before the in-flight open finished")
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return once the in-flight open finished")
	}

	assert.True(t, chunks[0].closed)
	assert.True(t, chunks[1].closed, "the prefetched reader must be closed, not leaked")
	assert.False(t, chunks[2].closed)
	assert.Equal(t, 2, call, "nothing beyond the in-flight chunk is opened after Close")
	// chunk 2 was built by the test but never handed to the wrapper, so its
	// records are the test's to release; only then is a zero balance meaningful.
	require.NoError(t, chunks[2].Close())
	alloc.AssertSize(t, 0)
}

func TestIterativeRecordReader_PrefetchCloseReportsPrefetchedCloseError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	closeErr := errors.New("close failed")
	chunks := []*ownedChunkReader{chunkOf(alloc, 0, 1), chunkOf(alloc, 1, 1)}
	chunks[1].closeErr = closeErr
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	_, err := ir.Next()
	require.NoError(t, err)
	assert.ErrorIs(t, ir.Close(), closeErr, "an error closing the prefetched reader must not be swallowed")
	alloc.AssertSize(t, 0)
}

func TestIterativeRecordReader_PrefetchRecoversIteratePanic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	first := chunkOf(alloc, 0, 1)
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			call++
			if call == 1 {
				return first, nil
			}
			panic("boom")
		},
	}

	var err error
	assert.NotPanics(t, func() { _, err = drain(ir) })
	assert.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF, "a panic must surface as an error, not be mistaken for end of input")
	assert.NotPanics(t, func() { assert.NoError(t, ir.Close()) })
	alloc.AssertSize(t, 0)
}

// TestIterativeRecordReader_PrefetchCloseBeforeNext covers a wrapper that is
// closed without ever being read: nothing was opened, nothing to wait for.
func TestIterativeRecordReader_PrefetchCloseBeforeNext(t *testing.T) {
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate:  func() (RecordReader, error) { call++; return nil, io.EOF },
	}
	assert.NoError(t, ir.Close())
	assert.Equal(t, 0, call)
}

// TestIterativeRecordReader_SerialPathUnchanged pins that a reader that does not
// opt in behaves exactly as before: one chunk at a time, iterate() called lazily
// from Next, nothing opened ahead.
func TestIterativeRecordReader_SerialPathUnchanged(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	chunks := []*ownedChunkReader{chunkOf(alloc, 0, 1), chunkOf(alloc, 1, 1)}
	call := 0
	ir := &IterativeRecordReader{
		iterate: func() (RecordReader, error) {
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	rec, err := ir.Next()
	require.NoError(t, err)
	assert.Equal(t, int64(0), int64Of(rec))
	assert.Equal(t, 1, call, "serial path must not open the next chunk ahead of time")

	got, err := drain(ir)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []int64{100}, got)
	require.NoError(t, ir.Close())
	alloc.AssertSize(t, 0)
}

func TestNewIterativePackedRecordReader_PrefetchFlag(t *testing.T) {
	on := newIterativePackedRecordReader(nil, nil, 0, nil, nil, packed.ExternalReaderContext{}, true)
	off := newIterativePackedRecordReader(nil, nil, 0, nil, nil, packed.ExternalReaderContext{}, false)
	assert.True(t, on.prefetch)
	assert.False(t, off.prefetch)
	// With no paths at all both must report EOF on the first read and close cleanly.
	for _, ir := range []*IterativeRecordReader{on, off} {
		_, err := ir.Next()
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, ir.Close())
	}
}

func TestIterativeRecordReader_PrefetchCloseReportsCurrentCloseError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	closeErr := errors.New("current close failed")
	only := chunkOf(alloc, 0, 1)
	only.closeErr = closeErr
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= 1 {
				return nil, io.EOF
			}
			call++
			return only, nil
		},
	}

	_, err := ir.Next() // chunk 0 live; the prefetch of "next" learns there is none
	require.NoError(t, err)
	assert.ErrorIs(t, ir.Close(), closeErr, "an error closing the live chunk must be returned")
	alloc.AssertSize(t, 0)
}

// TestIterativeRecordReader_PrefetchSurfacesCloseErrorOnChunkBoundary drains a
// chunk whose Close fails: the failure must come out of Next() at the boundary
// instead of being swallowed while moving on to the next chunk.
func TestIterativeRecordReader_PrefetchSurfacesCloseErrorOnChunkBoundary(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	closeErr := errors.New("close failed at boundary")
	chunks := []*ownedChunkReader{chunkOf(alloc, 0, 1), chunkOf(alloc, 1, 1)}
	chunks[0].closeErr = closeErr
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[call]
			call++
			return c, nil
		},
	}

	_, err := ir.Next()
	require.NoError(t, err)
	_, err = ir.Next() // chunk 0 is exhausted here; its Close fails
	assert.ErrorIs(t, err, closeErr)
	// Both chunks were opened (chunk 1 by the prefetch); the wrapper must still
	// close what it holds, and the test releases what it never got to hand out.
	require.NoError(t, ir.Close())
	assert.True(t, chunks[1].closed)
	alloc.AssertSize(t, 0)
}

func TestWithReadPrefetch(t *testing.T) {
	opts := DefaultReaderOptions()
	assert.False(t, opts.readPrefetch, "prefetch is off unless a reader opts in")
	WithReadPrefetch(true)(opts)
	assert.True(t, opts.readPrefetch)
	WithReadPrefetch(false)(opts)
	assert.False(t, opts.readPrefetch)
}

// panicOnNextReader panics on its first Next; used to check that a panic in the
// live reader is turned into an error instead of escaping from Next().
type panicOnNextReader struct {
	closed bool
}

func (r *panicOnNextReader) Next() (Record, error) { panic("boom from live reader") }
func (r *panicOnNextReader) Close() error          { r.closed = true; return nil }

func TestIterativeRecordReader_PrefetchRecoversLiveNextPanic(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	live := &panicOnNextReader{}
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= 1 {
				return nil, io.EOF
			}
			call++
			return live, nil
		},
	}

	// The first record is read by the prefetch goroutine and recovered there.
	_, err := ir.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom from live reader")
	// After the failed open nothing is left to close, but Close must be safe.
	require.NoError(t, ir.Close())
	assert.True(t, live.closed, "a reader whose first Next panicked must still be closed")
	alloc.AssertSize(t, 0)
}

// TestIterativeRecordReader_PrefetchRecoversPanicFromCurrent makes the *live*
// reader panic on its second Next, i.e. inside Next() itself rather than in the
// prefetch goroutine, so the recover in Next() is what has to catch it.
func TestIterativeRecordReader_PrefetchRecoversPanicFromCurrent(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	first := chunkOf(alloc, 0, 1)
	live := &panicAfterFirstReader{inner: first}
	call := 0
	ir := &IterativeRecordReader{
		prefetch: true,
		iterate: func() (RecordReader, error) {
			if call >= 1 {
				return nil, io.EOF
			}
			call++
			return live, nil
		},
	}

	rec, err := ir.Next() // first record came from the prefetch, fine
	require.NoError(t, err)
	rec.Release()
	_, err = ir.Next() // second Next panics inside Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom on second next")
	require.NoError(t, ir.Close())
	assert.True(t, first.closed)
	alloc.AssertSize(t, 0)
}

// panicAfterFirstReader forwards the first Next and panics on the next one.
type panicAfterFirstReader struct {
	inner *ownedChunkReader
	calls int
}

func (r *panicAfterFirstReader) Next() (Record, error) {
	r.calls++
	if r.calls > 1 {
		panic("boom on second next")
	}
	return r.inner.Next()
}
func (r *panicAfterFirstReader) Close() error { return r.inner.Close() }
