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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

// exhaustedChunkReader is a minimal RecordReader that is already at EOF and
// records whether it was closed. It stands in for a binlog chunk that opened
// successfully and has been fully consumed.
type exhaustedChunkReader struct {
	closed bool
}

func (r *exhaustedChunkReader) Next() (Record, error) { return nil, io.EOF }

func (r *exhaustedChunkReader) Close() error {
	r.closed = true
	return nil
}

// TestIterativeRecordReader_MissingChunkDoesNotPanic reproduces #50927: when a
// later binlog chunk's object is missing in object storage, newPackedRecordReader
// returns a nil *packedRecordReader together with an error, and iterate() boxes
// that nil pointer into a non-nil RecordReader interface. The reader must surface
// the error from Next() and must NOT panic on the deferred Close() (previously a
// nil-pointer dereference that crashed the DataNode into CrashLoopBackOff).
func TestIterativeRecordReader_MissingChunkDoesNotPanic(t *testing.T) {
	missingErr := errors.New("IOError: Path does not exist")
	chunk := &exhaustedChunkReader{}
	call := 0
	ir := &IterativeRecordReader{
		iterate: func() (RecordReader, error) {
			call++
			if call == 1 {
				// first chunk opens fine (and is already exhausted)
				return chunk, nil
			}
			// second chunk's object is missing: mirror newPackedRecordReader
			// returning a typed-nil *packedRecordReader together with an error.
			var pr *packedRecordReader
			return pr, missingErr
		},
	}

	// Drive it the way storage.Sort does: read until a non-nil error.
	var gotErr error
	for {
		if _, err := ir.Next(); err != nil {
			gotErr = err
			break
		}
	}
	assert.ErrorIs(t, gotErr, missingErr, "the missing-object error must be surfaced, not swallowed/recovered")
	assert.True(t, chunk.closed, "the exhausted first chunk must have been closed")

	// The deferred Close() in sortSegment must not panic on the failed chunk.
	assert.NotPanics(t, func() {
		assert.NoError(t, ir.Close())
	})
}

// TestPackedRecordReader_CloseNilReceiverDoesNotPanic pins the defense-in-depth
// half of the #50927 fix: Close() on a typed-nil reader (the shape produced when
// newPackedRecordReader fails and its nil result is boxed into a RecordReader
// interface) must be a safe no-op, not a nil-pointer dereference.
func TestPackedRecordReader_CloseNilReceiverDoesNotPanic(t *testing.T) {
	var pr *packedRecordReader
	assert.NotPanics(t, func() {
		assert.NoError(t, pr.Close())
	})

	// Boxed into the interface, exactly as the deferred Close in sortSegment sees it.
	var rr RecordReader = pr
	assert.NotPanics(t, func() {
		assert.NoError(t, rr.Close())
	})
}
