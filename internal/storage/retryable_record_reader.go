// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// DefaultRetryableRecordReaderAttempts bounds in-place reopen+skip retries.
// It is deliberately small: after this budget the caller should retry the
// enclosing unit of work (for example one fragment write) or fail the task.
const DefaultRetryableRecordReaderAttempts uint = 3

// ReopenRecordReader opens a fresh reader positioned at the beginning of the
// same deterministic source. It is called after a transient read failure.
type ReopenRecordReader func(context.Context) (RecordReader, error)

// RetryableRecordReader retries transient (packed.ErrLoonTransient) Next
// failures by reopening the source and skipping the rows already returned to
// the caller. Next is atomic: a failed call never hands back a partial record,
// so skipping rowsRead rows replays exactly the consumed prefix.
//
// Permanent loon failures, data-integrity errors and unknown errors stop
// immediately (retry.Unrecoverable). A transient reopen/skip failure consumes
// the same bounded budget.
type RetryableRecordReader struct {
	ctx      context.Context
	open     ReopenRecordReader
	reader   RecordReader
	rowsRead int64
	attempts uint
	closed   bool
}

func NewRetryableRecordReader(ctx context.Context, open ReopenRecordReader, attempts uint) (*RetryableRecordReader, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("retryable record reader context is nil")
	}
	if open == nil {
		return nil, merr.WrapErrServiceInternalMsg("retryable record reader open func is nil")
	}
	if attempts == 0 {
		attempts = DefaultRetryableRecordReaderAttempts
	}
	r := &RetryableRecordReader{
		ctx:      ctx,
		open:     open,
		attempts: attempts,
	}
	if err := retry.Do(ctx, func() error {
		if err := r.reopenAndSkip(); err != nil {
			return classifyRetryableReadErr(err)
		}
		return nil
	}, retry.Attempts(attempts)); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RetryableRecordReader) Next() (Record, error) {
	if r.closed {
		return nil, io.EOF
	}

	var (
		rec Record
		eof bool
	)
	err := retry.Do(r.ctx, func() error {
		if r.reader == nil {
			if err := r.reopenAndSkip(); err != nil {
				return classifyRetryableReadErr(err)
			}
		}

		var err error
		rec, err = r.reader.Next()
		if err == nil {
			if rec == nil {
				eof = true
				return nil
			}
			r.rowsRead += int64(rec.Len())
			return nil
		}
		if errors.Is(err, io.EOF) {
			// EOF is a normal outcome, not a retry failure: report it after the
			// retry loop so retry.Do does not log a warning on every reader.
			eof = true
			return nil
		}
		if !errors.Is(err, packed.ErrLoonTransient) {
			return retry.Unrecoverable(err)
		}

		// The current reader's position is unknown after a transient failure.
		// Reopen from the beginning and skip rowsRead on the next attempt.
		_ = r.reader.Close()
		r.reader = nil
		return err
	}, retry.Attempts(r.attempts))
	if err != nil {
		return nil, err
	}
	if eof {
		return nil, io.EOF
	}
	return rec, nil
}

func (r *RetryableRecordReader) Close() error {
	r.closed = true
	if r.reader != nil {
		err := r.reader.Close()
		r.reader = nil
		return err
	}
	return nil
}

func (r *RetryableRecordReader) reopenAndSkip() error {
	if r.reader != nil {
		_ = r.reader.Close()
		r.reader = nil
	}
	next, err := r.open(r.ctx)
	if err != nil {
		return err
	}
	r.reader = next
	if err := r.skip(r.rowsRead); err != nil {
		_ = next.Close()
		r.reader = nil
		return err
	}
	return nil
}

func (r *RetryableRecordReader) skip(n int64) error {
	var skipped int64
	for skipped < n {
		rec, err := r.reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return merr.WrapErrDataIntegrityMsg(
					"retryable record reader: source ended after %d of %d already-returned rows", skipped, n)
			}
			return err
		}
		if rec == nil {
			return merr.WrapErrDataIntegrityMsg(
				"retryable record reader: source ended after %d of %d already-returned rows", skipped, n)
		}
		skipped += int64(rec.Len())
	}
	if skipped != n {
		return merr.WrapErrDataIntegrityMsg(
			"retryable record reader: row count changed while reopening, expected %d got %d", n, skipped)
	}
	return nil
}

func classifyRetryableReadErr(err error) error {
	if errors.Is(err, packed.ErrLoonTransient) {
		return err
	}
	return retry.Unrecoverable(err)
}
