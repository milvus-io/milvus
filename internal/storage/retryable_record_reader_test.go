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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeRecord struct {
	rows int
}

func (r fakeRecord) Column(FieldID) arrow.Array { return nil }
func (r fakeRecord) Len() int                   { return r.rows }
func (r fakeRecord) Release()                   {}
func (r fakeRecord) Retain()                    {}

type scriptedRecordReader struct {
	rows   []int
	failAt int
	next   int
}

func (r *scriptedRecordReader) Next() (Record, error) {
	if r.failAt >= 0 && r.next == r.failAt {
		r.next++
		return nil, merr.Wrapf(packed.ErrLoonTransient, "scripted transient read")
	}
	if r.next >= len(r.rows) {
		return nil, io.EOF
	}
	rows := r.rows[r.next]
	r.next++
	return fakeRecord{rows: rows}, nil
}

func (r *scriptedRecordReader) Close() error { return nil }

func TestRetryableRecordReaderReopensAndSkipsConsumedRows(t *testing.T) {
	rows := []int{2, 3, 4}
	opens := 0
	open := func(context.Context) (RecordReader, error) {
		opens++
		if opens == 1 {
			// Return the first record, then fail before the second.
			return &scriptedRecordReader{rows: rows, failAt: 1}, nil
		}
		return &scriptedRecordReader{rows: rows, failAt: -1}, nil
	}

	reader, err := NewRetryableRecordReader(context.Background(), open, 3)
	require.NoError(t, err)
	defer reader.Close()

	var got []int
	for {
		rec, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, rec.Len())
	}
	require.Equal(t, rows, got)
	require.Equal(t, 2, opens, "the reader must be reopened exactly once")
}

func TestRetryableRecordReaderRetriesInitialOpen(t *testing.T) {
	opens := 0
	open := func(context.Context) (RecordReader, error) {
		opens++
		if opens == 1 {
			return nil, merr.Wrapf(packed.ErrLoonTransient, "scripted transient open")
		}
		return &scriptedRecordReader{rows: []int{7}, failAt: -1}, nil
	}

	reader, err := NewRetryableRecordReader(context.Background(), open, 3)
	require.NoError(t, err)
	defer reader.Close()
	require.Equal(t, 2, opens, "the initial open must use the bounded budget")

	rec, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, 7, rec.Len())
}

func TestRetryableRecordReaderDoesNotRetryPermanentError(t *testing.T) {
	opens := 0
	permanent := &permanentRecordReader{}
	open := func(context.Context) (RecordReader, error) {
		opens++
		return permanent, nil
	}

	reader, err := NewRetryableRecordReader(context.Background(), open, 3)
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.Next()
	require.ErrorIs(t, err, packed.ErrLoonPermanent)
	require.Equal(t, 1, opens, "permanent errors must not reopen the source")
}

func TestRetryableRecordReaderStopsAfterAttempts(t *testing.T) {
	opens := 0
	open := func(context.Context) (RecordReader, error) {
		opens++
		return &scriptedRecordReader{rows: []int{1}, failAt: 0}, nil
	}

	reader, err := NewRetryableRecordReader(context.Background(), open, 2)
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.Next()
	require.ErrorIs(t, err, packed.ErrLoonTransient)
	require.Equal(t, 2, opens, "the retry budget is two attempts")
}

func TestRetryableRecordReaderReturnsEOF(t *testing.T) {
	open := func(context.Context) (RecordReader, error) {
		return &scriptedRecordReader{rows: nil, failAt: -1}, nil
	}

	reader, err := NewRetryableRecordReader(context.Background(), open, 3)
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.Next()
	require.ErrorIs(t, err, io.EOF)
}

type permanentRecordReader struct{}

func (r *permanentRecordReader) Next() (Record, error) {
	return nil, merr.Wrapf(packed.ErrLoonPermanent, "scripted permanent read")
}
func (r *permanentRecordReader) Close() error { return nil }
