package compactor

import (
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

// mockRecord implements storage.Record with a simple timestamp column.
type mockRecord struct {
	tsArray *array.Int64
	n       int
}

func newMockRecord(timestamps []int64) *mockRecord {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	for _, ts := range timestamps {
		builder.Append(ts)
	}
	arr := builder.NewInt64Array()
	builder.Release()
	return &mockRecord{tsArray: arr, n: len(timestamps)}
}

func (r *mockRecord) Column(i storage.FieldID) arrow.Array {
	if i == common.TimeStampField {
		return r.tsArray
	}
	return nil
}
func (r *mockRecord) Len() int { return r.n }
func (r *mockRecord) Release() { r.tsArray.Release() }
func (r *mockRecord) Retain()  { r.tsArray.Retain() }

func TestOverwriteRecordTimestamps_Zero(t *testing.T) {
	rec := newMockRecord([]int64{100, 200, 300})
	defer rec.Release()

	out := overwriteRecordTimestamps(rec, 0)
	// commitTs=0 should return original record unchanged
	assert.Equal(t, rec, out)
}

func TestOverwriteRecordTimestamps_NonZero(t *testing.T) {
	rec := newMockRecord([]int64{100, 200, 300})
	defer rec.Release()

	out := overwriteRecordTimestamps(rec, 5000)
	defer out.Release()

	assert.NotEqual(t, rec, out)
	assert.Equal(t, 3, out.Len())

	tsCol := out.Column(common.TimeStampField).(*array.Int64)
	for i := 0; i < 3; i++ {
		assert.Equal(t, int64(5000), tsCol.Value(i))
	}
}

func TestOverwriteRecordTimestamps_OtherColumnsUnchanged(t *testing.T) {
	rec := newMockRecord([]int64{100, 200})
	defer rec.Release()

	out := overwriteRecordTimestamps(rec, 5000)
	defer out.Release()

	// Non-timestamp column should return same as inner
	assert.Nil(t, out.Column(999))
}

// mockReader is a simple RecordReader that returns records in sequence.
type mockReader struct {
	records []storage.Record
	idx     int
}

func (r *mockReader) Next() (storage.Record, error) {
	if r.idx >= len(r.records) {
		return nil, io.EOF
	}
	rec := r.records[r.idx]
	r.idx++
	return rec, nil
}
func (r *mockReader) Close() error { return nil }

func TestWrapReaderWithTimestampOverwrite_Zero(t *testing.T) {
	inner := &mockReader{}
	wrapped := wrapReaderWithTimestampOverwrite(inner, 0)
	// commitTs=0 should return original reader
	assert.Equal(t, inner, wrapped)
}

func TestWrapReaderWithTimestampOverwrite_NonZero(t *testing.T) {
	rec := newMockRecord([]int64{100, 200, 300})
	inner := &mockReader{records: []storage.Record{rec}}

	wrapped := wrapReaderWithTimestampOverwrite(inner, 5000)
	assert.NotEqual(t, inner, wrapped)

	out, err := wrapped.Next()
	require.NoError(t, err)
	defer out.Release()

	tsCol := out.Column(common.TimeStampField).(*array.Int64)
	for i := 0; i < 3; i++ {
		assert.Equal(t, int64(5000), tsCol.Value(i))
	}

	_, err = wrapped.Next()
	assert.Equal(t, io.EOF, err)
}
