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
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// mockRecord implements storage.Record with a simple timestamp column and an
// explicit refcount so tests can assert Retain/Release balance.
type mockRecord struct {
	tsArray  *array.Int64
	n        int
	refCount int
}

func newMockRecord(timestamps []int64) *mockRecord {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	for _, ts := range timestamps {
		builder.Append(ts)
	}
	arr := builder.NewInt64Array()
	builder.Release()
	return &mockRecord{tsArray: arr, n: len(timestamps), refCount: 1}
}

func (r *mockRecord) Column(i storage.FieldID) arrow.Array {
	if i == common.TimeStampField {
		return r.tsArray
	}
	return nil
}
func (r *mockRecord) Len() int { return r.n }
func (r *mockRecord) Release() {
	r.refCount--
	if r.refCount == 0 {
		r.tsArray.Release()
	}
}

func (r *mockRecord) Retain() {
	r.refCount++
	r.tsArray.Retain()
}

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

// TestOverwriteRecordTimestamps_RetainReleaseBalance verifies that the wrapper
// holds its own reference on the inner record: after wrap + wrapper.Release(),
// the inner's refcount returns to its pre-wrap value so the caller can
// independently release the input exactly once.
func TestOverwriteRecordTimestamps_RetainReleaseBalance(t *testing.T) {
	rec := newMockRecord([]int64{100, 200, 300})
	require.Equal(t, 1, rec.refCount)

	out := overwriteRecordTimestamps(rec, 5000)
	// Wrapper must Retain the inner so caller still owns one reference.
	assert.Equal(t, 2, rec.refCount)

	out.Release()
	// Wrapper Release drops exactly one reference on the inner.
	assert.Equal(t, 1, rec.refCount)

	// Caller's own Release is still needed to fully free the input.
	rec.Release()
	assert.Equal(t, 0, rec.refCount)
}

// TestOverwriteRecordTimestamps_ZeroNoRetain verifies that when commitTs == 0
// no wrapper is created and no Retain/Release is implicitly issued.
func TestOverwriteRecordTimestamps_ZeroNoRetain(t *testing.T) {
	rec := newMockRecord([]int64{100})
	require.Equal(t, 1, rec.refCount)

	out := overwriteRecordTimestamps(rec, 0)
	assert.Equal(t, rec, out)
	assert.Equal(t, 1, rec.refCount)

	rec.Release()
	assert.Equal(t, 0, rec.refCount)
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

	tsCol := out.Column(common.TimeStampField).(*array.Int64)
	for i := 0; i < 3; i++ {
		assert.Equal(t, int64(5000), tsCol.Value(i))
	}

	// Subsequent Next() returns EOF and triggers Release of the previous
	// wrapper held by the reader; closing flushes any tail wrapper. Do not
	// Release `out` ourselves — the reader owns the wrapper's lifecycle.
	_, err = wrapped.Next()
	assert.Equal(t, io.EOF, err)
	require.NoError(t, wrapped.Close())
}

// TestOverwriteReader_DrainsAndReleases verifies the reader-owned contract:
// downstream callers (storage.MergeSort, storage.Sort) do not release records
// returned from Next(), so timestampOverwriteReader must release each wrapper
// it creates on the next Next() advance and on Close(). Without this, every
// batch leaks one int64 tsArray plus a Retain'd reference on the inner record.
func TestOverwriteReader_DrainsAndReleases(t *testing.T) {
	recs := []*mockRecord{
		newMockRecord([]int64{100, 200}),
		newMockRecord([]int64{300}),
		newMockRecord([]int64{400, 500, 600}),
	}
	innerRecs := make([]storage.Record, len(recs))
	for i, r := range recs {
		innerRecs[i] = r
	}
	inner := &mockReader{records: innerRecs}
	wrapped := wrapReaderWithTimestampOverwrite(inner, 5000)

	// Drain to EOF without releasing returned records — mimics MergeSort/Sort.
	for {
		_, err := wrapped.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, wrapped.Close())

	// After drain + Close, every wrapper must have released its extra ref on
	// the inner record. Each mockRecord starts at refCount == 1, the wrapper
	// bumps to 2 during Next, the next Next/Close drops back to 1.
	for i, r := range recs {
		assert.Equal(t, 1, r.refCount, "record %d leaked a reference", i)
	}
}

// TestOverwriteReader_CloseEarly verifies that calling Close() without draining
// to EOF still releases the last wrapper returned by Next().
func TestOverwriteReader_CloseEarly(t *testing.T) {
	rec := newMockRecord([]int64{100, 200})
	inner := &mockReader{records: []storage.Record{rec}}
	wrapped := wrapReaderWithTimestampOverwrite(inner, 5000)

	_, err := wrapped.Next()
	require.NoError(t, err)
	// Wrapper Retain'd the inner: refCount == 2.
	require.Equal(t, 2, rec.refCount)

	require.NoError(t, wrapped.Close())
	// Close must release the in-flight wrapper.
	assert.Equal(t, 1, rec.refCount)
}

// TestOverwriteReader_CommitTsZero verifies that when commitTs == 0 the reader
// is bypassed entirely (no wrapper is created and refcounts on the underlying
// records are untouched by our layer). Regression guard against accidentally
// tracking and releasing inner-reader-owned records.
func TestOverwriteReader_CommitTsZero(t *testing.T) {
	rec := newMockRecord([]int64{100})
	inner := &mockReader{records: []storage.Record{rec}}

	wrapped := wrapReaderWithTimestampOverwrite(inner, 0)
	assert.Equal(t, inner, wrapped, "commitTs=0 should bypass the wrapper")

	out, err := wrapped.Next()
	require.NoError(t, err)
	assert.Equal(t, rec, out)
	assert.Equal(t, 1, rec.refCount, "no implicit Retain/Release when commitTs=0")

	_, err = wrapped.Next()
	assert.Equal(t, io.EOF, err)
	require.NoError(t, wrapped.Close())
	assert.Equal(t, 1, rec.refCount)
}
