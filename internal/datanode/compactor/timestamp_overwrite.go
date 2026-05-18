package compactor

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// timestampOverwriteRecord wraps a Record and overrides the timestamp column
// so that all rows carry the given commitTs. This is used during compaction
// of import/CDC segments: the output segment's row timestamps are normalized
// to commit_ts, after which commit_ts can be cleared (set to 0).
type timestampOverwriteRecord struct {
	inner   storage.Record
	tsArray arrow.Array
}

var _ storage.Record = (*timestampOverwriteRecord)(nil)

func (r *timestampOverwriteRecord) Column(i storage.FieldID) arrow.Array {
	if i == common.TimeStampField {
		return r.tsArray
	}
	return r.inner.Column(i)
}

func (r *timestampOverwriteRecord) Len() int { return r.inner.Len() }
func (r *timestampOverwriteRecord) Release() { r.tsArray.Release(); r.inner.Release() }
func (r *timestampOverwriteRecord) Retain()  { r.tsArray.Retain(); r.inner.Retain() }

// overwriteRecordTimestamps returns a Record with the timestamp column filled
// with commitTs. If commitTs is 0, the original record is returned unchanged.
//
// When wrapping (commitTs != 0) the wrapper holds its own reference on the
// inner record via Retain, so the caller can continue to manage the input
// record independently. The wrapper's Release balances this Retain.
func overwriteRecordTimestamps(rec storage.Record, commitTs uint64) storage.Record {
	if commitTs == 0 {
		return rec
	}
	n := rec.Len()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	ts := int64(commitTs)
	for i := 0; i < n; i++ {
		builder.Append(ts)
	}
	tsArray := builder.NewArray()
	builder.Release()
	rec.Retain()
	return &timestampOverwriteRecord{inner: rec, tsArray: tsArray}
}

// timestampOverwriteReader wraps a RecordReader and overwrites the timestamp
// column of each record with commitTs. Used in merge-sort and sort compaction
// paths where we cannot intercept individual records before writing.
//
// Downstream consumers (storage.MergeSort, storage.Sort) treat records returned
// from Next() as reader-owned and do not call Release on them. Each wrapper
// produced by overwriteRecordTimestamps holds a Retain'd reference on the inner
// record plus an allocated tsArray, so the reader must Release the previous
// wrapper on every Next() advance and again on Close() — otherwise each batch
// leaks one int64 array and pins the inner record's Arrow buffers.
type timestampOverwriteReader struct {
	inner    storage.RecordReader
	commitTs uint64
	last     storage.Record // wrapper returned by previous Next(); nil when not wrapped
}

var _ storage.RecordReader = (*timestampOverwriteReader)(nil)

func (r *timestampOverwriteReader) Next() (storage.Record, error) {
	if r.last != nil {
		r.last.Release()
		r.last = nil
	}
	rec, err := r.inner.Next()
	if err != nil {
		return nil, err
	}
	out := overwriteRecordTimestamps(rec, r.commitTs)
	if out != rec {
		// Only track wrappers we actually created. When commitTs == 0 the
		// return value aliases the inner reader's record, whose lifecycle is
		// owned by the inner reader — touching it here would double-release.
		r.last = out
	}
	return out, nil
}

func (r *timestampOverwriteReader) Close() error {
	if r.last != nil {
		r.last.Release()
		r.last = nil
	}
	return r.inner.Close()
}

// wrapReaderWithTimestampOverwrite wraps a RecordReader to overwrite timestamps
// if commitTs is non-zero. Returns the original reader if commitTs is 0.
func wrapReaderWithTimestampOverwrite(reader storage.RecordReader, commitTs uint64) storage.RecordReader {
	if commitTs == 0 {
		return reader
	}
	return &timestampOverwriteReader{inner: reader, commitTs: commitTs}
}
