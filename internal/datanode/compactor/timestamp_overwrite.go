package compactor

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
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
type timestampOverwriteReader struct {
	inner    storage.RecordReader
	commitTs uint64
}

var _ storage.RecordReader = (*timestampOverwriteReader)(nil)

func (r *timestampOverwriteReader) Next() (storage.Record, error) {
	rec, err := r.inner.Next()
	if err != nil {
		return nil, err
	}
	return overwriteRecordTimestamps(rec, r.commitTs), nil
}

func (r *timestampOverwriteReader) Close() error {
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
