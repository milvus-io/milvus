package iterator

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	// ErrNoMoreRecord is the error that the iterator does not have next record.
	ErrNoMoreRecord = errors.New("no more record")
	// ErrDisposed is the error that the iterator is disposed.
	ErrDisposed = errors.New("iterator is disposed")
)

const InvalidID int64 = -1

type Row interface{}

type InsertRow struct {
	ID        int64
	PK        storage.PrimaryKey
	Timestamp typeutil.Timestamp
	Value     map[storage.FieldID]interface{}
}

type DeltalogRow struct {
	Pk        storage.PrimaryKey
	Timestamp typeutil.Timestamp
}

type Label struct {
	segmentID typeutil.UniqueID
}

type LabeledRowData struct {
	label *Label
	data  Row
}

func (l *LabeledRowData) GetSegmentID() typeutil.UniqueID {
	if l.label == nil {
		return InvalidID
	}

	return l.label.segmentID
}

func NewLabeledRowData(data Row, label *Label) *LabeledRowData {
	return &LabeledRowData{
		label: label,
		data:  data,
	}
}

type Iterator interface {
	HasNext() bool
	Next() (*LabeledRowData, error)
	Dispose()
	WaitForDisposed() // wait until the iterator is disposed
}
