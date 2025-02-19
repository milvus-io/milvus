package iterator

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	// ErrNoMoreRecord is the error that the iterator does not have next record.
	ErrNoMoreRecord = errors.New("no more record")
	// ErrDisposed is the error that the iterator is disposed.
	ErrDisposed = errors.New("iterator is disposed")
)

const InvalidID int64 = -1

type Row interface {
	GetPk() storage.PrimaryKey
	GetTimestamp() uint64
}

type InsertRow struct {
	ID        int64
	Pk        storage.PrimaryKey
	Timestamp typeutil.Timestamp
	Value     map[storage.FieldID]interface{}
}

func (r *InsertRow) GetPk() storage.PrimaryKey {
	return r.Pk
}

func (r *InsertRow) GetTimestamp() uint64 {
	return r.Timestamp
}

type DeltalogRow struct {
	Pk        storage.PrimaryKey
	Timestamp typeutil.Timestamp
}

func (r *DeltalogRow) GetPk() storage.PrimaryKey {
	return r.Pk
}

func (r *DeltalogRow) GetTimestamp() uint64 {
	return r.Timestamp
}

type Label struct {
	segmentID typeutil.UniqueID
}

type LabeledRowData struct {
	label *Label
	data  Row
}

func (l *LabeledRowData) GetLabel() *Label {
	return l.label
}

func (l *LabeledRowData) GetPk() storage.PrimaryKey {
	return l.data.GetPk()
}

func (l *LabeledRowData) GetTimestamp() uint64 {
	return l.data.GetTimestamp()
}

func (l *LabeledRowData) GetSegmentID() typeutil.UniqueID {
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
