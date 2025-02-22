package iterator

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BinlogIterator struct {
	disposed     atomic.Bool
	disposedCh   chan struct{}
	disposedOnce sync.Once

	data      *storage.InsertData
	label     *Label
	pkFieldID int64
	pkType    schemapb.DataType
	pos       int
}

var _ Iterator = (*BinlogIterator)(nil)

// NewInsertBinlogIterator creates a new iterator
func NewInsertBinlogIterator(v [][]byte, pkFieldID typeutil.UniqueID, pkType schemapb.DataType, label *Label) (*BinlogIterator, error) {
	blobs := make([]*storage.Blob, len(v))
	for i := range blobs {
		blobs[i] = &storage.Blob{Value: v[i]}
	}

	reader := storage.NewInsertCodec()
	_, _, iData, err := reader.Deserialize(blobs)
	if err != nil {
		return nil, err
	}

	return &BinlogIterator{
		disposedCh: make(chan struct{}),
		data:       iData,
		pkFieldID:  pkFieldID,
		pkType:     pkType,
		label:      label,
	}, nil
}

// HasNext returns true if the iterator have unread record
func (i *BinlogIterator) HasNext() bool {
	return !i.isDisposed() && i.hasNext()
}

func (i *BinlogIterator) Next() (*LabeledRowData, error) {
	if i.isDisposed() {
		return nil, ErrDisposed
	}

	if !i.hasNext() {
		return nil, ErrNoMoreRecord
	}

	fields := make(map[int64]interface{})
	for fieldID, fieldData := range i.data.Data {
		fields[fieldID] = fieldData.GetRow(i.pos)
	}

	pk, err := storage.GenPrimaryKeyByRawData(i.data.Data[i.pkFieldID].GetRow(i.pos), i.pkType)
	if err != nil {
		return nil, err
	}

	row := &InsertRow{
		ID:        i.data.Data[common.RowIDField].GetRow(i.pos).(int64),
		Timestamp: uint64(i.data.Data[common.TimeStampField].GetRow(i.pos).(int64)),
		Pk:        pk,
		Value:     fields,
	}
	i.pos++
	return NewLabeledRowData(row, i.label), nil
}

// Dispose disposes the iterator
func (i *BinlogIterator) Dispose() {
	i.disposed.CompareAndSwap(false, true)
	i.disposedOnce.Do(func() {
		close(i.disposedCh)
	})
}

func (i *BinlogIterator) hasNext() bool {
	return i.pos < i.data.GetRowNum()
}

func (i *BinlogIterator) isDisposed() bool {
	return i.disposed.Load()
}

// Disposed wait forever for the iterator to dispose
func (i *BinlogIterator) WaitForDisposed() {
	<-i.disposedCh
}
