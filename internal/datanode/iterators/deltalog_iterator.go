package iterator

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/storage"
)

var _ Iterator = (*DeltalogIterator)(nil)

type DeltalogIterator struct {
	disposeCh    chan struct{}
	disposedOnce sync.Once
	disposed     atomic.Bool

	data  *storage.DeleteData
	label *Label
	pos   int
}

func NewDeltalogIterator(v [][]byte, label *Label) (*DeltalogIterator, error) {
	blobs := make([]*storage.Blob, len(v))
	for i := range blobs {
		blobs[i] = &storage.Blob{Value: v[i]}
	}

	reader := storage.NewDeleteCodec()
	_, _, dData, err := reader.Deserialize(blobs)
	if err != nil {
		return nil, err
	}
	return &DeltalogIterator{
		disposeCh: make(chan struct{}),
		data:      dData,
		label:     label,
	}, nil
}

func (d *DeltalogIterator) HasNext() bool {
	return !d.isDisposed() && d.hasNext()
}

func (d *DeltalogIterator) Next() (*LabeledRowData, error) {
	if d.isDisposed() {
		return nil, ErrDisposed
	}

	if !d.hasNext() {
		return nil, ErrNoMoreRecord
	}

	row := &DeltalogRow{
		Pk:        d.data.Pks[d.pos],
		Timestamp: d.data.Tss[d.pos],
	}
	d.pos++

	return NewLabeledRowData(row, d.label), nil
}

func (d *DeltalogIterator) Dispose() {
	d.disposed.CompareAndSwap(false, true)
	d.disposedOnce.Do(func() {
		close(d.disposeCh)
	})
}

func (d *DeltalogIterator) hasNext() bool {
	return int64(d.pos) < d.data.RowCount
}

func (d *DeltalogIterator) isDisposed() bool {
	return d.disposed.Load()
}

func (d *DeltalogIterator) WaitForDisposed() {
	<-d.disposeCh
}
