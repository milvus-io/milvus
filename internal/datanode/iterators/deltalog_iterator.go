package iterator

import (
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

var _ Iterator = (*DeltalogIterator)(nil)

type DeltalogIterator struct {
	disposeCh    chan struct{}
	disposedOnce sync.Once
	disposed     atomic.Bool

	data  *storage.DeltaData
	blobs []*storage.Blob
	label *Label
	pos   int
}

func NewDeltalogIterator(v [][]byte, label *Label) *DeltalogIterator {
	blobs := make([]*storage.Blob, len(v))
	for i := range blobs {
		blobs[i] = &storage.Blob{Value: v[i]}
	}

	return &DeltalogIterator{
		disposeCh: make(chan struct{}),
		blobs:     blobs,
		label:     label,
	}
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
		Pk:        d.data.DeletePks().Get(d.pos),
		Timestamp: d.data.DeleteTimestamps()[d.pos],
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
	if d.data == nil && d.blobs != nil {
		reader := storage.NewDeleteCodec()
		_, _, dData, err := reader.Deserialize(d.blobs)
		if err != nil {
			log.Warn("Deltalog iterator failed to deserialize blobs", zap.Error(err))
			return false
		}
		d.data = dData
		d.blobs = nil
	}
	return int64(d.pos) < d.data.DeleteRowCount()
}

func (d *DeltalogIterator) isDisposed() bool {
	return d.disposed.Load()
}

func (d *DeltalogIterator) WaitForDisposed() {
	<-d.disposeCh
}
