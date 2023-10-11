package iterator

import (
	"time"

	"github.com/milvus-io/milvus/pkg/log"

	"go.uber.org/atomic"
)

type InterDynamicIterator interface {
	Iterator
	Add(iter Iterator)
	ReadOnly() bool
	SetReadOnly()
}

// DynamicIterator can add, remove iterators while iterating.
// A ReadOnly DynamicIterator cannot add new iterators. And readers of the iterator
// will wait forever if the iterator is NOT ReadyOnly.
//
// So whoever WRITES iterators into DynamicIterator should set it to
// ReadOnly when there're no iterators to add, so that the readers WON'T WAIT FOREVER.
//
// Why we need DynamicIterator? Now we store lots of binlog files for 1 segment, usually too many
// files that we could easily hit the S3 limit. So we need to downloading files in batch, ending with
// the need for DynamicIterator.
type DynamicIterator[T Iterator] struct {
	backlogs chan T
	active   Iterator

	readonly atomic.Bool
	disposed atomic.Bool
}

var _ InterDynamicIterator = &DynamicIterator[*BinlogIterator]{}
var _ InterDynamicIterator = &DynamicIterator[*DeltalogIterator]{}

func NewDynamicBinlogIterator() *DynamicIterator[*BinlogIterator] {

	iter := DynamicIterator[*BinlogIterator]{
		backlogs: make(chan *BinlogIterator, 100), // TODO configurable
	}
	return &iter
}

func NewDynamicDeltalogIterator() *DynamicIterator[*DeltalogIterator] {
	iter := DynamicIterator[*DeltalogIterator]{
		backlogs: make(chan *DeltalogIterator, 100), // TODO configurable
	}
	return &iter
}

func (d *DynamicIterator[T]) HasNext() bool {
	return !d.isDisposed() && d.hasNext()
}

func (d *DynamicIterator[T]) Next() (*LabeledRowData, error) {
	if d.isDisposed() {
		return nil, ErrDisposed
	}

	if !d.hasNext() {
		return nil, ErrNoMoreRecord
	}

	return d.active.Next()
}

func (d *DynamicIterator[T]) Dispose() {
	if d.isDisposed() {
		return
	}

	d.active.Dispose()

	for b := range d.backlogs {
		b.Dispose()
	}

	d.disposed.CompareAndSwap(false, true)
}

func (d *DynamicIterator[T]) Add(iter Iterator) {
	if d.ReadOnly() {
		log.Warn("Adding iterators to an readonly dynamic iterator")
		return
	}
	d.backlogs <- iter.(T)
}

func (d *DynamicIterator[T]) ReadOnly() bool {
	return d.readonly.Load()
}

func (d *DynamicIterator[T]) SetReadOnly() {
	d.readonly.CompareAndSwap(false, true)
}

// WaitForDisposed does nothing, do not use
func (d *DynamicIterator[T]) WaitForDisposed() <-chan struct{} {
	notifyCh := make(chan struct{})
	close(notifyCh)
	return notifyCh
}

func (d *DynamicIterator[T]) isDisposed() bool {
	return d.disposed.Load()
}

func (d *DynamicIterator[T]) empty() bool {
	// No active iterator or the active iterator doesn't have next
	// and nothing in the backlogs
	return (d.active == nil || !d.active.HasNext()) || len(d.backlogs) > 0
}

// waitForData waits for readonly or not empty
func (d *DynamicIterator[T]) waitForActive() bool {
	if d.active != nil {
		d.active.Dispose()
		d.active = nil
	}

	// d.active == nil here
	ticker := time.NewTicker(time.Second)
	timer := time.NewTimer(1 * time.Hour) // TODO configurable
	for !d.ReadOnly() && len(d.backlogs) == 0 {
		select {
		case <-ticker.C:
		case <-timer.C:
			log.Warn("Timeout after waiting for 1h")
			return false
		}
	}

	if d.ReadOnly() {
		return false
	}

	d.active = <-d.backlogs
	return true
}

func (d *DynamicIterator[T]) hasNext() bool {
	if d.active != nil && d.active.HasNext() {
		return true
	}
	return d.waitForActive()
}
