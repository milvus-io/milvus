package iterator

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"go.uber.org/atomic"
)

// TODO
type Filter func(label *LabeledRowData) bool

// FilterIterator deals with insert binlogs and delta binlogs
// While iterating through insert data and delta data, FilterIterator
// filters the data by various Filters, for example, ExpireFilter, TimetravelFilter, and
// most importantly DeletionFilter.
//
// DeletionFilter is constructed from the delta data, thus FilterIterator needs to go through
// ALL delta data from a segment before processing insert data. See hasNext() for details.
type FilterIterator struct {
	disposed atomic.Bool

	addOnce sync.Once
	filters []Filter

	iIter *DynamicIterator[*BinlogIterator]
	dIter *DynamicIterator[*DeltalogIterator]

	deleted   map[interface{}]typeutil.Timestamp
	nextCache *LabeledRowData
}

var _ InterDynamicIterator = (*FilterIterator)(nil)

func NewFilterIterator(travelTs, now typeutil.Timestamp, ttl int64) *FilterIterator {
	i := FilterIterator{
		iIter:   NewDynamicBinlogIterator(),
		dIter:   NewDynamicDeltalogIterator(),
		deleted: make(map[interface{}]typeutil.Timestamp),
	}

	i.filters = []Filter{
		// ExpireFilter(ttl, now),
	}
	return &i
}

func (f *FilterIterator) HasNext() bool {
	return !f.isDisposed() && f.hasNext()
}

func (f *FilterIterator) Next() (*LabeledRowData, error) {
	if f.isDisposed() {
		return nil, ErrDisposed
	}

	if !f.hasNext() {
		return nil, ErrNoMoreRecord
	}

	tmp := f.nextCache
	f.nextCache = nil
	return tmp, nil
}

func (f *FilterIterator) isDisposed() bool {
	return f.disposed.Load()
}

func (f *FilterIterator) hasNext() bool {
	if f.nextCache != nil {
		return true
	}

	var (
		filtered bool = true
		labeled  *LabeledRowData
	)

	// Get Next until the row isn't filtered
	for f.dIter.HasNext() && filtered {
		labeled, _ = f.dIter.Next()
		for _, filter := range f.filters {
			filtered = filter(labeled)
		}

		// Add delete data for physical deletion
		data, _ := labeled.data.(*DeltalogRow)
		if filtered {
			f.deleted[data.Pk.GetValue()] = data.Timestamp
		}
	}

	// Add a new deletion filter for insertLogs
	// when iterate deltaLogs done for the first time.
	if !f.dIter.HasNext() && filtered {
		f.addDeletionFilterOnce()
	}

	// Get Next until the row isn't filtered or no data
	for f.iIter.HasNext() && filtered {
		labeled, _ = f.iIter.Next()
		for _, filter := range f.filters {
			filtered = filter(labeled)
		}
	}

	if filtered {
		return false
	}

	f.nextCache = labeled
	return true
}

func (f *FilterIterator) addDeletionFilterOnce() {
	f.addOnce.Do(func() {
		// TODO
		// f.filters = append(f.filters, DeletionFilter(f.deleted))
	})
}

func (f *FilterIterator) Dispose() {
	f.iIter.Dispose()
	f.dIter.Dispose()
	f.disposed.CompareAndSwap(false, true)
}

// WaitForDisposed does nothing, do not use
func (f *FilterIterator) WaitForDisposed() <-chan struct{} {
	notifyCh := make(chan struct{})
	close(notifyCh)
	return notifyCh
}

func (f *FilterIterator) Add(iter Iterator) {
	switch iter.(type) {
	case *BinlogIterator:
		f.iIter.Add(iter)
	case *DeltalogIterator:
		f.dIter.Add(iter)
		// TODO default, unexpected behaviour, should not happen
	}
}

func (f *FilterIterator) ReadOnly() bool {
	return f.iIter.ReadOnly() && f.dIter.ReadOnly()
}

func (f *FilterIterator) SetReadOnly() {
	if !f.dIter.ReadOnly() {
		f.dIter.SetReadOnly()
	} else {
		f.iIter.SetReadOnly()
	}
}
