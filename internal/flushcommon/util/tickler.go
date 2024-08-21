package util

import "go.uber.org/atomic"

// Tickler counts every time when called inc(),
type Tickler struct {
	count     *atomic.Int32
	total     *atomic.Int32
	closedSig *atomic.Bool

	progressSig chan struct{}
}

func (t *Tickler) Inc() {
	t.count.Inc()
	t.progressSig <- struct{}{}
}

func (t *Tickler) SetTotal(total int32) {
	t.total.Store(total)
}

// progress returns the count over total if total is set
// else just return the count number.
func (t *Tickler) Progress() int32 {
	if t.total.Load() == 0 {
		return t.count.Load()
	}
	return t.count.Load() * 100 / t.total.Load()
}

func (t *Tickler) Close() {
	t.closedSig.CompareAndSwap(false, true)
}

func (t *Tickler) IsClosed() bool {
	return t.closedSig.Load()
}

func (t *Tickler) GetProgressSig() chan struct{} {
	return t.progressSig
}

func NewTickler() *Tickler {
	return &Tickler{
		count:       atomic.NewInt32(0),
		total:       atomic.NewInt32(0),
		closedSig:   atomic.NewBool(false),
		progressSig: make(chan struct{}, 200),
	}
}
