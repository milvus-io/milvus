package broadcaster

import (
	"slices"
	"sync"
)

// maxAppendedCopyBroadcasts bounds the broadcasts appendedCopies remembers.
// A record is dropped with its task; the bound covers reports of broadcasts
// whose task never exists or was already collected. Losing a record only
// makes a wait end later, at the acknowledgement.
const maxAppendedCopyBroadcasts = 1024

// appendedCopies remembers, per broadcast and vchannel, the time tick at which
// a copy was reported appended to the WAL of this cluster. The zero value is
// ready to use.
type appendedCopies struct {
	mu    sync.Mutex
	ticks map[uint64]map[string]uint64
	order []uint64 // broadcast ids in the order of their first report
}

// Record keeps the smallest time tick reported for the copy.
func (a *appendedCopies) Record(broadcastID uint64, vchannel string, timeTick uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.ticks == nil {
		a.ticks = make(map[uint64]map[string]uint64)
	}
	copies, ok := a.ticks[broadcastID]
	if !ok {
		for len(a.order) >= maxAppendedCopyBroadcasts {
			delete(a.ticks, a.order[0])
			a.order = a.order[1:]
		}
		copies = make(map[string]uint64)
		a.ticks[broadcastID] = copies
		a.order = append(a.order, broadcastID)
	}
	if old, ok := copies[vchannel]; !ok || timeTick < old {
		copies[vchannel] = timeTick
	}
}

// TimeTickOf returns the greatest reported time tick of the vchannels, and
// false if one of them was not reported.
func (a *appendedCopies) TimeTickOf(broadcastID uint64, vchannels []string) (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	copies := a.ticks[broadcastID]
	if len(copies) == 0 || len(vchannels) == 0 {
		return 0, false
	}
	var timeTick uint64
	for _, vchannel := range vchannels {
		tt, ok := copies[vchannel]
		if !ok {
			return 0, false
		}
		timeTick = max(timeTick, tt)
	}
	return timeTick, true
}

// Forget drops the records of the broadcasts.
func (a *appendedCopies) Forget(broadcastIDs ...uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.ticks) == 0 {
		return
	}
	for _, id := range broadcastIDs {
		if _, ok := a.ticks[id]; !ok {
			continue
		}
		delete(a.ticks, id)
		if idx := slices.Index(a.order, id); idx >= 0 {
			a.order = slices.Delete(a.order, idx, idx+1)
		}
	}
}
