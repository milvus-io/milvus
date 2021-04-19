package querynode

import (
	"sync"
)

type tSafeWatcher struct {
	notifyChan chan bool
}

func newTSafeWatcher() *tSafeWatcher {
	return &tSafeWatcher{
		notifyChan: make(chan bool, 1),
	}
}

func (watcher *tSafeWatcher) notify() {
	if len(watcher.notifyChan) == 0 {
		watcher.notifyChan <- true
	}
}

func (watcher *tSafeWatcher) hasUpdate() {
	<-watcher.notifyChan
}

type tSafe interface {
	get() Timestamp
	set(t Timestamp)
	registerTSafeWatcher(t *tSafeWatcher)
}

type tSafeImpl struct {
	tSafeMu     sync.Mutex
	tSafe       Timestamp
	watcherList []*tSafeWatcher
}

func newTSafe() tSafe {
	var t tSafe = &tSafeImpl{
		watcherList: make([]*tSafeWatcher, 0),
	}
	return t
}

func (ts *tSafeImpl) registerTSafeWatcher(t *tSafeWatcher) {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()
	ts.watcherList = append(ts.watcherList, t)
}

func (ts *tSafeImpl) get() Timestamp {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()
	return ts.tSafe
}

func (ts *tSafeImpl) set(t Timestamp) {
	ts.tSafeMu.Lock()
	defer ts.tSafeMu.Unlock()

	ts.tSafe = t
	for _, watcher := range ts.watcherList {
		watcher.notify()
	}
}
