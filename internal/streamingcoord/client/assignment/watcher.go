package assignment

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrWatcherClosed = errors.New("watcher is closed")

// newWatcher creates a new watcher.
func newWatcher() *watcher {
	return &watcher{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		lastVersionedAssignment: types.VersionedStreamingNodeAssignments{
			Version:     typeutil.VersionInt64Pair{Global: -1, Local: -1},
			Assignments: make(map[int64]types.StreamingNodeAssignment),
		},
	}
}

// watcher is the watcher for assignment discovery.
type watcher struct {
	cond                    *syncutil.ContextCond
	lastVersionedAssignment types.VersionedStreamingNodeAssignments
}

// AssignmentDiscover watches the assignment discovery.
func (w *watcher) AssignmentDiscover(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
	w.cond.L.Lock()
	for {
		if err := cb(&w.lastVersionedAssignment); err != nil {
			w.cond.L.Unlock()
			return err
		}
		if err := w.cond.Wait(ctx); err != nil {
			return err
		}
	}
}

// Update updates the assignment.
func (w *watcher) Update(assignments types.VersionedStreamingNodeAssignments) {
	w.cond.LockAndBroadcast()
	if assignments.Version.GT(w.lastVersionedAssignment.Version) {
		w.lastVersionedAssignment = assignments
	}
	w.cond.L.Unlock()
}
