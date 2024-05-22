package balancer

import (
	"context"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// newNodeStatusWatcher creates a new NodeStatusWatcher.
func newNodeStatusWatcher(layout *layout.Layout) *nodeStatusWatcher {
	globalVersion := paramtable.GetNodeID()
	return &nodeStatusWatcher{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		version: &util.VersionInt64Pair{
			Global: globalVersion, // global version should be keep increasing globally, it's ok to use node id.
			Local:  layout.Version,
		},
		nodes: layout.CloneNodes(),
	}
}

type nodeStatusWatcher struct {
	cond    *syncutil.ContextCond
	version *util.VersionInt64Pair
	nodes   map[int64]*layout.NodeStatus
}

func (w *nodeStatusWatcher) WatchBalanceResult(ctx context.Context, cb func(v *util.VersionInt64Pair, nodeStatus map[int64]*layout.NodeStatus) error) error {
	w.cond.L.Lock()
	node := w.nodes
	version := w.version
	w.cond.L.Unlock()

	// push the first layout to watcher callback.
	if node != nil {
		cb(version, node)
	}

	for {
		// wait for the layout to be updated.
		if err := w.watchLayoutChange(ctx, version); err != nil {
			return err
		}
		w.cond.L.Lock()
		node = w.nodes
		version = w.version
		w.cond.L.Unlock()

		// push the layout to watcher.
		if err := cb(version, node); err != nil {
			return err
		}
	}
}

func (w *nodeStatusWatcher) watchLayoutChange(ctx context.Context, version util.Version) error {
	w.cond.L.Lock()
	for version.Equal(w.version) {
		if err := w.cond.Wait(ctx); err != nil {
			return err
		}
	}
	w.cond.L.Unlock()
	return nil
}

func (w *nodeStatusWatcher) UpdateBalanceResultAndNotify(layout *layout.Layout) {
	nodes := layout.CloneNodes()
	// replace the old layout with the new one.
	w.cond.LockAndBroadcast()
	w.nodes = nodes
	w.version = &util.VersionInt64Pair{
		Global: w.version.Global,
		Local:  layout.Version,
	}

	// update metrics.
	metrics.LogCoordAssignmentInfo.WithLabelValues(
		paramtable.GetStringNodeID(),
		strconv.FormatInt(w.version.Global, 10),
		strconv.FormatInt(w.version.Local, 10),
	).Set(1)
	w.cond.L.Unlock()
}
