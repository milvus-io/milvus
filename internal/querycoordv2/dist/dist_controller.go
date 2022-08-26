package dist

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
)

type Controller struct {
	mu          sync.RWMutex
	handlers    map[int64]*distHandler
	client      session.Cluster
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	targetMgr   *meta.TargetManager
	scheduler   task.Scheduler
}

func (dc *Controller) StartDistInstance(ctx context.Context, nodeID int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if _, ok := dc.handlers[nodeID]; ok {
		log.Info("node has started", zap.Int64("nodeID", nodeID))
		return
	}
	h := newDistHandler(ctx, nodeID, dc.client, dc.nodeManager, dc.scheduler, dc.dist, dc.targetMgr)
	dc.handlers[nodeID] = h
}

func (dc *Controller) Remove(nodeID int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if h, ok := dc.handlers[nodeID]; ok {
		h.stop()
		delete(dc.handlers, nodeID)
	}
}

func (dc *Controller) SyncAll(ctx context.Context) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	wg := sync.WaitGroup{}
	for _, h := range dc.handlers {
		handler := h
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.getDistribution(ctx)
		}()
	}
	wg.Wait()
}

func (dc *Controller) Stop() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, h := range dc.handlers {
		h.stop()
	}
}

func NewDistController(
	client session.Cluster,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	scheduler task.Scheduler,
) *Controller {
	return &Controller{
		handlers:    make(map[int64]*distHandler),
		client:      client,
		nodeManager: nodeManager,
		dist:        dist,
		targetMgr:   targetMgr,
		scheduler:   scheduler,
	}
}
