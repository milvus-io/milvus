package syncer

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/log"
)

var (
	_ ReliableSyncer = (*reliableSyncer)(nil)

	// ErrSyncerClosed is returned when SyncViews is called on a closed ReliableSyncer.
	ErrSyncerClosed = errors.New("reliable syncer is closed")
)

type reliableSyncer struct {
	client ViewSyncClient

	mu               sync.Mutex
	resumableSyncers map[qviews.WorkNodeKey]*resumableSyncer
	closed           bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReliableSyncer creates a new ReliableSyncer.
func NewReliableSyncer(client ViewSyncClient) ReliableSyncer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &reliableSyncer{
		client:           client,
		resumableSyncers: make(map[qviews.WorkNodeKey]*resumableSyncer),
		ctx:              ctx,
		cancel:           cancel,
	}
	s.wg.Add(1)
	go s.watchNodes()
	return s
}

func (s *reliableSyncer) SyncViews(ctx context.Context, group SyncGroup) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for nodeKey, views := range group.ViewsByNode {
		rs, closed := s.getOrCreateSyncer(ctx, nodeKey, views)
		if closed {
			return ErrSyncerClosed
		}
		if rs != nil {
			rs.Sync(views)
			continue
		}
		// Node not found — notify views immediately.
		for _, sv := range views {
			notifyQueryNodeLost(sv)
		}
	}
	return nil
}

func notifyQueryNodeLost(sv SyncView) {
	if sv.OnQueryNodeLost == nil {
		return
	}
	qn, ok := sv.View.WorkNode().(qviews.QueryNode)
	if !ok {
		return
	}
	sv.OnQueryNodeLost(qn)
}

// getOrCreateSyncer returns the existing ResumableSyncer for the node,
// creates one if the node is alive, or returns (nil, false) if the node is not found.
// Returns (nil, true) if the syncer is closed.
func (s *reliableSyncer) getOrCreateSyncer(ctx context.Context, nodeKey qviews.WorkNodeKey, views []SyncView) (rs *resumableSyncer, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, true
	}
	if rs, ok := s.resumableSyncers[nodeKey]; ok {
		return rs, false
	}
	if len(views) == 0 {
		return nil, false
	}

	// IsNodeAlive is a local cache lookup, safe to call under lock.
	node := views[0].View.WorkNode()
	if !s.client.IsNodeAlive(ctx, node) {
		return nil, false
	}

	log.Info("ReliableSyncer: node discovered on demand, creating ResumableSyncer",
		zap.String("node", nodeKey))
	rs = newResumableSyncer(s.ctx, node, s.client)
	s.resumableSyncers[nodeKey] = rs
	return rs, false
}

func (s *reliableSyncer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.cancel()
	s.wg.Wait()

	// Close all remaining ResumableSyncers (graceful shutdown, no drain).
	s.mu.Lock()
	syncers := s.resumableSyncers
	s.resumableSyncers = nil
	s.mu.Unlock()

	for _, rs := range syncers {
		rs.Close()
	}
	return nil
}

// watchNodes watches node membership changes and drains ResumableSyncers for removed nodes.
func (s *reliableSyncer) watchNodes() {
	defer s.wg.Done()

	for s.ctx.Err() == nil {
		ch, err := s.client.WatchNodeChanged(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Warn("ReliableSyncer: WatchNodeChanged failed, retrying", zap.Error(err))
			continue
		}

		// Initial sync.
		s.drainRemovedNodes()

		// Watch for changes.
		for {
			select {
			case <-s.ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					// Channel closed, re-watch.
					break
				}
				s.drainRemovedNodes()
				continue
			}
			break
		}
	}
}

// drainRemovedNodes fetches the current node set and drains ResumableSyncers for removed nodes.
// It does NOT create ResumableSyncers for new nodes — that is done lazily by tryCreateSyncer.
func (s *reliableSyncer) drainRemovedNodes() {
	nodes, err := s.client.GetAllNodes(s.ctx)
	if err != nil {
		if s.ctx.Err() != nil {
			return
		}
		log.Warn("ReliableSyncer: GetAllNodes failed", zap.Error(err))
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	// Find removed nodes — collect ResumableSyncers to close.
	var removed []removedNode
	for nodeKey, rs := range s.resumableSyncers {
		if _, exists := nodes[nodeKey]; !exists {
			removed = append(removed, removedNode{key: nodeKey, syncer: rs})
			delete(s.resumableSyncers, nodeKey)
		}
	}
	s.mu.Unlock()

	// Close removed ResumableSyncers and drain pending views (node lost).
	for _, r := range removed {
		log.Info("ReliableSyncer: node removed, closing ResumableSyncer",
			zap.String("node", r.key))
		r.syncer.Close()
		r.syncer.DrainPendingIfNodeLost()
	}
}

type removedNode struct {
	key    string
	syncer *resumableSyncer
}
