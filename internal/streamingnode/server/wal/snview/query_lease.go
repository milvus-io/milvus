package snview

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (h *SNQueryViewHandler) AcquireUpView(ctx context.Context, shardID qviews.ShardID, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	h.mu.Lock()
	shard := h.shards[shardID]
	h.mu.Unlock()
	if shard == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", shardID.String())
	}
	return shard.acquireUpView(ctx, version)
}

func (s *snShardView) acquireUpView(ctx context.Context, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return nil, viewerror.NewViewNotFound("query view %s is not found", version.String())
	}
	s.advancePendingDownLocked(version, entry)
	if entry.sm.State() != qviews.QueryViewStateUp {
		return nil, viewerror.NewViewInvalidated("query view %s is not up, current state is %s", version.String(), entry.sm.State().String())
	}
	return s.newQueryViewLeaseLocked(version, entry), nil
}

// Caller holds the shard mutex.
func (s *snShardView) newQueryViewLeaseLocked(version qviews.QueryViewVersion, entry *snViewEntry) *QueryViewLease {
	entry.queryRefs++
	s.renewServingLeaseLocked(entry)
	// A pending Down command must not appear as the locally leased state.
	view := entry.sm.buildReport()
	var once sync.Once
	released := false
	return &QueryViewLease{
		Version: version,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    view,
		Renew: func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !released && !s.closed && s.views[version] == entry && entry.sm.State() == qviews.QueryViewStateUp {
				s.renewServingLeaseLocked(entry)
			}
		},
		Release: func() {
			once.Do(func() {
				s.mu.Lock()
				released = true
				s.mu.Unlock()
				s.releaseQueryViewLease(version)
			})
		},
	}
}

// Caller holds the shard mutex. Timers are only needed after a Down request.
func (s *snShardView) renewServingLeaseLocked(entry *snViewEntry) {
	if s.leaseDuration > 0 {
		deadline := time.Now().Add(s.leaseDuration)
		if deadline.After(entry.leaseExpireAt) {
			entry.leaseExpireAt = deadline
		}
	}
}

// advancePendingDownLocked serializes access, expiry, and Coord's Down intent.
// Active calls protect long-running planning/MVCC waits even past the deadline.
func (s *snShardView) advancePendingDownLocked(version qviews.QueryViewVersion, entry *snViewEntry) {
	if !entry.pendingDown {
		return
	}
	if entry.queryRefs > 0 {
		return
	}
	if remaining := time.Until(entry.leaseExpireAt); remaining > 0 {
		if entry.downTimer == nil {
			entry.downTimer = time.AfterFunc(remaining, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				if s.closed || s.ctx.Err() != nil || s.views[version] != entry {
					return
				}
				entry.downTimer = nil
				s.advancePendingDownLocked(version, entry)
			})
		}
		return
	}
	entry.cancelPendingDown()
	entry.sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	s.consumeReportPersistAndCleanup(version, entry)
}

func (entry *snViewEntry) cancelPendingDown() {
	entry.pendingDown = false
	if entry.downTimer != nil {
		entry.downTimer.Stop()
		entry.downTimer = nil
	}
}
