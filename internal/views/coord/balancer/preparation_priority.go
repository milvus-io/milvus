package balancer

import (
	"math"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

const (
	recentPreparationAccess       = 2 * time.Minute
	minPreparationPriority  int32 = nodescheduler.MinPriority
	maxPreparationPriority  int32 = nodescheduler.MaxPriority
)

// Access recency selects urgency; traffic and configuration only refine it.
// Unknown access history is an initial/manual load, not evidence of cold data.
func preparationPriority(config commonpb.LoadPriority, feedback *viewfeedback.Snapshot, groupRate float64, hasUp, autoLoad bool, now time.Time) int32 {
	recent := feedback != nil && feedback.LastAccessUnixNanos != 0 && now.Sub(time.Unix(0, feedback.LastAccessUnixNanos)) <= recentPreparationAccess
	var base, trafficLimit int32
	switch {
	case recent && !hasUp:
		base, trafficLimit = 10, 8
	case autoLoad && !hasUp:
		base = 10
	case recent:
		trafficLimit = 2
	case hasUp:
		base = -10
	case feedback != nil && feedback.LastAccessUnixNanos != 0:
		base = -5
	}
	if trafficLimit > 0 && groupRate > 0 {
		share := min(1.0, max(0.0, feedback.RequestRates.Last6Windows/groupRate))
		base += int32(math.Round(float64(trafficLimit) * math.Log1p(100*share) / math.Log1p(100)))
	}
	if config == commonpb.LoadPriority_HIGH {
		base += 2
	}
	return min(maxPreparationPriority, max(minPreparationPriority, base))
}

// ViewFeedbackReader returns immutable, replica-wide snapshots owned by the store.
type ViewFeedbackReader interface {
	GetViewFeedback(replicaID int64) (*viewfeedback.Snapshot, bool)
}

type PolicyOption func(*DefaultBalancePolicy)

func WithViewFeedbackReader(reader ViewFeedbackReader) PolicyOption {
	return func(p *DefaultBalancePolicy) { p.feedback = reader }
}

// One operation retains the same replica snapshots for numerator and denominator.
// RG totals span all configured collections; the store's global total includes
// other RGs and therefore is not the denominator used for preparation priority.
type preparationSnapshot struct {
	replicas   map[int64]*viewfeedback.Snapshot
	groupRates map[string]float64
}

func (p *planningContext) preparationPriority(id qviews.ShardID) int32 {
	cfg := p.ConfigForShard(id)
	if cfg == nil {
		return 0
	}
	replica := findReplica(cfg, id.ReplicaID)
	p.capturePreparationFeedback()

	return preparationPriority(replica.Priority, p.preparationFeedback.replicas[id.ReplicaID], p.preparationFeedback.groupRates[replica.ResourceGroup], healthyUp(p, id), cfg.AutoLoad, p.priorityNow)
}

// Capture on first use, once per operation. Reads are O(replica count), without
// scanning shards or windows; subsequent feedback lookups are O(1).
func (p *planningContext) capturePreparationFeedback() {
	if p.preparationFeedback != nil {
		return
	}
	p.preparationFeedback = &preparationSnapshot{replicas: make(map[int64]*viewfeedback.Snapshot), groupRates: make(map[string]float64)}
	if p.feedback == nil {
		return
	}
	p.RangeCollectionIDs(func(id int64) bool {
		collection := p.GetCollection(id)
		if collection == nil || collection.LoadConfig() == nil {
			return true
		}
		for _, replica := range collection.LoadConfig().Replicas {
			sample, ok := p.feedback.GetViewFeedback(replica.ReplicaID)
			if !ok {
				continue
			}
			p.preparationFeedback.replicas[replica.ReplicaID] = sample
			p.preparationFeedback.groupRates[replica.ResourceGroup] += sample.RequestRates.Last6Windows
		}
		return true
	})
}

func (b *DefaultBalancer) preparationPriorities() *planningContext {
	p := newPlanningContext(b.cache)
	if policy, ok := b.policy.(*DefaultBalancePolicy); ok {
		p.feedback = policy.feedback
	}
	return p
}
