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

// ViewFeedbackReader returns one consistent version of replica feedback and RG totals.
type ViewFeedbackReader interface {
	GetPlanningSnapshot() *viewfeedback.PlanningSnapshot
}

type PolicyOption func(*DefaultBalancePolicy)

func WithViewFeedbackReader(reader ViewFeedbackReader) PolicyOption {
	return func(p *DefaultBalancePolicy) { p.feedback = reader }
}

func (p *planningContext) preparationPriority(id qviews.ShardID) int32 {
	cfg := p.ConfigForShard(id)
	if cfg == nil {
		return 0
	}
	replica := findReplica(cfg, id.ReplicaID)
	if p.preparationFeedback == nil {
		if p.feedback != nil {
			p.preparationFeedback = p.feedback.GetPlanningSnapshot()
		} else {
			p.preparationFeedback = &viewfeedback.PlanningSnapshot{}
		}
	}
	sample, rates := p.preparationFeedback.GetReplicaFeedback(id.ReplicaID, replica.ResourceGroup)

	return preparationPriority(replica.Priority, sample, rates.Last6Windows, healthyUp(p, id), cfg.AutoLoad, p.priorityNow)
}

func (b *DefaultBalancer) preparationPriorities() *planningContext {
	p := newPlanningContext(b.cache)
	if policy, ok := b.policy.(*DefaultBalancePolicy); ok {
		p.feedback = policy.feedback
	}
	return p
}
