package snview

import (
	"math/rand/v2"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// linearHandoff holds no resource references. Existing serving leases alone decide
// whether the source remains eligible. Recovery deliberately does not restore it.
type linearHandoff struct {
	source   qviews.QueryViewVersion
	started  time.Time
	duration time.Duration
	initial  float64
}

func (w *linearHandoff) targetWeight(now time.Time) float64 {
	progress := min(1.0, max(0.0, float64(now.Sub(w.started))/float64(w.duration)))
	return w.initial + (1-w.initial)*progress
}

// Schema-less statistics retain the newest schema. Within one schema, the
// newest immutable load configuration is the data-visibility boundary.
func newerPlanningCandidate(candidate, current *snViewEntry) bool {
	a, b := candidate.sm.meta, current.sm.meta
	if a.GetSchemaVersion() != b.GetSchemaVersion() {
		return a.GetSchemaVersion() > b.GetSchemaVersion()
	}
	if a.GetLoadInfoVersion() != b.GetLoadInfoVersion() {
		return a.GetLoadInfoVersion() > b.GetLoadInfoVersion()
	}
	return candidate.View.QueryViewKey().QueryViewVersion.GT(current.View.QueryViewKey().QueryViewVersion)
}

// Called exactly once for a persisted Ready -> Up transition, under shard.mu.
func (s *snShardView) startHandoffLocked(version qviews.QueryViewVersion, target *snViewEntry) {
	cfg := &paramtable.Get().QueryViewCfg
	if cfg.GetHandoffPolicy() != paramtable.HandoffPolicyLinear {
		return
	}
	duration := cfg.LinearHandoffDuration.GetAsDurationByParse()
	initial := cfg.LinearHandoffInitialRatio.GetAsFloat()
	if duration <= 0 || !(initial >= 0 && initial < 1) {
		return
	}
	var source *snViewEntry
	for otherVersion, other := range s.views {
		if other == target {
			continue
		}
		s.advancePendingDownLocked(otherVersion, other)
		if other.leaseDisabled || other.sm.State() != qviews.QueryViewStateUp || other.sm.meta.GetSchemaVersion() != target.sm.meta.GetSchemaVersion() {
			continue
		}
		if newerPlanningCandidate(other, target) {
			return
		}
		if other.sm.meta.GetLoadInfoVersion() == target.sm.meta.GetLoadInfoVersion() && (source == nil || newerPlanningCandidate(other, source)) {
			source = other
		}
	}
	if source == nil {
		return
	}
	now := time.Now()
	if source.handoff != nil && source.handoff.targetWeight(now) < 1 {
		// Replace an in-progress target, retaining its source when still eligible.
		// The new target starts a fresh window; never stack traffic distributions.
		previousSource := s.views[source.handoff.source]
		source.handoff = nil
		metrics.QVHandoffWindows.WithLabelValues("superseded").Inc()
		if previousSource != nil && !previousSource.leaseDisabled && previousSource.sm.State() == qviews.QueryViewStateUp {
			source = previousSource
		}
	}
	target.handoff = &linearHandoff{source: source.View.QueryViewKey().QueryViewVersion, started: now, duration: duration, initial: initial}
	metrics.QVHandoffWindows.WithLabelValues("started").Inc()
	mlog.Info(s.ctx, "query view handoff started", mlog.String("shard", s.shardID.String()), mlog.String("source", target.handoff.source.String()), mlog.String("target", version.String()), mlog.Duration("duration", duration), mlog.Float64("initialRatio", initial))
}

// Caller has advanced all pending Down entries under shard.mu. Each planning
// attempt samples once; explicit-version execution bypasses this selector.
func (s *snShardView) selectHandoffLocked(version qviews.QueryViewVersion, target *snViewEntry) (qviews.QueryViewVersion, *snViewEntry) {
	w := target.handoff
	if w == nil {
		return version, target
	}
	source := s.views[w.source]
	weight := w.targetWeight(time.Now())
	reason := ""
	switch {
	case paramtable.Get().QueryViewCfg.GetHandoffPolicy() != paramtable.HandoffPolicyLinear:
		reason = "policy_changed"
	case weight >= 1:
		reason = "elapsed"
	case source == nil || source.leaseDisabled || source.sm.State() != qviews.QueryViewStateUp:
		reason = "source_unavailable"
	}
	if reason != "" {
		metrics.QVHandoffWindows.WithLabelValues(reason).Inc()
		mlog.Info(s.ctx, "query view handoff ended", mlog.String("shard", s.shardID.String()), mlog.String("target", version.String()), mlog.String("reason", reason))
		target.handoff = nil
		return version, target
	}
	if rand.Float64() >= weight {
		metrics.QVHandoffPlans.WithLabelValues("source").Inc()
		return w.source, source
	}
	metrics.QVHandoffPlans.WithLabelValues("target").Inc()
	return version, target
}
