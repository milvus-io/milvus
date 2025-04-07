package metricsutil

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

// TimeTickMetrics is the metrics for time tick.
type TimeTickMetrics struct {
	mu                                 syncutil.ClosableLock
	constLabel                         prometheus.Labels
	allocatedTimeTickCounter           *prometheus.CounterVec
	allocatedTimeTickDuration          prometheus.Observer
	acknowledgedTimeTickCounterForSync prometheus.Counter
	syncTimeTickCounterForSync         prometheus.Counter
	acknowledgedTimeTickCounter        prometheus.Counter
	syncTimeTickCounter                prometheus.Counter
	lastAllocatedTimeTick              prometheus.Gauge
	lastConfirmedTimeTick              prometheus.Gauge
	persistentTimeTickSyncCounter      prometheus.Counter
	persistentTimeTickSync             prometheus.Gauge
	nonPersistentTimeTickSyncCounter   prometheus.Counter
	nonPersistentTimeTickSync          prometheus.Gauge
}

// NewTimeTickMetrics creates a new time tick metrics.
func NewTimeTickMetrics(pchannel string) *TimeTickMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	return &TimeTickMetrics{
		mu:                                 syncutil.ClosableLock{},
		constLabel:                         constLabel,
		allocatedTimeTickCounter:           metrics.WALAllocateTimeTickTotal.MustCurryWith(constLabel),
		allocatedTimeTickDuration:          metrics.WALTimeTickAllocateDurationSeconds.With(constLabel),
		acknowledgedTimeTickCounterForSync: metrics.WALAcknowledgeTimeTickTotal.MustCurryWith(constLabel).WithLabelValues("sync"),
		syncTimeTickCounterForSync:         metrics.WALSyncTimeTickTotal.MustCurryWith(constLabel).WithLabelValues("sync"),
		acknowledgedTimeTickCounter:        metrics.WALAcknowledgeTimeTickTotal.MustCurryWith(constLabel).WithLabelValues("common"),
		syncTimeTickCounter:                metrics.WALSyncTimeTickTotal.MustCurryWith(constLabel).WithLabelValues("common"),
		lastAllocatedTimeTick:              metrics.WALLastAllocatedTimeTick.With(constLabel),
		lastConfirmedTimeTick:              metrics.WALLastConfirmedTimeTick.With(constLabel),
		persistentTimeTickSyncCounter:      metrics.WALTimeTickSyncTotal.MustCurryWith(constLabel).WithLabelValues("persistent"),
		persistentTimeTickSync:             metrics.WALTimeTickSyncTimeTick.MustCurryWith(constLabel).WithLabelValues("persistent"),
		nonPersistentTimeTickSyncCounter:   metrics.WALTimeTickSyncTotal.MustCurryWith(constLabel).WithLabelValues("memory"),
		nonPersistentTimeTickSync:          metrics.WALTimeTickSyncTimeTick.MustCurryWith(constLabel).WithLabelValues("memory"),
	}
}

// StartAllocateTimeTick starts to allocate time tick.
func (m *TimeTickMetrics) StartAllocateTimeTick() *AllocateTimeTickMetricsGuard {
	return &AllocateTimeTickMetricsGuard{
		start: time.Now(),
		inner: m,
	}
}

// AllocateTimeTickMetricsGuard is a guard for allocate time tick metrics.
type AllocateTimeTickMetricsGuard struct {
	inner *TimeTickMetrics
	start time.Time
}

// Done finishes the allocate time tick metrics.
func (g *AllocateTimeTickMetricsGuard) Done(ts uint64, err error) {
	status := parseError(err)

	if !g.inner.mu.LockIfNotClosed() {
		return
	}
	g.inner.allocatedTimeTickDuration.Observe(time.Since(g.start).Seconds())
	g.inner.allocatedTimeTickCounter.WithLabelValues(status).Inc()
	if err == nil {
		g.inner.lastAllocatedTimeTick.Set(tsoutil.PhysicalTimeSeconds(ts))
	}
	g.inner.mu.Unlock()
}

func (m *TimeTickMetrics) CountAcknowledgeTimeTick(isSync bool) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	if isSync {
		m.acknowledgedTimeTickCounterForSync.Inc()
	} else {
		m.acknowledgedTimeTickCounter.Inc()
	}
	m.mu.Unlock()
}

func (m *TimeTickMetrics) CountSyncTimeTick(isSync bool) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	if isSync {
		m.syncTimeTickCounterForSync.Inc()
	} else {
		m.syncTimeTickCounter.Inc()
	}
	m.mu.Unlock()
}

func (m *TimeTickMetrics) CountTimeTickSync(ts uint64, persist bool) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	if persist {
		m.persistentTimeTickSyncCounter.Inc()
		m.persistentTimeTickSync.Set(tsoutil.PhysicalTimeSeconds(ts))
	} else {
		m.nonPersistentTimeTickSyncCounter.Inc()
		m.nonPersistentTimeTickSync.Set(tsoutil.PhysicalTimeSeconds(ts))
	}
	m.mu.Unlock()
}

func (m *TimeTickMetrics) UpdateLastConfirmedTimeTick(ts uint64) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.lastConfirmedTimeTick.Set(tsoutil.PhysicalTimeSeconds(ts))
	m.mu.Unlock()
}

func (m *TimeTickMetrics) Close() {
	// mark as closed and delete all labeled metrics
	m.mu.Close()
	metrics.WALAllocateTimeTickTotal.DeletePartialMatch(m.constLabel)
	metrics.WALTimeTickAllocateDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALLastAllocatedTimeTick.Delete(m.constLabel)
	metrics.WALLastConfirmedTimeTick.Delete(m.constLabel)
	metrics.WALAcknowledgeTimeTickTotal.DeletePartialMatch(m.constLabel)
	metrics.WALSyncTimeTickTotal.DeletePartialMatch(m.constLabel)
	metrics.WALTimeTickSyncTimeTick.DeletePartialMatch(m.constLabel)
	metrics.WALTimeTickSyncTotal.DeletePartialMatch(m.constLabel)
}
