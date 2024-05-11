package metricsutil

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// labeledRecord is a labeled sample point.
type labeledRecord interface {
	// Label of the access metric.
	Label() SegmentLabel

	// Finish finishes the record.
	Finish(err error)

	// getError returns the error of the record.
	// current metric system simply reject the error operation.
	getError() error
}

// globalObserver is the global resource groups observer.
var (
	once           sync.Once
	globalObserver *segmentsObserver
)

func getGlobalObserver() *segmentsObserver {
	once.Do(func() {
		globalObserver = newSegmentsObserver()
		go func() {
			d := 15 * time.Minute
			ticker := time.NewTicker(d)
			defer ticker.Stop()
			for range ticker.C {
				expireAt := time.Now().Add(-d)
				globalObserver.Expire(expireAt)
			}
		}()
	})
	return globalObserver
}

// newSegmentsObserver creates a new segmentsObserver.
// Used to check if a segment is hot or cold.
func newSegmentsObserver() *segmentsObserver {
	return &segmentsObserver{
		nodeID:   strconv.FormatInt(paramtable.GetNodeID(), 10),
		segments: typeutil.NewConcurrentMap[SegmentLabel, *segmentObserver](),
	}
}

// segmentsObserver is a observer all segments metrics.
type segmentsObserver struct {
	nodeID   string
	segments *typeutil.ConcurrentMap[SegmentLabel, *segmentObserver] // map segment id to observer.
	// one segment can be removed from one query node, for balancing or compacting.
	// no more search operation will be performed on the segment after it is removed.
	// all related metric should be expired after a while.
	// may be a huge map with 100000+ entries.
}

// Observe records a new metric
func (o *segmentsObserver) Observe(m labeledRecord) {
	if m.getError() != nil {
		return // reject error record.
		// TODO: add error as a label of metrics.
	}
	// fast path.
	label := m.Label()
	observer, ok := o.segments.Get(label)
	if !ok {
		// slow path.
		newObserver := newSegmentObserver(o.nodeID, label)
		observer, _ = o.segments.GetOrInsert(label, newObserver)
	}
	// do a observer.
	observer.Observe(m)
}

// Expire expires the observer.
func (o *segmentsObserver) Expire(expiredAt time.Time) {
	o.segments.Range(func(label SegmentLabel, value *segmentObserver) bool {
		if value.IsExpired(expiredAt) {
			o.segments.Remove(label)
			value.Clear()
			return true
		}
		return true
	})
}

// newSegmentObserver creates a new segmentObserver.
func newSegmentObserver(nodeID string, label SegmentLabel) *segmentObserver {
	now := time.Now()
	return &segmentObserver{
		label:       label,
		prom:        newPromObserver(nodeID, label),
		lastUpdates: atomic.NewPointer[time.Time](&now),
	}
}

// segmentObserver is a observer for segment metrics.
type segmentObserver struct {
	label SegmentLabel // never updates
	// observers.
	prom promMetricsObserver // prometheus metrics observer.
	// for expiration.
	lastUpdates *atomic.Pointer[time.Time] // update every access.
}

// IsExpired checks if the segment observer is expired.
func (o *segmentObserver) IsExpired(expireAt time.Time) bool {
	return o.lastUpdates.Load().Before(expireAt)
}

// Observe observe a new
func (o *segmentObserver) Observe(m labeledRecord) {
	now := time.Now()
	o.lastUpdates.Store(&now)

	switch mm := m.(type) {
	case QuerySegmentAccessRecord:
		o.prom.ObserveQueryAccess(mm)
	case SearchSegmentAccessRecord:
		o.prom.ObserveSearchAccess(mm)
	default:
		panic("unknown segment access metric")
	}
}

// Clear clears the observer.
func (o *segmentObserver) Clear() {
	o.prom.Clear()
}

// newPromObserver creates a new promMetrics.
func newPromObserver(nodeID string, label SegmentLabel) promMetricsObserver {
	return promMetricsObserver{
		nodeID:                               nodeID,
		label:                                label,
		QuerySegmentAccessTotal:              metrics.QueryNodeSegmentAccessTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		QuerySegmentAccessDuration:           metrics.QueryNodeSegmentAccessDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		QuerySegmentAccessWaitCacheTotal:     metrics.QueryNodeSegmentAccessWaitCacheTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		QuerySegmentAccessWaitCacheDuration:  metrics.QueryNodeSegmentAccessWaitCacheDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		SearchSegmentAccessTotal:             metrics.QueryNodeSegmentAccessTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
		SearchSegmentAccessDuration:          metrics.QueryNodeSegmentAccessDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
		SearchSegmentAccessWaitCacheTotal:    metrics.QueryNodeSegmentAccessWaitCacheTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
		SearchSegmentAccessWaitCacheDuration: metrics.QueryNodeSegmentAccessWaitCacheDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),

		QuerySegmentAccessGlobalDuration:           metrics.QueryNodeSegmentAccessGlobalDuration.WithLabelValues(nodeID, metrics.QueryLabel),
		SearchSegmentAccessGlobalDuration:          metrics.QueryNodeSegmentAccessGlobalDuration.WithLabelValues(nodeID, metrics.SearchLabel),
		QuerySegmentAccessWaitCacheGlobalDuration:  metrics.QueryNodeSegmentAccessWaitCacheGlobalDuration.WithLabelValues(nodeID, metrics.QueryLabel),
		SearchSegmentAccessWaitCacheGlobalDuration: metrics.QueryNodeSegmentAccessWaitCacheGlobalDuration.WithLabelValues(nodeID, metrics.SearchLabel),
	}
}

// promMetricsObserver is a observer for prometheus metrics.
type promMetricsObserver struct {
	nodeID string
	label  SegmentLabel // never updates

	QuerySegmentAccessTotal              prometheus.Counter
	QuerySegmentAccessDuration           prometheus.Counter
	QuerySegmentAccessWaitCacheTotal     prometheus.Counter
	QuerySegmentAccessWaitCacheDuration  prometheus.Counter
	SearchSegmentAccessTotal             prometheus.Counter
	SearchSegmentAccessDuration          prometheus.Counter
	SearchSegmentAccessWaitCacheTotal    prometheus.Counter
	SearchSegmentAccessWaitCacheDuration prometheus.Counter

	QuerySegmentAccessGlobalDuration           prometheus.Observer
	SearchSegmentAccessGlobalDuration          prometheus.Observer
	QuerySegmentAccessWaitCacheGlobalDuration  prometheus.Observer
	SearchSegmentAccessWaitCacheGlobalDuration prometheus.Observer
}

// ObserveQueryAccess records a new query access.
func (o *promMetricsObserver) ObserveQueryAccess(r QuerySegmentAccessRecord) {
	o.QuerySegmentAccessTotal.Inc()
	d := r.getMilliseconds()
	o.QuerySegmentAccessDuration.Add(d)
	o.QuerySegmentAccessGlobalDuration.Observe(d)
	if r.isCacheMiss {
		o.QuerySegmentAccessWaitCacheTotal.Inc()
		d := r.getWaitLoadMilliseconds()
		o.QuerySegmentAccessWaitCacheDuration.Add(d)
		o.QuerySegmentAccessWaitCacheGlobalDuration.Observe(d)
	}
}

// ObserveSearchAccess records a new search access.
func (o *promMetricsObserver) ObserveSearchAccess(r SearchSegmentAccessRecord) {
	o.SearchSegmentAccessTotal.Inc()
	d := r.getMilliseconds()
	o.SearchSegmentAccessDuration.Add(d)
	o.SearchSegmentAccessGlobalDuration.Observe(d)
	if r.isCacheMiss {
		o.SearchSegmentAccessWaitCacheTotal.Inc()
		d := r.getWaitLoadMilliseconds()
		o.SearchSegmentAccessWaitCacheDuration.Add(d)
		o.SearchSegmentAccessWaitCacheGlobalDuration.Observe(d)
	}
}

// Clear clears the prometheus metrics.
func (o *promMetricsObserver) Clear() {
	label := o.label

	metrics.QueryNodeSegmentAccessTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)
	metrics.QueryNodeSegmentAccessDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)

	metrics.QueryNodeSegmentAccessWaitCacheTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessWaitCacheTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)
	metrics.QueryNodeSegmentAccessWaitCacheDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessWaitCacheDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)
}
