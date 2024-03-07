package metricsutil

import (
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/metricsutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

// labeledRecord is a labeled sample point.
type labeledRecord interface {
	// Label of the access metric.
	Label() SegmentLabel

	Finish(err error)
}

// globalObserver is the global resource groups observer.
var (
	once           sync.Once
	globalObserver *segmentsObserver
)

func getGlobalObserver() *segmentsObserver {
	once.Do(func() {
		globalObserver = newSegmentsObserver()
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

// ExpireAndObserve expire all expired observer and observe all non-expired observer.
func (o *segmentsObserver) ExpireAndObserve(observer func(label SegmentLabel, snapshot SegmentObserverSnapshot), expiredAt time.Time) {
	o.segments.Range(func(label SegmentLabel, value *segmentObserver) bool {
		if value.IsExpired(expiredAt) {
			o.segments.Remove(label)
			value.Clear()
			return true
		}
		observer(label, value.Snapshot())
		return true
	})
}

// newSegmentObserver creates a new segmentObserver.
func newSegmentObserver(nodeID string, label SegmentLabel) *segmentObserver {
	now := time.Now()
	return &segmentObserver{
		label:         label,
		prom:          newPromObserver(nodeID, label),
		cache:         newSegmentCacheObserver(),
		searchAccess:  newSegmentAccessObserver(),
		queryAccess:   newSegmentAccessObserver(),
		lastUpdates:   atomic.NewPointer[time.Time](&now),
		resourceUsage: atomic.NewPointer[ResourceUsageMetrics](&ResourceUsageMetrics{}),
	}
}

// segmentObserver is a observer for segment metrics.
type segmentObserver struct {
	label SegmentLabel // never updates

	// observers.
	prom         promMetricsObserver   // prometheus metrics observer.
	cache        segmentCacheObserver  // cache metrics observer.
	searchAccess segmentAccessObserver // search metrics observer.
	queryAccess  segmentAccessObserver // query metrics observer.

	// resource usage.
	resourceUsage *atomic.Pointer[ResourceUsageMetrics] // resource usage observer.
	// for expiration.
	lastUpdates *atomic.Pointer[time.Time] // update every access.
}

// Observe observe a new
func (o *segmentObserver) Observe(m labeledRecord) {
	now := time.Now()
	o.lastUpdates.Store(&now)

	switch m.(type) {
	case *CacheLoadRecord:
		o.prom.ObserveLoad(m.(*CacheLoadRecord))
		o.cache.ObserveLoad(m.(*CacheLoadRecord))
	case *CacheEvictRecord:
		o.prom.ObserveEvict(m.(*CacheEvictRecord))
		o.cache.ObserveEvict(m.(*CacheEvictRecord))
	case QuerySegmentAccessRecord:
		o.prom.ObserveQueryAccess(m.(QuerySegmentAccessRecord))
		o.queryAccess.Observe(m.(QuerySegmentAccessRecord).segmentAccessRecord)
	case SearchSegmentAccessRecord:
		o.prom.ObserveSearchAccess(m.(SearchSegmentAccessRecord))
		o.searchAccess.Observe(m.(SearchSegmentAccessRecord).segmentAccessRecord)
	case *ResourceEstimateRecord:
		o.resourceUsage.Store(m.(*ResourceEstimateRecord).ResourceUsageMetrics)
	default:
		panic("unknown segment access metric")
	}
}

// IsExpired checks if the segment observer is expired.
func (o *segmentObserver) IsExpired(expireAt time.Time) bool {
	return o.lastUpdates.Load().Before(expireAt)
}

// Snapshot returns a snapshot of the observer.
func (o *segmentObserver) Snapshot() SegmentObserverSnapshot {
	return SegmentObserverSnapshot{
		Time:          time.Now(),
		Label:         o.label,
		ResourceUsage: *o.resourceUsage.Load(),
		Cache:         o.cache.Snapshot(),
		SearchAccess:  o.searchAccess.Snapshot(),
		QueryAccess:   o.queryAccess.Snapshot(),
	}
}

func (o *segmentObserver) Clear() {
	o.prom.Clear()
}

// newSegmentCacheObserver creates a new segmentCacheObserver.
func newSegmentCacheObserver() segmentCacheObserver {
	return segmentCacheObserver{
		loadTotal:    metricsutil.NewCounter(),
		loadDuration: metricsutil.NewDuration(),
	}
}

// segmentCacheObserver is a observer for segment cache metrics.
type segmentCacheObserver struct {
	loadTotal    metricsutil.Counter  // real load data count.
	loadDuration metricsutil.Duration // real load data time cost.
}

// ObserveLoad records a new load.
func (o *segmentCacheObserver) ObserveLoad(r *CacheLoadRecord) {
	o.loadTotal.Inc()
	o.loadDuration.Add(r.getDuration())
}

// ObserveLoad records a new load.
func (o *segmentCacheObserver) ObserveEvict(r *CacheEvictRecord) {
	o.loadTotal.Inc()
	o.loadDuration.Add(r.getDuration())
}

// Snapshot returns a snapshot of the observer.
func (o *segmentCacheObserver) Snapshot() CacheObserverSnapshot {
	return CacheObserverSnapshot{
		LoadTotal:    int64(o.loadTotal.Get()),
		LoadDuration: o.loadDuration.Get(),
	}
}

// newSegmentAccessObserver creates a new segmentAccessObserver.
func newSegmentAccessObserver() segmentAccessObserver {
	return segmentAccessObserver{
		total:            metricsutil.NewCounter(),
		duration:         metricsutil.NewDuration(),
		waitLoadTotal:    metricsutil.NewCounter(),
		waitLoadDuration: metricsutil.NewDuration(),
	}
}

// segmentAccessObserver is a observer for segment access metrics.
type segmentAccessObserver struct {
	total            metricsutil.Counter  // total access count.
	duration         metricsutil.Duration // search or query time cost.
	waitLoadTotal    metricsutil.Counter  // cache missing count.
	waitLoadDuration metricsutil.Duration // search or query may blocked by loading data when cache miss, total time cost.
}

// recordAccess records a new access.
func (o *segmentAccessObserver) Observe(r *segmentAccessRecord) {
	o.total.Inc()
	o.duration.Add(r.getDuration())
	if r.isCacheMiss {
		o.waitLoadTotal.Inc()
		o.waitLoadDuration.Add(r.getWaitLoadDuration())
	}
}

// Snapshot returns a snapshot of the observer.
func (o *segmentAccessObserver) Snapshot() AccessObserverSnapshot {
	return AccessObserverSnapshot{
		Total:            int64(o.total.Get()),
		Duration:         o.duration.Get(),
		WaitLoadTotal:    int64(o.waitLoadTotal.Get()),
		WaitLoadDuration: o.waitLoadDuration.Get(),
	}
}

// newPromObserver creates a new promMetrics.
func newPromObserver(nodeID string, label SegmentLabel) promMetricsObserver {
	return promMetricsObserver{
		nodeID:                               nodeID,
		label:                                label,
		DiskCacheLoadTotal:                   metrics.QueryNodeDiskCacheLoadTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup),
		DiskCacheLoadDuration:                metrics.QueryNodeDiskCacheLoadDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup),
		DiskCacheLoadBytes:                   metrics.QueryNodeDiskCacheLoadBytes.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup),
		DiskCacheEvictTotal:                  metrics.QueryNodeDiskCacheEvictTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup),
		DiskCacheEvictDuration:               metrics.QueryNodeDiskCacheEvictDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup),
		QuerySegmentAccessTotal:              metrics.QueryNodeSegmentAccessTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		QuerySegmentAccessDuration:           metrics.QueryNodeSegmentAccessDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		QuerySegmentAccessWaitCacheTotal:     metrics.QueryNodeSegmentAccessWaitCacheTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		QuerySegmentAccessWaitCacheDuration:  metrics.QueryNodeSegmentAccessWaitCacheDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel),
		SearchSegmentAccessTotal:             metrics.QueryNodeSegmentAccessTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
		SearchSegmentAccessDuration:          metrics.QueryNodeSegmentAccessDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
		SearchSegmentAccessWaitCacheTotal:    metrics.QueryNodeSegmentAccessWaitCacheTotal.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
		SearchSegmentAccessWaitCacheDuration: metrics.QueryNodeSegmentAccessWaitCacheDuration.WithLabelValues(nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel),
	}
}

// promMetricsObserver is a observer for prometheus metrics.
type promMetricsObserver struct {
	nodeID string
	label  SegmentLabel // never updates

	DiskCacheLoadTotal                   prometheus.Counter
	DiskCacheLoadDuration                prometheus.Counter
	DiskCacheLoadBytes                   prometheus.Counter
	DiskCacheEvictTotal                  prometheus.Counter
	DiskCacheEvictDuration               prometheus.Counter
	QuerySegmentAccessTotal              prometheus.Counter
	QuerySegmentAccessDuration           prometheus.Counter
	QuerySegmentAccessWaitCacheTotal     prometheus.Counter
	QuerySegmentAccessWaitCacheDuration  prometheus.Counter
	SearchSegmentAccessTotal             prometheus.Counter
	SearchSegmentAccessDuration          prometheus.Counter
	SearchSegmentAccessWaitCacheTotal    prometheus.Counter
	SearchSegmentAccessWaitCacheDuration prometheus.Counter
}

// RecordAccess records a new access.
func (o *promMetricsObserver) ObserveLoad(r *CacheLoadRecord) {
	o.DiskCacheLoadTotal.Inc()
	o.DiskCacheLoadDuration.Add(r.getSeconds())
	o.DiskCacheLoadBytes.Add(r.getBytes())
}

// RecordAccess records a new access.
func (o *promMetricsObserver) ObserveEvict(r *CacheEvictRecord) {
	o.DiskCacheEvictTotal.Inc()
	o.DiskCacheEvictDuration.Add(r.getSeconds())
}

func (o *promMetricsObserver) ObserveQueryAccess(r QuerySegmentAccessRecord) {
	o.QuerySegmentAccessTotal.Inc()
	o.QuerySegmentAccessDuration.Add(r.getSeconds())
	if r.isCacheMiss {
		o.QuerySegmentAccessWaitCacheTotal.Inc()
		o.QuerySegmentAccessWaitCacheDuration.Add(r.getWaitLoadSeconds())
	}
}

func (o *promMetricsObserver) ObserveSearchAccess(r SearchSegmentAccessRecord) {
	o.SearchSegmentAccessTotal.Inc()
	o.SearchSegmentAccessDuration.Add(r.getSeconds())
	if r.isCacheMiss {
		o.SearchSegmentAccessWaitCacheTotal.Inc()
		o.SearchSegmentAccessWaitCacheDuration.Add(r.getWaitLoadSeconds())
	}
}

func (o *promMetricsObserver) Clear() {
	label := o.label

	metrics.QueryNodeDiskCacheLoadTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup)
	metrics.QueryNodeDiskCacheLoadBytes.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup)
	metrics.QueryNodeDiskCacheLoadDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup)
	metrics.QueryNodeDiskCacheEvictTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup)
	metrics.QueryNodeDiskCacheEvictDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup)

	metrics.QueryNodeSegmentAccessTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)
	metrics.QueryNodeSegmentAccessDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)

	metrics.QueryNodeSegmentAccessWaitCacheTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessWaitCacheTotal.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)
	metrics.QueryNodeSegmentAccessWaitCacheDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.SearchLabel)
	metrics.QueryNodeSegmentAccessWaitCacheDuration.DeleteLabelValues(o.nodeID, label.DatabaseName, label.ResourceGroup, metrics.QueryLabel)
}
