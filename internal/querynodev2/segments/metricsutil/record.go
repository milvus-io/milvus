package metricsutil

import (
	"time"

	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var (
	_ labeledRecord = QuerySegmentAccessRecord{}
	_ labeledRecord = SearchSegmentAccessRecord{}
	_ labeledRecord = &ResourceEstimateRecord{}
	_ labeledRecord = &CacheLoadRecord{}
	_ labeledRecord = &CacheEvictRecord{}
)

// CacheLoadRecord records the metrics of a cache load.
type CacheLoadRecord struct {
	bytes uint64
	baseRecord
}

// NewCacheLoadRecord creates a new CacheLoadRecord.
func NewCacheLoadRecord(label SegmentLabel) *CacheLoadRecord {
	return &CacheLoadRecord{
		baseRecord: newBaseRecord(label),
	}
}

func (r *CacheLoadRecord) WithBytes(bytes uint64) *CacheLoadRecord {
	r.bytes = bytes
	return r
}

func (r *CacheLoadRecord) getBytes() float64 {
	return float64(r.bytes)
}

// Finish finishes the record.
func (r *CacheLoadRecord) Finish(err error) {
	r.baseRecord.finish(err)
	getGlobalObserver().Observe(r)
}

type CacheEvictRecord struct {
	baseRecord
}

// NewCacheEvictRecord creates a new CacheEvictRecord.
func NewCacheEvictRecord(label SegmentLabel) *CacheEvictRecord {
	return &CacheEvictRecord{
		baseRecord: newBaseRecord(label),
	}
}

// Finish finishes the record.
func (r *CacheEvictRecord) Finish(err error) {
	r.baseRecord.finish(err)
	getGlobalObserver().Observe(r)
}

// NewResourceEstimateRecord creates a new ResourceEstimateRecord.
func NewResourceEstimateRecord(label SegmentLabel) *ResourceEstimateRecord {
	return &ResourceEstimateRecord{
		label:                label,
		ResourceUsageMetrics: &ResourceUsageMetrics{},
	}
}

// ResourceEstimateRecord records the resource usage of a segment.
// It is just a estimate of the resource usage.
type ResourceEstimateRecord struct {
	label SegmentLabel
	*ResourceUsageMetrics
}

// Label returns the label of the recorder.
func (r *ResourceEstimateRecord) Label() SegmentLabel {
	return r.label
}

// WithMemUsage sets the memory usage of the segment.
func (r *ResourceEstimateRecord) WithMemUsage(memUsage uint64) *ResourceEstimateRecord {
	r.MemUsage = memUsage
	return r
}

// WithDiskUsage sets the disk usage of the segment.
func (r *ResourceEstimateRecord) WithDiskUsage(diskUsage uint64) *ResourceEstimateRecord {
	r.DiskUsage = diskUsage
	return r
}

// WithRowNum sets the row number of the segment.
func (r *ResourceEstimateRecord) WithRowNum(rowNum uint64) *ResourceEstimateRecord {
	r.RowNum = rowNum
	return r
}

func (r *ResourceEstimateRecord) Finish(_ error) {
	getGlobalObserver().Observe(r)
}

// NewQuerySegmentAccessRecord creates a new QuerySegmentMetricRecorder.
func NewQuerySegmentAccessRecord(label SegmentLabel) QuerySegmentAccessRecord {
	return QuerySegmentAccessRecord{
		segmentAccessRecord: newSegmentAccessRecord(label),
	}
}

// NewSearchSegmentAccessRecord creates a new SearchSegmentMetricRecorder.
func NewSearchSegmentAccessRecord(label SegmentLabel) SearchSegmentAccessRecord {
	return SearchSegmentAccessRecord{
		segmentAccessRecord: newSegmentAccessRecord(label),
	}
}

// QuerySegmentAccessRecord records the metrics of a query segment.
type QuerySegmentAccessRecord struct {
	*segmentAccessRecord
}

func (r QuerySegmentAccessRecord) Finish(err error) {
	r.finish(err)
	getGlobalObserver().Observe(r)
}

// SearchSegmentAccessRecord records the metrics of a search segment.
type SearchSegmentAccessRecord struct {
	*segmentAccessRecord
}

func (r SearchSegmentAccessRecord) Finish(err error) {
	r.finish(err)
	getGlobalObserver().Observe(r)
}

// segmentAccessRecord records the metrics of the segment.
type segmentAccessRecord struct {
	isCacheMiss  bool          // whether the access is a cache miss.
	waitLoadCost time.Duration // time cost of waiting for loading data.
	baseRecord
}

// newSegmentAccessRecord creates a new accessMetricRecorder.
func newSegmentAccessRecord(label SegmentLabel) *segmentAccessRecord {
	return &segmentAccessRecord{
		baseRecord: newBaseRecord(label),
	}
}

// CacheMissing records the cache missing.
func (r *segmentAccessRecord) CacheMissing() {
	r.isCacheMiss = true
	r.waitLoadCost = r.timeRecorder.RecordSpan()
}

// getWaitLoadSeconds returns the wait load seconds of the recorder.
func (r *segmentAccessRecord) getWaitLoadSeconds() float64 {
	return r.waitLoadCost.Seconds()
}

// getWaitLoadDuration returns the wait load duration of the recorder.
func (r *segmentAccessRecord) getWaitLoadDuration() time.Duration {
	return r.waitLoadCost
}

// newBaseRecord returns a new baseRecord.
func newBaseRecord(label SegmentLabel) baseRecord {
	return baseRecord{
		label:        label,
		timeRecorder: timerecord.NewTimeRecorder(""),
	}
}

// baseRecord records the metrics of the segment.
type baseRecord struct {
	label        SegmentLabel
	duration     time.Duration
	err          error
	timeRecorder *timerecord.TimeRecorder
}

// Label returns the label of the recorder.
func (r *baseRecord) Label() SegmentLabel {
	return r.label
}

// getError returns the error of the recorder.
func (r *baseRecord) getError() error {
	return r.err
}

// getDuration returns the duration of the recorder.
func (r *baseRecord) getDuration() time.Duration {
	return r.duration
}

// getSeconds returns the duration of the recorder in seconds.
func (r *baseRecord) getSeconds() float64 {
	return r.duration.Seconds()
}

// finish finishes the record.
func (r *baseRecord) finish(err error) {
	r.err = err
	r.duration = r.timeRecorder.ElapseSpan()
}
