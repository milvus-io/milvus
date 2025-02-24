package metricsutil

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

var (
	_ labeledRecord = QuerySegmentAccessRecord{}
	_ labeledRecord = SearchSegmentAccessRecord{}
	_ labeledRecord = &CacheLoadRecord{}
	_ labeledRecord = &CacheEvictRecord{}
)

// SegmentLabel is the label of a segment.
type SegmentLabel struct {
	DatabaseName  string `expr:"DatabaseName"`
	ResourceGroup string `expr:"ResourceGroup"`
}

// CacheLoadRecord records the metrics of a cache load.
type CacheLoadRecord struct {
	numBytes uint64
	baseRecord
}

// NewCacheLoadRecord creates a new CacheLoadRecord.
func NewCacheLoadRecord(label SegmentLabel) *CacheLoadRecord {
	return &CacheLoadRecord{
		baseRecord: newBaseRecord(label),
	}
}

// WithBytes sets the bytes of the record.
func (r *CacheLoadRecord) WithBytes(bytes uint64) *CacheLoadRecord {
	r.numBytes = bytes
	return r
}

// getBytes returns the bytes of the record.
func (r *CacheLoadRecord) getBytes() float64 {
	return float64(r.numBytes)
}

// Finish finishes the record.
func (r *CacheLoadRecord) Finish(err error) {
	r.baseRecord.finish(err)
	getGlobalObserver().Observe(r)
}

type CacheEvictRecord struct {
	bytes uint64
	baseRecord
}

// NewCacheEvictRecord creates a new CacheEvictRecord.
func NewCacheEvictRecord(label SegmentLabel) *CacheEvictRecord {
	return &CacheEvictRecord{
		baseRecord: newBaseRecord(label),
	}
}

// WithBytes sets the bytes of the record.
func (r *CacheEvictRecord) WithBytes(bytes uint64) *CacheEvictRecord {
	r.bytes = bytes
	return r
}

// getBytes returns the bytes of the record.
func (r *CacheEvictRecord) getBytes() float64 {
	return float64(r.bytes)
}

// Finish finishes the record.
func (r *CacheEvictRecord) Finish(err error) {
	r.baseRecord.finish(err)
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

// getWaitLoadMilliseconds returns the wait load seconds of the recorder.
func (r *segmentAccessRecord) getWaitLoadMilliseconds() float64 {
	return r.waitLoadCost.Seconds() * 1000
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

// getMilliseconds returns the duration of the recorder in seconds.
func (r *baseRecord) getMilliseconds() float64 {
	return r.duration.Seconds() * 1000
}

// finish finishes the record.
func (r *baseRecord) finish(err error) {
	r.err = err
	r.duration = r.timeRecorder.ElapseSpan()
}
