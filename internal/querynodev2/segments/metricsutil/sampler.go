package metricsutil

import (
	"time"
)

const (
	metricExpire   = 30 * time.Minute
	snapshotExpire = 5 * time.Minute
)

// newSampler creates a new sampler.
func newSampler() *sampler {
	return &sampler{
		history: make(map[SegmentLabel]*segmentHistory),
	}
}

// sampler observes the metrics in global observer, sample a snapshot, and update the metrics output.
type sampler struct {
	history map[SegmentLabel]*segmentHistory // history may be a large map with 100000+ entries.
}

// GetHistory returns the history of the sampler.
func (o *sampler) GetHistory() map[SegmentLabel]*segmentHistory {
	return o.history
}

// Scrape scrapes the metrics.
func (o *sampler) Scrape() {
	now := time.Now()
	o.scrapeMetricSamples(now.Add(-metricExpire))
	o.historyExpiration(now.Add(-snapshotExpire))
}

// scrapeMetricSamples observes the metrics, and updates the history.
func (o *sampler) scrapeMetricSamples(expireAt time.Time) {
	getGlobalObserver().ExpireAndObserve(func(label SegmentLabel, snapshot SegmentObserverSnapshot) {
		if _, ok := o.history[label]; !ok {
			o.history[label] = newSegmentHistory()
		}
		o.history[label].Append(snapshot)
	}, expireAt)
}

// historyExpiration shrinks the snapshots.
func (o *sampler) historyExpiration(expireAt time.Time) {
	for label, h := range o.history {
		h.Shrink(expireAt)
		if h.Len() == 0 {
			delete(o.history, label)
		}
	}
}

// getTestSegmentHistory returns a test segment history.
func getTestSegmentHistory() *segmentHistory {
	testSegmentHistory := newSegmentHistory()
	testSegmentHistory.Append(SegmentObserverSnapshot{
		Time: time.Now(),
	})
	testSegmentHistory.Append(SegmentObserverSnapshot{
		Time: time.Now(),
	})
	return testSegmentHistory
}
