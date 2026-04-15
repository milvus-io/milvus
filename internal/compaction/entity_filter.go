package compaction

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type EntityFilter interface {
	Filtered(pk any, ts typeutil.Timestamp, expirationTimeMicros int64) bool

	GetExpiredCount() int
	GetDeletedCount() int
	GetDeltalogDeleteCount() int
	GetMissingDeleteCount() int
}

func NewEntityFilter(deletedPkTs map[interface{}]typeutil.Timestamp, ttl int64, currTime time.Time, commitTs typeutil.Timestamp) EntityFilter {
	return newEntityFilter(deletedPkTs, ttl, currTime, commitTs)
}

type EntityFilterImpl struct {
	deletedPkTs map[interface{}]typeutil.Timestamp // pk2ts
	ttl         int64                              // nanoseconds
	currentTime time.Time
	// commitTs is SegmentInfo.commit_timestamp for import/CDC segments.
	// When non-zero, row timestamps in binlogs are stale (they predate the
	// actual write time). isEntityExpired uses max(row_ts, commitTs) so that
	// no row is prematurely expired due to a stale timestamp.
	commitTs typeutil.Timestamp

	expiredCount int
	deletedCount int
}

func newEntityFilter(deletedPkTs map[interface{}]typeutil.Timestamp, ttl int64, currTime time.Time, commitTs typeutil.Timestamp) *EntityFilterImpl {
	if deletedPkTs == nil {
		deletedPkTs = make(map[interface{}]typeutil.Timestamp)
	}
	return &EntityFilterImpl{
		deletedPkTs: deletedPkTs,
		ttl:         ttl,
		currentTime: currTime,
		commitTs:    commitTs,
	}
}

func (filter *EntityFilterImpl) Filtered(pk any, ts typeutil.Timestamp, expirationTimeMicros int64) bool {
	if filter.isEntityDeleted(pk, ts) {
		filter.deletedCount++
		return true
	}

	// Filtering expired entity
	if filter.isEntityExpired(ts) {
		filter.expiredCount++
		return true
	}

	if filter.isEntityExpiredByTTLField(expirationTimeMicros) {
		filter.expiredCount++
		return true
	}
	return false
}

func (filter *EntityFilterImpl) GetExpiredCount() int {
	return filter.expiredCount
}

func (filter *EntityFilterImpl) GetDeletedCount() int {
	return filter.deletedCount
}

func (filter *EntityFilterImpl) GetDeltalogDeleteCount() int {
	return len(filter.deletedPkTs)
}

func (filter *EntityFilterImpl) GetMissingDeleteCount() int {
	diff := filter.GetDeltalogDeleteCount() - filter.GetDeletedCount()
	if diff <= 0 {
		diff = 0
	}
	return diff
}

func (filter *EntityFilterImpl) isEntityDeleted(pk interface{}, pkTs typeutil.Timestamp) bool {
	if deleteTs, ok := filter.deletedPkTs[pk]; ok {
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if pkTs < deleteTs {
			return true
		}
	}
	return false
}

func (filter *EntityFilterImpl) isEntityExpired(entityTs typeutil.Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if filter.ttl <= 0 {
		return false
	}

	// For import/CDC segments, row timestamps in binlogs may predate the actual
	// commit time.  Use whichever is larger so a row is never marked expired
	// due to an outdated timestamp alone.
	entityTime, _ := tsoutil.ParseTS(tsoutil.EffectiveTimestamp(entityTs, filter.commitTs))

	// this dur can represents 292 million years before or after 1970, enough for milvus
	// ttl calculation
	dur := filter.currentTime.UnixMilli() - entityTime.UnixMilli()

	// filter.ttl is nanoseconds
	return filter.ttl/int64(time.Millisecond) <= dur
}

func (filter *EntityFilterImpl) isEntityExpiredByTTLField(expirationTimeMicros int64) bool {
	// entity expire is not enabled if expirationTimeMicros < 0
	if expirationTimeMicros < 0 {
		return false
	}

	// entityExpireTs is microseconds
	return filter.currentTime.UnixMicro() >= expirationTimeMicros
}
