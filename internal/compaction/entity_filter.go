package compaction

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type EntityFilter interface {
	Filtered(pk any, ts typeutil.Timestamp) bool

	GetExpiredCount() int
	GetDeletedCount() int
	GetDeltalogDeleteCount() int
	GetMissingDeleteCount() int
}

func NewEntityFilter(deletedPkTs map[interface{}]typeutil.Timestamp, ttl int64, currTime time.Time) EntityFilter {
	return newEntityFilter(deletedPkTs, ttl, currTime)
}

type EntityFilterImpl struct {
	deletedPkTs map[interface{}]typeutil.Timestamp // pk2ts
	ttl         int64                              // nanoseconds
	currentTime time.Time

	expiredCount int
	deletedCount int
}

func newEntityFilter(deletedPkTs map[interface{}]typeutil.Timestamp, ttl int64, currTime time.Time) *EntityFilterImpl {
	if deletedPkTs == nil {
		deletedPkTs = make(map[interface{}]typeutil.Timestamp)
	}
	return &EntityFilterImpl{
		deletedPkTs: deletedPkTs,
		ttl:         ttl,
		currentTime: currTime,
	}
}

func (filter *EntityFilterImpl) Filtered(pk any, ts typeutil.Timestamp) bool {
	if filter.isEntityDeleted(pk, ts) {
		filter.deletedCount++
		return true
	}

	// Filtering expired entity
	if filter.isEntityExpired(ts) {
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
	entityTime, _ := tsoutil.ParseTS(entityTs)

	// this dur can represents 292 million years before or after 1970, enough for milvus
	// ttl calculation
	dur := filter.currentTime.UnixMilli() - entityTime.UnixMilli()

	// filter.ttl is nanoseconds
	return filter.ttl/int64(time.Millisecond) <= dur
}
