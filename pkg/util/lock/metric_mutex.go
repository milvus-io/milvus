package lock

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type MetricsLockManager struct {
	rwLocks map[string]*MetricsRWMutex
}

type MetricsRWMutex struct {
	mutex          sync.RWMutex
	lockName       string
	acquireTimeMap map[string]time.Time
}

const (
	readLock  = "READ_LOCK"
	writeLock = "WRITE_LOCK"
	hold      = "HOLD"
	acquire   = "ACQUIRE"
)

func (mRWLock *MetricsRWMutex) RLock(source string) {
	if paramtable.Get().CommonCfg.EnableLockMetrics.GetAsBool() {
		before := time.Now()
		mRWLock.mutex.RLock()
		mRWLock.acquireTimeMap[source] = time.Now()
		logLock(time.Since(before), mRWLock.lockName, source, readLock, acquire)
	} else {
		mRWLock.mutex.RLock()
	}
}

func (mRWLock *MetricsRWMutex) Lock(source string) {
	if paramtable.Get().CommonCfg.EnableLockMetrics.GetAsBool() {
		before := time.Now()
		mRWLock.mutex.Lock()
		mRWLock.acquireTimeMap[source] = time.Now()
		logLock(time.Since(before), mRWLock.lockName, source, writeLock, acquire)
	} else {
		mRWLock.mutex.Lock()
	}
}

func (mRWLock *MetricsRWMutex) UnLock(source string) {
	if mRWLock.maybeLogUnlockDuration(source, writeLock) != nil {
		return
	}
	mRWLock.mutex.Unlock()
}

func (mRWLock *MetricsRWMutex) RUnLock(source string) {
	if mRWLock.maybeLogUnlockDuration(source, readLock) != nil {
		return
	}
	mRWLock.mutex.RUnlock()
}

func (mRWLock *MetricsRWMutex) maybeLogUnlockDuration(source string, lockType string) error {
	if paramtable.Get().CommonCfg.EnableLockMetrics.GetAsBool() {
		acquireTime, ok := mRWLock.acquireTimeMap[source]
		if ok {
			logLock(time.Since(acquireTime), mRWLock.lockName, source, lockType, hold)
			delete(mRWLock.acquireTimeMap, source)
		} else {
			mlog.Error(context.TODO(), "there's no lock history for the source, there may be some defects in codes",
				mlog.String("source", source))
			return merr.WrapErrParameterInvalidMsg("unknown source")
		}
	}
	return nil
}

func logLock(duration time.Duration, lockName string, source string, lockType string, opType string) {
	if duration >= paramtable.Get().CommonCfg.LockSlowLogWarnThreshold.GetAsDuration(time.Millisecond) {
		mlog.Warn(context.TODO(), "lock takes too long", mlog.String("lockName", lockName), mlog.String("lockType", lockType),
			mlog.String("source", source), mlog.String("opType", opType),
			mlog.Duration("time_cost", duration))
	} else if duration >= paramtable.Get().CommonCfg.LockSlowLogInfoThreshold.GetAsDuration(time.Millisecond) {
		mlog.Info(context.TODO(), "lock takes too long", mlog.String("lockName", lockName), mlog.String("lockType", lockType),
			mlog.String("source", source), mlog.String("opType", opType),
			mlog.Duration("time_cost", duration))
	}
	metrics.LockCosts.WithLabelValues(lockName, source, lockType, opType).Set(float64(duration.Milliseconds()))
}

// currently, we keep metricsLockManager as a communal gate for metrics lock
// we may use this manager as a centralized statistical site to provide overall cost for locks
func (mlManager *MetricsLockManager) applyRWLock(name string) *MetricsRWMutex {
	return &MetricsRWMutex{
		lockName:       name,
		acquireTimeMap: make(map[string]time.Time, 0),
	}
}
