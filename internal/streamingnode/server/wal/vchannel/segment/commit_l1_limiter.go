package segment

import (
	"context"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type commitL1Limiter struct {
	mu        sync.RWMutex
	semaphore *syncutil.Semaphore
}

func newCommitL1Limiter(concurrency int) *commitL1Limiter {
	limiter := &commitL1Limiter{}
	limiter.UpdateConcurrency(concurrency)
	return limiter
}

func (l *commitL1Limiter) Acquire(ctx context.Context) (func(), error) {
	l.mu.RLock()
	semaphore := l.semaphore
	l.mu.RUnlock()
	if semaphore == nil {
		return func() {}, nil
	}
	if err := semaphore.Acquire(ctx); err != nil {
		return nil, err
	}
	return semaphore.Release, nil
}

func (l *commitL1Limiter) UpdateConcurrency(concurrency int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if concurrency <= 0 {
		if l.semaphore != nil {
			l.semaphore.SetCapacity(maxInt())
		}
		l.semaphore = nil
		return
	}
	if l.semaphore == nil {
		l.semaphore = syncutil.NewSemaphore(concurrency)
		return
	}
	l.semaphore.SetCapacity(concurrency)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

type commitL1LimiterRegistry struct {
	once    sync.Once
	limiter *commitL1Limiter
}

var globalCommitL1LimiterRegistry = &commitL1LimiterRegistry{}

func getDynamicCommitL1Limiter(param *paramtable.ParamItem) *commitL1Limiter {
	globalCommitL1LimiterRegistry.once.Do(func() {
		globalCommitL1LimiterRegistry.limiter = newCommitL1Limiter(param.GetAsInt())
		param.RegisterCallback(func(_ context.Context, _, _, newValue string) error {
			concurrency, err := strconv.Atoi(newValue)
			if err != nil {
				return err
			}
			globalCommitL1LimiterRegistry.limiter.UpdateConcurrency(concurrency)
			return nil
		})
	})
	return globalCommitL1LimiterRegistry.limiter
}
