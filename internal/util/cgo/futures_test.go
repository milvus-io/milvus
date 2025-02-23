package cgo

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	initCGO()
	exitCode := m.Run()
	if exitCode > 0 {
		os.Exit(exitCode)
	}
}

func TestFutureWithConcurrentReleaseAndCancel(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		future := createFutureWithTestCase(context.Background(), testCase{
			interval: 100 * time.Millisecond,
			loopCnt:  10,
			caseNo:   100,
		})
		wg.Add(3)
		// Double release should be ok.
		go func() {
			defer wg.Done()
			future.Release()
		}()
		go func() {
			defer wg.Done()
			future.Release()
		}()
		go func() {
			defer wg.Done()
			future.cancel(context.DeadlineExceeded)
		}()
	}
	wg.Wait()
}

func TestFutureWithSuccessCase(t *testing.T) {
	// Test success case.
	future := createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   100,
	})
	defer future.Release()

	start := time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err := future.BlockAndLeakyGet()
	assert.NoError(t, err)
	assert.Equal(t, 100, getCInt(result))
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.
	freeCInt(result)
	runtime.GC()

	_, err = future.BlockAndLeakyGet()
	assert.ErrorIs(t, err, ErrConsumed)
}

func TestFutureWithCaseNoInterrupt(t *testing.T) {
	// Test success case.
	future := createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoNoInterrupt,
	})
	defer future.Release()

	start := time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err := future.BlockAndLeakyGet()
	assert.NoError(t, err)
	assert.Equal(t, 0, getCInt(result))
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.
	freeCInt(result)

	// Test cancellation on no interrupt handling case.
	start = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	future = createFutureWithTestCase(ctx, testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  20,
		caseNo:   caseNoNoInterrupt,
	})
	defer future.Release()

	result, err = future.BlockAndLeakyGet()
	// the future is timeout by the context after 200ms, but the underlying task doesn't handle the cancel, the future will return after 2s.
	assert.Greater(t, time.Since(start).Seconds(), 2.0)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 0, getCInt(result))
	freeCInt(result)
}

// TestFutures test the future implementation.
func TestFutures(t *testing.T) {
	// Test failed case, throw folly exception.
	future := createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoThrowStdException,
	})
	defer future.Release()

	start := time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err := future.BlockAndLeakyGet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreUnsupported)
	assert.Nil(t, result)
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)

	// Test failed case, throw std exception.
	future = createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoThrowFollyException,
	})
	defer future.Release()
	start = time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err = future.BlockAndLeakyGet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyOtherException)
	assert.Nil(t, result)
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.

	// Test failed case, throw std exception.
	future = createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoThrowSegcoreException,
	})
	defer future.Release()
	start = time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err = future.BlockAndLeakyGet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcorePretendFinished)
	assert.Nil(t, result)
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.

	// Test cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	future = createFutureWithTestCase(ctx, testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  20,
		caseNo:   100,
	})
	defer future.Release()
	// canceled before the future(2s) is ready.
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start = time.Now()
	result, err = future.BlockAndLeakyGet()
	// the future is canceled by the context after 200ms, so the future should be done in 1s but not 2s.
	assert.Less(t, time.Since(start).Seconds(), 1.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.Nil(t, result)

	// Test cancellation.
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	future = createFutureWithTestCase(ctx, testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  20,
		caseNo:   100,
	})
	defer future.Release()
	start = time.Now()
	result, err = future.BlockAndLeakyGet()
	// the future is timeout by the context after 200ms, so the future should be done in 1s but not 2s.
	assert.Less(t, time.Since(start).Seconds(), 1.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Nil(t, result)
	runtime.GC()
}

func TestConcurrent(t *testing.T) {
	// Test is compatible with old implementation of fast fail future.
	// So it's complicated and not easy to understand.
	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(4)
		// success case
		go func() {
			defer wg.Done()
			// Test success case.
			future := createFutureWithTestCase(context.Background(), testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  10,
				caseNo:   100,
			})
			defer future.Release()
			result, err := future.BlockAndLeakyGet()
			assert.NoError(t, err)
			assert.Equal(t, 100, getCInt(result))
			freeCInt(result)
		}()

		// fail case
		go func() {
			defer wg.Done()
			// Test success case.
			future := createFutureWithTestCase(context.Background(), testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  10,
				caseNo:   caseNoThrowStdException,
			})
			defer future.Release()
			result, err := future.BlockAndLeakyGet()
			assert.Error(t, err)
			assert.Nil(t, result)
		}()

		// timeout case
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			future := createFutureWithTestCase(ctx, testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  20,
				caseNo:   100,
			})
			defer future.Release()
			result, err := future.BlockAndLeakyGet()
			assert.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
			assert.True(t, errors.Is(err, context.DeadlineExceeded))
			assert.Nil(t, result)
		}()

		// no interrupt with timeout case
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			future := createFutureWithTestCase(ctx, testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  10,
				caseNo:   caseNoNoInterrupt,
			})
			defer future.Release()
			result, err := future.BlockAndLeakyGet()
			if err == nil {
				assert.Equal(t, 0, getCInt(result))
			} else {
				// the future may be queued and not started,
				// so the underlying task may be throw a cancel exception if it's not started.
				assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
				assert.True(t, errors.Is(err, context.DeadlineExceeded))
			}
			freeCInt(result)
		}()
	}
	wg.Wait()
	assert.Eventually(t, func() bool {
		stat := futureManager.Stat()
		fmt.Printf("active count: %d\n", stat.ActiveCount)
		return stat.ActiveCount == 0
	}, 5*time.Second, 100*time.Millisecond)
	runtime.GC()
}
