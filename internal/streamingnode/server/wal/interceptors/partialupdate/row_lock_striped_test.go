package partialupdate

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test probes pin a table until test cleanup. A mutex pointer without a table
// reference could point into a recycled table after the last production release.
type pkLockKey struct {
	vchannel    string
	kind        primaryKeyKind
	int64Value  int64
	stringValue string
}

func testPKMutex(t *testing.T, manager *rowLockManager, key pkLockKey) *sync.RWMutex {
	t.Helper()
	table := manager.pin(key.vchannel)
	t.Cleanup(func() { manager.unpin(key.vchannel, table) })
	stripe := int64RowLockStripe(key.int64Value)
	if key.kind == primaryKeyKindString {
		stripe = stringRowLockStripe(key.stringValue)
	}
	return &table.stripes[stripe]
}

func collidingInt64PK(t *testing.T, value int64) int64 {
	t.Helper()
	stripe := int64RowLockStripe(value)
	for candidate := int64(0); candidate < 1_000_000; candidate++ {
		if candidate != value && int64RowLockStripe(candidate) == stripe {
			return candidate
		}
	}
	t.Fatal("could not find a colliding PK")
	return 0
}

func TestStripedRowLocksCollidingPKsShareExclusion(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		t.Run(fmt.Sprint(ordinary), func(t *testing.T) {
			manager := newRowLockManager()
			collision := collidingInt64PK(t, 10)
			holder := acquireTestRowLock(t, manager, "v1", primaryKeys{int64Values: []int64{10}}, !ordinary)
			waiter := startTestRowLock(context.Background(), manager, "v1", primaryKeys{int64Values: []int64{collision}}, true)
			require.Eventually(t, func() bool {
				manager.mu.Lock()
				defer manager.mu.Unlock()
				return manager.channels["v1"].refs >= 2
			}, time.Second, time.Millisecond)
			waitForTestPKWriter(t, manager, "v1", collision)
			// The same stripe number in another vchannel has its own mutex.
			other := acquireTestRowLock(t, manager, "v2", primaryKeys{int64Values: []int64{collision}}, true)
			other()
			select {
			case result := <-waiter:
				if result.release != nil {
					result.release()
				}
				t.Fatal("colliding PK bypassed the held stripe")
			default:
			}
			holder()
			acquired := awaitTestRowLock(t, waiter)
			require.NoError(t, acquired.err)
			acquired.release()
		})
	}
}

func TestStripedRowLocksCollisionDeduplicatesReaderBeforeQueuedWriter(t *testing.T) {
	manager := newRowLockManager()
	collision := collidingInt64PK(t, 10)
	ctx := &pauseAfterFirstPKContext{
		Context: context.Background(), manager: manager, t: t,
		key:    pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10},
		locked: make(chan struct{}), resume: make(chan struct{}),
	}
	var once sync.Once
	resume := func() { once.Do(func() { close(ctx.resume) }) }
	t.Cleanup(resume)
	reader := startTestRowLock(ctx, manager, "v1", primaryKeys{int64Values: []int64{collision, 10, collision}}, false)
	select {
	case <-ctx.locked:
	case <-time.After(time.Second):
		t.Fatal("reader did not acquire the colliding stripe")
	}
	writer := startTestRowLock(context.Background(), manager, "v1", primaryKeys{int64Values: []int64{collision}}, true)
	waitForTestPKWriter(t, manager, "v1", collision)
	resume()
	// A second RLock of the collision stripe would wait behind the queued
	// writer while still holding the first RLock and could never finish.
	acquired := awaitTestRowLock(t, reader)
	require.NoError(t, acquired.err)
	acquired.release()
	acquired = awaitTestRowLock(t, writer)
	require.NoError(t, acquired.err)
	acquired.release()
}

func TestStripedRowLocksStringFastPathMatchesMixedWriteSet(t *testing.T) {
	manager := newRowLockManager()
	var collision string
	for value := int64(0); value < 1_000_000; value++ {
		candidate := fmt.Sprint(value)
		if stringRowLockStripe(candidate) == int64RowLockStripe(10) {
			collision = candidate
			break
		}
	}
	require.NotEmpty(t, collision)
	holder := acquireTestRowLock(t, manager, "v1", primaryKeys{stringValues: []string{collision}}, false)
	waiter := startTestRowLock(context.Background(), manager, "v1", primaryKeys{
		int64Values: []int64{10}, stringValues: []string{collision, collision},
	}, true)
	waitForTestPKWriter(t, manager, "v1", 10)
	select {
	case result := <-waiter:
		if result.release != nil {
			result.release()
		}
		t.Fatal("mixed set bypassed the string row's shared stripe")
	default:
	}
	holder()
	result := awaitTestRowLock(t, waiter)
	require.NoError(t, result.err)
	result.release()
}

func TestStripedRowLocksLargeMixedSetsUseOneOrder(t *testing.T) {
	manager := newRowLockManager()
	var ascending, descending primaryKeys
	for value := int64(0); value < 10_000; value++ {
		ascending.int64Values = append(ascending.int64Values, value, value)
		ascending.stringValues = append(ascending.stringValues, fmt.Sprint(value))
		descending.int64Values = append(descending.int64Values, 9_999-value)
		descending.stringValues = append(descending.stringValues, fmt.Sprint(9_999-value))
	}
	start := make(chan struct{})
	done := make(chan error, 2)
	for _, pks := range []primaryKeys{ascending, descending} {
		go func(pks primaryKeys) {
			<-start
			for attempt := 0; attempt < 5; attempt++ {
				release, err := manager.acquire(context.Background(), "v1", pks, true)
				if err != nil {
					done <- err
					return
				}
				runtime.Gosched()
				release()
			}
			done <- nil
		}(pks)
	}
	close(start)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("overlapping hash stripes deadlocked for reversed input order")
		}
	}
	manager.mu.Lock()
	require.Empty(t, manager.channels)
	manager.mu.Unlock()
}

func TestStripedRowLocksWaiterPinsTableUntilLastRelease(t *testing.T) {
	manager := newRowLockManager()
	pks := primaryKeys{int64Values: []int64{10}}
	holder, err := manager.acquire(context.Background(), "v1", pks, true)
	require.NoError(t, err)
	manager.mu.Lock()
	original := manager.channels["v1"]
	require.Equal(t, 1, original.refs)
	manager.mu.Unlock()
	waiter := startTestRowLock(context.Background(), manager, "v1", pks, false)
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.channels["v1"] == original && original.refs == 2
	}, time.Second, time.Millisecond)
	holder()
	acquired := awaitTestRowLock(t, waiter)
	require.NoError(t, acquired.err)
	manager.mu.Lock()
	require.Same(t, original, manager.channels["v1"])
	require.Equal(t, 1, original.refs)
	manager.mu.Unlock()
	acquired.release()
	manager.mu.Lock()
	require.Empty(t, manager.channels)
	manager.mu.Unlock()
	// Clearing a pool does not affect active tables or future lock ownership.
	runtime.GC()
	again, err := manager.acquire(context.Background(), "v1", pks, true)
	require.NoError(t, err)
	runtime.GC()
	again()
	manager.mu.Lock()
	require.Empty(t, manager.channels)
	manager.mu.Unlock()
}

func TestStripedRowLocksCanceledWaiterReleasesTable(t *testing.T) {
	manager := newRowLockManager()
	pks := primaryKeys{int64Values: []int64{10}}
	holder, err := manager.acquire(context.Background(), "v1", pks, true)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	waiter := startTestRowLock(ctx, manager, "v1", pks, false)
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.channels["v1"].refs == 2
	}, time.Second, time.Millisecond)
	cancel()
	holder()
	result := awaitTestRowLock(t, waiter)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Nil(t, result.release)
	manager.mu.Lock()
	require.Empty(t, manager.channels)
	manager.mu.Unlock()
}

func BenchmarkRowLockManagerStriped(b *testing.B) {
	for _, size := range []int{1, 1_000, 10_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			manager := newRowLockManager()
			pks := primaryKeys{int64Values: make([]int64, size)}
			for index := range pks.int64Values {
				pks.int64Values[index] = int64(index)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				release, err := manager.acquire(context.Background(), "v1", pks, false)
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
	}
}

func BenchmarkRowLockManagerStripedSharedParallel(b *testing.B) {
	for _, size := range []int{1, 1_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			manager := newRowLockManager()
			pks := primaryKeys{int64Values: make([]int64, size)}
			for index := range pks.int64Values {
				pks.int64Values[index] = int64(index)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					release, err := manager.acquire(context.Background(), "v1", pks, false)
					if err != nil {
						panic(err)
					}
					release()
				}
			})
		})
	}
}
