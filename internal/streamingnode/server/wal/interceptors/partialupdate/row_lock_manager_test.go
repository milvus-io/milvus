package partialupdate

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func int64TestKeys(values ...int64) primaryKeys {
	return primaryKeys{kind: primaryKeyKindInt64, int64Values: values}
}

func rangeTestKeys(from, to int64, extra ...int64) primaryKeys {
	keys := int64TestKeys()
	for value := from; value < to; value++ {
		keys.int64Values = append(keys.int64Values, value)
	}
	keys.int64Values = append(keys.int64Values, extra...)
	return keys
}

func TestRowLockManagerOrdinaryWritesWithSamePKsNeverBlock(t *testing.T) {
	g := newRowLockManager()
	pks := primaryKeys{int64Values: []int64{10, 20, 10}, stringValues: []string{"a"}}
	var results []<-chan testRowLockResult
	for range 8 {
		results = append(results, startTestRowLock(context.Background(), g, "v1", pks, false))
	}
	var releases []func()
	for _, result := range results {
		acquired := awaitTestRowLock(t, result)
		require.NoError(t, acquired.err)
		releases = append(releases, acquired.release)
	}
	// All eight writes hold the PKs at the same time.
	require.Equal(t, testPKLock{ordinary: 8}, inspectTestPK(g, "v1", int64(10)))
	require.Equal(t, testPKLock{ordinary: 8}, inspectTestPK(g, "v1", "a"))
	for _, release := range releases {
		release()
	}
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerHeldCASBlocksOnlyOverlappingOrdinaryWrites(t *testing.T) {
	g := newRowLockManager()
	cas := acquireTestRowLock(t, g, "v1", int64TestKeys(10), true)
	overlapping := startTestRowLock(context.Background(), g, "v1", int64TestKeys(5, 10), false)
	waitForTestPKOrdinary(t, g, "v1", 5, 1)
	requireTestRowLockPending(t, overlapping)
	// Ordinary writes never wait for each other, so a write that shares PK 5
	// with the waiting write but not PK 10 with the CAS proceeds.
	disjoint := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1", int64TestKeys(5, 30), false))
	require.NoError(t, disjoint.err)
	disjoint.release()
	// A CAS on PK 5 arrives after the waiting write, so it waits for that write.
	later := startTestRowLock(context.Background(), g, "v1", int64TestKeys(5), true)
	waitForTestPKAwaited(t, g, "v1", 5)
	requireTestRowLockPending(t, later)
	requireTestRowLockPending(t, overlapping)
	cas()
	acquired := awaitTestRowLock(t, overlapping)
	require.NoError(t, acquired.err)
	// The write holds PK 10 and the later CAS still waits for it.
	require.Equal(t, testPKLock{ordinary: 1, awaited: 1}, inspectTestPK(g, "v1", int64(10)))
	requireTestRowLockPending(t, later)
	acquired.release()
	laterAcquired := awaitTestRowLock(t, later)
	require.NoError(t, laterAcquired.err)
	laterAcquired.release()
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerCASWaitsOnlyForEarlierOverlappingOrdinaryWrites(t *testing.T) {
	g := newRowLockManager()
	// A huge earlier write that does not contain PK -1 never delays its CAS.
	huge := acquireTestRowLock(t, g, "v1", rangeTestKeys(0, 100_000), false)
	disjoint := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1", int64TestKeys(-1), true))
	require.NoError(t, disjoint.err)
	disjoint.release()

	// An earlier write with duplicate PKs that contains PK 200_020 delays the CAS.
	earlier := acquireTestRowLock(t, g, "v1", int64TestKeys(200_020, 200_010, 200_020), false)
	casResult := startTestRowLock(context.Background(), g, "v1", int64TestKeys(200_020), true)
	waitForTestPKAwaited(t, g, "v1", 200_020)
	requireTestRowLockPending(t, casResult)
	// A write arriving after the CAS waits for it, even while the CAS itself
	// still waits for the earlier write.
	later := startTestRowLock(context.Background(), g, "v1", int64TestKeys(200_020), false)
	waitForTestPKOrdinary(t, g, "v1", 200_020, 2)
	requireTestRowLockPending(t, later)
	require.Equal(t, testPKLock{queued: true, ordinary: 2, awaited: 1}, inspectTestPK(g, "v1", int64(200_020)))
	earlier()
	cas := awaitTestRowLock(t, casResult)
	require.NoError(t, cas.err)
	requireTestRowLockPending(t, later)
	cas.release()
	acquired := awaitTestRowLock(t, later)
	require.NoError(t, acquired.err)
	acquired.release()

	// The huge write blocks a CAS only when it contains the CAS PK.
	casResult = startTestRowLock(context.Background(), g, "v1", int64TestKeys(99_999, -2), true)
	waitForTestPKAwaited(t, g, "v1", 99_999)
	requireTestRowLockPending(t, casResult)
	huge()
	cas = awaitTestRowLock(t, casResult)
	require.NoError(t, cas.err)
	cas.release()
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerOverlappingCASRunInRegistrationOrder(t *testing.T) {
	g := newRowLockManager()
	first := acquireTestRowLock(t, g, "v1", int64TestKeys(10, 20), true)
	second := startTestRowLock(context.Background(), g, "v1", int64TestKeys(30, 20), true)
	waitForTestPKQueued(t, g, "v1", 30)
	requireTestRowLockPending(t, second)
	// third shares only PK 30 with second, so it waits for second and, through
	// it, for first.
	third := startTestRowLock(context.Background(), g, "v1", int64TestKeys(30, 31), true)
	waitForTestPKQueued(t, g, "v1", 31)
	requireTestRowLockPending(t, third)
	// Disjoint CAS commits proceed at once, and duplicate PKs do not make a
	// CAS wait for itself.
	disjoint := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1", int64TestKeys(40, 50), true))
	require.NoError(t, disjoint.err)
	duplicates := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1", int64TestKeys(60, 60, 60), true))
	require.NoError(t, duplicates.err, "duplicate PKs deadlocked one CAS")
	require.True(t, inspectTestPK(g, "v1", int64(60)).queued)
	ordinary := startTestRowLock(context.Background(), g, "v1", int64TestKeys(60), false)
	waitForTestPKOrdinary(t, g, "v1", 60, 1)
	requireTestRowLockPending(t, ordinary)
	duplicates.release()
	acquired := awaitTestRowLock(t, ordinary)
	require.NoError(t, acquired.err)
	acquired.release()
	disjoint.release()
	requireTestRowLockPending(t, second)
	requireTestRowLockPending(t, third)

	first()
	secondAcquired := awaitTestRowLock(t, second)
	require.NoError(t, secondAcquired.err)
	requireTestRowLockPending(t, third)
	// first left PK 20 to second, so an ordinary write of PK 20 now waits for
	// second.
	ordinary = startTestRowLock(context.Background(), g, "v1", int64TestKeys(20), false)
	waitForTestPKOrdinary(t, g, "v1", 20, 1)
	requireTestRowLockPending(t, ordinary)
	secondAcquired.release()
	for _, result := range []<-chan testRowLockResult{third, ordinary} {
		acquired := awaitTestRowLock(t, result)
		require.NoError(t, acquired.err)
		acquired.release()
	}
	requireTestRowLockTablesEmpty(t, g)
}

// A waiting large write L directly delays only later writes that share one of
// its PKs; a CAS disjoint from L and from C1 proceeds at once, and L runs in
// its turn.
func TestRowLockManagerWaitingLargeWriteDelaysOnlyOverlappingCAS(t *testing.T) {
	const a, b, c = int64(7), int64(9_000), int64(-1)
	g := newRowLockManager()
	c1 := acquireTestRowLock(t, g, "v1", int64TestKeys(a), true)
	large := startTestRowLock(context.Background(), g, "v1", rangeTestKeys(0, 10_000), false)
	waitForTestPKOrdinary(t, g, "v1", b, 1)
	requireTestRowLockPending(t, large)
	c2 := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1", int64TestKeys(c), true))
	require.NoError(t, c2.err, "a CAS disjoint from both C1 and L waited")
	c2.release()
	// C3 arrives after L and shares PK b with it, so it waits for L.
	c3 := startTestRowLock(context.Background(), g, "v1", int64TestKeys(b), true)
	waitForTestPKAwaited(t, g, "v1", b)
	requireTestRowLockPending(t, c3)
	requireTestRowLockPending(t, large)
	c1()
	acquired := awaitTestRowLock(t, large)
	require.NoError(t, acquired.err)
	requireTestRowLockPending(t, c3)
	acquired.release()
	c3Acquired := awaitTestRowLock(t, c3)
	require.NoError(t, c3Acquired.err)
	require.Equal(t, testPKLock{queued: true}, inspectTestPK(g, "v1", b))
	c3Acquired.release()
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerInt64AndVarCharPKsDoNotAlias(t *testing.T) {
	g := newRowLockManager()
	intCAS := acquireTestRowLock(t, g, "v1", int64TestKeys(10), true)
	stringWrite := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1",
		primaryKeys{kind: primaryKeyKindString, stringValues: []string{"10"}}, false))
	require.NoError(t, stringWrite.err)
	stringWrite.release()
	stringCAS := awaitTestRowLock(t, startTestRowLock(context.Background(), g, "v1",
		primaryKeys{kind: primaryKeyKindString, stringValues: []string{"10"}}, true))
	require.NoError(t, stringCAS.err)
	require.True(t, inspectTestPK(g, "v1", "10").queued)
	require.True(t, inspectTestPK(g, "v1", int64(10)).queued)
	// A mixed write set waits for the CAS commits of both types.
	mixed := startTestRowLock(context.Background(), g, "v1",
		primaryKeys{kind: primaryKeyKindMixed, int64Values: []int64{10}, stringValues: []string{"10"}}, false)
	waitForTestPKOrdinary(t, g, "v1", 10, 1)
	intCAS()
	requireTestRowLockPending(t, mixed)
	stringCAS.release()
	acquired := awaitTestRowLock(t, mixed)
	require.NoError(t, acquired.err)
	acquired.release()
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerCancellationLeavesNoLockBehind(t *testing.T) {
	cases := []struct {
		name string
		// The blocker holds PK 10 in this mode; the canceled waiter uses mode.
		blockerExclusive, exclusive bool
		waiting                     func(t *testing.T, g *rowLockManager)
		// keepsPlace means the canceled waiter stays in line on PKs 20 and 30
		// until the blocker releases.
		keepsPlace bool
		// blocked is the state of PK 10 after the canceled waiter returns.
		blocked testPKLock
	}{
		{
			name:             "ordinary_waits_for_cas",
			blockerExclusive: true,
			waiting:          func(t *testing.T, g *rowLockManager) { waitForTestPKOrdinary(t, g, "v1", 20, 1) },
			blocked:          testPKLock{queued: true},
		},
		{
			name:             "cas_waits_for_cas",
			blockerExclusive: true,
			exclusive:        true,
			waiting:          func(t *testing.T, g *rowLockManager) { waitForTestPKQueued(t, g, "v1", 20) },
			keepsPlace:       true,
			blocked:          testPKLock{queued: true},
		},
		{
			name:      "cas_waits_for_ordinary",
			exclusive: true,
			waiting:   func(t *testing.T, g *rowLockManager) { waitForTestPKAwaited(t, g, "v1", 10) },
			blocked:   testPKLock{ordinary: 1, awaited: 1},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRowLockManager()
			// An unrelated write pins the table for the whole case, so an entry
			// left behind cannot disappear with the table.
			pin := acquireTestRowLock(t, g, "v1", int64TestKeys(999), false)
			blocker := acquireTestRowLock(t, g, "v1", int64TestKeys(10), c.blockerExclusive)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := startTestRowLock(ctx, g, "v1", int64TestKeys(20, 10, 30), c.exclusive)
			c.waiting(t, g)
			var later <-chan testRowLockResult
			if c.exclusive && !c.blockerExclusive {
				// A write that waits for the canceled CAS is woken.
				later = startTestRowLock(context.Background(), g, "v1", int64TestKeys(30), false)
				waitForTestPKOrdinary(t, g, "v1", 30, 1)
				requireTestRowLockPending(t, later)
			}
			// Waits are cancellable while the blocker is still held.
			cancel()
			acquired := awaitTestRowLock(t, result)
			require.ErrorIs(t, acquired.err, context.Canceled)
			require.Nil(t, acquired.release)
			if later != nil {
				laterAcquired := awaitTestRowLock(t, later)
				require.NoError(t, laterAcquired.err)
				laterAcquired.release()
			}
			if c.keepsPlace {
				require.Equal(t, testPKLock{queued: true}, inspectTestPK(g, "v1", int64(20)))
				require.Equal(t, testPKLock{queued: true}, inspectTestPK(g, "v1", int64(30)))
				require.Equal(t, 3, testRowLockRefs(g, "v1"))
			} else {
				requireTestRowLocksAvailable(t, g, "v1", 20, 30)
				require.Equal(t, 2, testRowLockRefs(g, "v1"))
			}
			require.Equal(t, c.blocked, inspectTestPK(g, "v1", int64(10)))
			blocker()
			// A canceled CAS that kept its place gives it up from another
			// goroutine. Once only the pin is left, no PK of the canceled waiter
			// may still have an entry, and the empty CAS map must be dropped.
			waitForTestRowLockRefs(t, g, "v1", 1)
			requireTestRowLocksAvailable(t, g, "v1", 10, 20, 30)
			int64Map, _ := testCASMapsAllocated(g, "v1")
			require.False(t, int64Map, "the empty int64 CAS map was not dropped")
			pin()
			requireTestRowLockTablesEmpty(t, g)
		})
	}

	g := newRowLockManager()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, exclusive := range []bool{false, true} {
		release, err := g.acquire(ctx, "v1", int64TestKeys(10), exclusive)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, release)
	}
	release, err := g.acquire(ctx, "v1", primaryKeys{}, true)
	require.NoError(t, err, "an empty write set takes no lock and ignores cancellation")
	release()
	requireTestRowLockTablesEmpty(t, g)
}

// A canceled CAS returns at once but keeps its place in line until the CAS
// commits before it have released. A later write that found it as the latest
// CAS of a PK waits only for it; if it left at once, that write would run
// while an earlier CAS still holds the PK.
func TestRowLockManagerCanceledCASKeepsItsPlaceUntilEarlierCASRelease(t *testing.T) {
	for _, laterExclusive := range []bool{true, false} {
		t.Run(fmt.Sprintf("later_exclusive_%t", laterExclusive), func(t *testing.T) {
			const k = int64(10)
			g := newRowLockManager()
			p := acquireTestRowLock(t, g, "v1", int64TestKeys(k), true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := startTestRowLock(ctx, g, "v1", int64TestKeys(k, 20), true)
			waitForTestPKQueued(t, g, "v1", 20)
			r2 := startTestRowLock(context.Background(), g, "v1", int64TestKeys(k, 30), laterExclusive)
			if laterExclusive {
				waitForTestPKQueued(t, g, "v1", 30)
			} else {
				waitForTestPKOrdinary(t, g, "v1", 30, 1)
			}
			requireTestRowLockPending(t, r)
			requireTestRowLockPending(t, r2)
			cancel()
			canceled := awaitTestRowLock(t, r)
			require.ErrorIs(t, canceled.err, context.Canceled)
			require.Nil(t, canceled.release)
			// p still holds k, so r2 keeps waiting.
			requireTestRowLockPending(t, r2)
			p()
			acquired := awaitTestRowLock(t, r2)
			require.NoError(t, acquired.err)
			acquired.release()
			requireTestRowLockTablesEmpty(t, g)
		})
	}
}

// A CAS that replaces several earlier CAS commits waits for all of them, and
// a canceled CAS keeps its place until all of them have released. Map order
// decides which earlier CAS a broken loop would wait for, so a wait for only
// one of them passes a round with probability 1/8; each case runs 3 rounds.
func TestRowLockManagerCASWaitsForEveryEarlierCAS(t *testing.T) {
	const holders = 8
	run := func(t *testing.T, canceled, laterExclusive bool) {
		g := newRowLockManager()
		var releases []func()
		for pk := int64(1); pk <= holders; pk++ {
			releases = append(releases, acquireTestRowLock(t, g, "v1", int64TestKeys(pk), true))
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cas := startTestRowLock(ctx, g, "v1", rangeTestKeys(1, holders+1, 100), true)
		waitForTestPKQueued(t, g, "v1", 100)
		waiting := cas
		if canceled {
			// later finds only the canceled CAS as the latest CAS of PKs 1-8.
			waiting = startTestRowLock(context.Background(), g, "v1", rangeTestKeys(1, holders+1, 200), laterExclusive)
			if laterExclusive {
				waitForTestPKQueued(t, g, "v1", 200)
			} else {
				waitForTestPKOrdinary(t, g, "v1", 200, 1)
			}
			cancel()
			require.ErrorIs(t, awaitTestRowLock(t, cas).err, context.Canceled)
		}
		for _, release := range releases[:holders-1] {
			release()
		}
		requireTestRowLockPending(t, waiting)
		releases[holders-1]()
		acquired := awaitTestRowLock(t, waiting)
		require.NoError(t, acquired.err)
		acquired.release()
		requireTestRowLockTablesEmpty(t, g)
	}
	t.Run("granted", func(t *testing.T) {
		for range 3 {
			run(t, false, false)
		}
	})
	for _, laterExclusive := range []bool{true, false} {
		t.Run(fmt.Sprintf("canceled/later_exclusive_%t", laterExclusive), func(t *testing.T) {
			for range 3 {
				run(t, true, laterExclusive)
			}
		})
	}
}

// A CAS canceled while it waits for an ordinary write gives up its place at
// once, which is safe only because it has already waited for its earlier CAS
// commits. A later CAS behind it must still wait for the CAS holding PK 10.
func TestRowLockManagerCASWaitsForEarlierCASBeforeOrdinaryWrites(t *testing.T) {
	g := newRowLockManager()
	holder := acquireTestRowLock(t, g, "v1", int64TestKeys(10), true)
	ordinary := acquireTestRowLock(t, g, "v1", int64TestKeys(20), false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := startTestRowLock(ctx, g, "v1", int64TestKeys(10, 20), true)
	waitForTestPKQueued(t, g, "v1", 20)
	later := startTestRowLock(context.Background(), g, "v1", int64TestKeys(10, 30), true)
	waitForTestPKQueued(t, g, "v1", 30)
	cancel()
	require.ErrorIs(t, awaitTestRowLock(t, canceled).err, context.Canceled)
	requireTestRowLockPending(t, later)
	holder()
	acquired := awaitTestRowLock(t, later)
	require.NoError(t, acquired.err)
	acquired.release()
	ordinary()
	requireTestRowLockTablesEmpty(t, g)
}

// A canceled ordinary write wakes the CAS commits that wait for it.
func TestRowLockManagerCanceledOrdinaryWriteWakesWaitingCAS(t *testing.T) {
	g := newRowLockManager()
	holder := acquireTestRowLock(t, g, "v1", int64TestKeys(10), true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ordinary := startTestRowLock(ctx, g, "v1", int64TestKeys(10, 20), false)
	waitForTestPKOrdinary(t, g, "v1", 20, 1)
	cas := startTestRowLock(context.Background(), g, "v1", int64TestKeys(20), true)
	waitForTestPKAwaited(t, g, "v1", 20)
	requireTestRowLockPending(t, cas)
	cancel()
	require.ErrorIs(t, awaitTestRowLock(t, ordinary).err, context.Canceled)
	// The CAS shares no PK with the holder, which still holds PK 10.
	acquired := awaitTestRowLock(t, cas)
	require.NoError(t, acquired.err)
	acquired.release()
	holder()
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerWaiterPinsTableUntilLastRelease(t *testing.T) {
	g := newRowLockManager()
	pks := int64TestKeys(10)
	holder, err := g.acquire(context.Background(), "v1", pks, true)
	require.NoError(t, err)
	g.mu.Lock()
	original := g.channels["v1"]
	require.Equal(t, 1, original.refs)
	g.mu.Unlock()
	waiter := startTestRowLock(context.Background(), g, "v1", pks, false)
	waitForTestRowLockRefs(t, g, "v1", 2)
	holder()
	acquired := awaitTestRowLock(t, waiter)
	require.NoError(t, acquired.err)
	g.mu.Lock()
	require.Same(t, original, g.channels["v1"])
	require.Equal(t, 1, original.refs)
	g.mu.Unlock()
	acquired.release()
	requireTestRowLockTablesEmpty(t, g)
	// A later write installs a fresh table.
	again, err := g.acquire(context.Background(), "v1", pks, true)
	require.NoError(t, err)
	g.mu.Lock()
	require.NotSame(t, original, g.channels["v1"])
	g.mu.Unlock()
	again()
	requireTestRowLockTablesEmpty(t, g)
}

// TestRowLockManagerRandomizedExclusion checks the row-lock invariant with
// per-PK counters: while a CAS holds a PK, no other CAS and no ordinary write
// holds it. Holders count after acquire returns and uncount before release, so
// the counters cover a subset of each real hold. Set ROW_LOCK_TEST_SEED to
// reuse a logged seed: it fixes each worker's operations, not the interleaving
// of the workers, so a failure reproduces only with some probability.
func TestRowLockManagerRandomizedExclusion(t *testing.T) {
	seed := time.Now().UnixNano()
	if value := os.Getenv("ROW_LOCK_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		require.NoError(t, err)
	}
	t.Logf("seed %d", seed)
	const (
		workers = 16
		// Enough iterations that a broken critical section fails most runs
		// even without the race detector.
		iterations = 4000
		intPKs     = 12
	)
	stringPKs := []string{"0", "1", "2", "3"}
	g := newRowLockManager()
	var casHolders, ordinaryHolders [intPKs + 4]atomic.Int32
	slot := func(pks primaryKeys) []int {
		var slots []int
		for _, pk := range pks.int64Values {
			slots = append(slots, int(pk))
		}
		for _, pk := range pks.stringValues {
			slots = append(slots, intPKs+slices.Index(stringPKs, pk))
		}
		slices.Sort(slots)
		return slices.Compact(slots)
	}
	var violations, acquired, canceled atomic.Int64
	var failure sync.Once
	var firstViolation atomic.Value
	violate := func(format string, args ...any) {
		violations.Add(1)
		failure.Do(func() { firstViolation.Store(fmt.Sprintf(format, args...)) })
	}
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			random := rand.New(rand.NewSource(seed))
			for range iterations {
				var pks primaryKeys
				for range 1 + random.Intn(6) {
					if random.Intn(4) == 0 {
						pks.stringValues = append(pks.stringValues, stringPKs[random.Intn(len(stringPKs))])
					} else {
						pks.int64Values = append(pks.int64Values, int64(random.Intn(intPKs)))
					}
				}
				exclusive := random.Intn(3) == 0
				ctx, cancel := context.Background(), context.CancelFunc(func() {})
				if random.Intn(10) == 0 {
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(random.Intn(200))*time.Microsecond)
				}
				release, err := g.acquire(ctx, "v1", pks, exclusive)
				cancel()
				if err != nil {
					canceled.Add(1)
					continue
				}
				acquired.Add(1)
				slots := slot(pks)
				for _, s := range slots {
					if exclusive {
						if n := casHolders[s].Add(1); n != 1 {
							violate("%d CAS commits hold slot %d", n, s)
						}
						if n := ordinaryHolders[s].Load(); n != 0 {
							violate("CAS acquired slot %d held by %d ordinary writes", s, n)
						}
					} else {
						ordinaryHolders[s].Add(1)
						if n := casHolders[s].Load(); n != 0 {
							violate("ordinary write acquired slot %d held by a CAS", s)
						}
					}
				}
				if random.Intn(4) == 0 {
					time.Sleep(time.Duration(random.Intn(50)) * time.Microsecond)
				} else {
					runtime.Gosched()
				}
				for _, s := range slots {
					if exclusive {
						if n := ordinaryHolders[s].Load(); n != 0 {
							violate("ordinary write acquired slot %d during a CAS hold", s)
						}
						casHolders[s].Add(-1)
					} else {
						ordinaryHolders[s].Add(-1)
					}
				}
				release()
			}
		}(seed + int64(worker))
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("randomized row-lock workload deadlocked")
	}
	if v := firstViolation.Load(); v != nil {
		t.Fatalf("%d violations, first: %s", violations.Load(), v)
	}
	require.Positive(t, acquired.Load())
	t.Logf("acquired %d, canceled %d", acquired.Load(), canceled.Load())
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerWaitRowLockCanceledWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	close(done)
	// Both select cases are ready, so the result must not depend on the case
	// select picks.
	for range 100 {
		require.ErrorIs(t, waitRowLock(ctx, done), context.Canceled)
	}
	require.NoError(t, waitRowLock(context.Background(), done))
}

// A vchannel holds one collection, so a VarChar table has no Int64 entries.
func TestRowLockManagerVarCharCASOrdersWithOrdinaryWrites(t *testing.T) {
	g := newRowLockManager()
	stringKeys := func(values ...string) primaryKeys {
		return primaryKeys{kind: primaryKeyKindString, stringValues: values}
	}
	ordinary := acquireTestRowLock(t, g, "v1", stringKeys("b", "a"), false)
	cas := startTestRowLock(context.Background(), g, "v1", stringKeys("a"), true)
	waitForTestPK(t, g, "v1", "a", func(s testPKLock) bool { return s.queued && s.awaited == 1 },
		"the CAS does not wait for the earlier write")
	requireTestRowLockPending(t, cas)
	ordinary()
	acquired := awaitTestRowLock(t, cas)
	require.NoError(t, acquired.err)
	// A later ordinary write waits for the held CAS.
	later := startTestRowLock(context.Background(), g, "v1", stringKeys("c", "a"), false)
	waitForTestPK(t, g, "v1", "c", func(s testPKLock) bool { return s.ordinary == 1 }, "the write did not register")
	requireTestRowLockPending(t, later)
	acquired.release()
	laterAcquired := awaitTestRowLock(t, later)
	require.NoError(t, laterAcquired.err)
	laterAcquired.release()
	requireTestRowLockTablesEmpty(t, g)
}

// Every CAS waiting for the same ordinary write must share one wait channel;
// otherwise a later waiter would replace it and the earlier one never wakes.
func TestRowLockManagerTwoCASWaitingForOneOrdinaryWriteBothWake(t *testing.T) {
	g := newRowLockManager()
	ordinary := acquireTestRowLock(t, g, "v1", int64TestKeys(1, 2), false)
	a := startTestRowLock(context.Background(), g, "v1", int64TestKeys(1), true)
	waitForTestPKAwaited(t, g, "v1", 1)
	b := startTestRowLock(context.Background(), g, "v1", int64TestKeys(2), true)
	waitForTestPKQueued(t, g, "v1", 2)
	waitForTestRowLockRefs(t, g, "v1", 3)
	requireTestRowLockPending(t, a)
	requireTestRowLockPending(t, b)
	ordinary()
	for _, result := range []<-chan testRowLockResult{a, b} {
		acquired := awaitTestRowLock(t, result)
		require.NoError(t, acquired.err)
		acquired.release()
	}
	requireTestRowLockTablesEmpty(t, g)
}

// The CAS snapshot walks the list of ordinary writes, so the list must stay
// intact for every release order.
func TestRowLockManagerOrdinaryListSurvivesEveryReleaseOrder(t *testing.T) {
	for _, order := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			g := newRowLockManager()
			releases := make([]func(), 3)
			for i := range releases {
				releases[i] = acquireTestRowLock(t, g, "v1", int64TestKeys(int64(i)), false)
			}
			releases[order[0]]()
			releases[order[1]]()
			last := int64(order[2])
			require.Equal(t, testPKLock{ordinary: 1}, inspectTestPK(g, "v1", last))
			cas := startTestRowLock(context.Background(), g, "v1", int64TestKeys(last), true)
			waitForTestPKAwaited(t, g, "v1", last)
			requireTestRowLockPending(t, cas)
			releases[order[2]]()
			acquired := awaitTestRowLock(t, cas)
			require.NoError(t, acquired.err)
			acquired.release()
			requireTestRowLockTablesEmpty(t, g)
		})
	}
}

func TestRowLockManagerDoubleReleasePanics(t *testing.T) {
	g := newRowLockManager()
	other := acquireTestRowLock(t, g, "v1", int64TestKeys(42), false)
	release, err := g.acquire(context.Background(), "v1", int64TestKeys(5), false)
	require.NoError(t, err)
	extra := acquireTestRowLock(t, g, "v1", int64TestKeys(6), false)
	release()
	require.PanicsWithValue(t, "partial update row lock released twice", release)
	// The second release changed nothing: the other writes stay registered and
	// keep the table pinned.
	require.Equal(t, testPKLock{ordinary: 1}, inspectTestPK(g, "v1", int64(42)))
	require.Equal(t, testPKLock{ordinary: 1}, inspectTestPK(g, "v1", int64(6)))
	require.Equal(t, 2, testRowLockRefs(g, "v1"))
	cas := startTestRowLock(context.Background(), g, "v1", int64TestKeys(42), true)
	waitForTestPKAwaited(t, g, "v1", 42)
	requireTestRowLockPending(t, cas)
	other()
	acquired := awaitTestRowLock(t, cas)
	require.NoError(t, acquired.err)
	acquired.release()
	require.Panics(t, acquired.release)
	require.Equal(t, 1, testRowLockRefs(g, "v1"))
	extra()
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerCASSkipsReleasedOrdinaryWrites(t *testing.T) {
	cas := newCASRowLock(primaryKeys{int64Values: []int64{3*rowLockScanChunk - 1}, stringValues: []string{"x"}})
	write := &ordinaryRowLock{pks: rangeTestKeys(0, 3*rowLockScanChunk)}
	write.registered.Store(true)
	require.True(t, cas.overlapsInFlight(write))
	// A released write never needs a wait, however much of it is left to scan.
	write.registered.Store(false)
	require.False(t, cas.overlapsInFlight(write))
	stringWrite := &ordinaryRowLock{pks: primaryKeys{stringValues: []string{"y", "x"}}}
	stringWrite.registered.Store(true)
	require.True(t, cas.overlapsInFlight(stringWrite))
	stringWrite.pks.stringValues = []string{"y"}
	require.False(t, cas.overlapsInFlight(stringWrite))
}

// TestRowLockManagerLargeWritesProgressUnderSteadyCAS checks that a large
// ordinary batch or CAS gets in while small CAS commits on random, cold PKs run
// continuously. A large write waits only for the writes that took their place
// before it, so newer CAS commits never overtake it.
func TestRowLockManagerLargeWritesProgressUnderSteadyCAS(t *testing.T) {
	const (
		casWorkers = 16
		casPKs     = 10
		space      = 100_000
		largePKs   = 10_000
		hold       = 2 * time.Millisecond
	)
	randomKeys := func(random *rand.Rand, n int) primaryKeys {
		keys := int64TestKeys(make([]int64, n)...)
		for i := range keys.int64Values {
			keys.int64Values[i] = random.Int63n(space)
		}
		return keys
	}
	g := newRowLockManager()
	// Canceling stops the workers even if a lock regression blocks them.
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for worker := range casWorkers {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			random := rand.New(rand.NewSource(seed))
			for ctx.Err() == nil {
				release, err := g.acquire(ctx, "v1", randomKeys(random, casPKs), true)
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("small CAS failed: %v", err)
					}
					return
				}
				time.Sleep(hold)
				release()
			}
		}(int64(worker))
	}
	defer func() {
		stop()
		wg.Wait()
		requireTestRowLockTablesEmpty(t, g)
	}()
	time.Sleep(20 * time.Millisecond)
	random := rand.New(rand.NewSource(int64(casWorkers)))
	for _, exclusive := range []bool{false, true} {
		for range 3 {
			largeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			start := time.Now()
			release, err := g.acquire(largeCtx, "v1", randomKeys(random, largePKs), exclusive)
			cancel()
			require.NoError(t, err, "a %d-PK write (exclusive %t) starved", largePKs, exclusive)
			t.Logf("%d-PK write (exclusive %t) got in after %v", largePKs, exclusive, time.Since(start))
			time.Sleep(hold)
			release()
		}
	}
}

func BenchmarkRowLockManagerOrdinary(b *testing.B) {
	for _, size := range []int{1, 1_000, 10_000, 100_000} {
		pks := rangeTestKeys(0, int64(size))
		b.Run(fmt.Sprintf("no_cas/%d", size), func(b *testing.B) {
			g := newRowLockManager()
			b.ReportAllocs()
			for b.Loop() {
				release, err := g.acquire(context.Background(), "v1", pks, false)
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
		// A disjoint active CAS keeps the table alive and makes ordinary
		// writes look up their PKs.
		b.Run(fmt.Sprintf("active_cas/%d", size), func(b *testing.B) {
			g := newRowLockManager()
			cas, err := g.acquire(context.Background(), "v1", int64TestKeys(-1), true)
			if err != nil {
				b.Fatal(err)
			}
			defer cas()
			b.ReportAllocs()
			for b.Loop() {
				release, err := g.acquire(context.Background(), "v1", pks, false)
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
	}
}

func BenchmarkRowLockManagerCAS(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			g := newRowLockManager()
			pks := rangeTestKeys(0, int64(size))
			b.ReportAllocs()
			for b.Loop() {
				release, err := g.acquire(context.Background(), "v1", pks, true)
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
	}
}

func BenchmarkRowLockManagerOrdinaryParallel(b *testing.B) {
	for _, size := range []int{1, 1_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			g := newRowLockManager()
			pks := rangeTestKeys(0, int64(size))
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					release, err := g.acquire(context.Background(), "v1", pks, false)
					if err != nil {
						panic(err)
					}
					release()
				}
			})
		})
	}
}
