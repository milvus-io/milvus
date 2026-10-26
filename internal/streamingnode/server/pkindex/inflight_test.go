// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pkindex

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hookAuthority runs beforeMultiGet before each engine probe, to place observe
// between the in-flight set read and the engine read of a decision.
type hookAuthority struct {
	Authority
	beforeMultiGet func()
}

func (a *hookAuthority) MultiGet(ctx context.Context, pks []PK) ([]*Entry, error) {
	if a.beforeMultiGet != nil {
		a.beforeMultiGet()
	}
	return a.Authority.MultiGet(ctx, pks)
}

// Registration races with observe. A write of p was appended (filled in, tt 10)
// and a later write of p registers. Observe of tt 10 can run before the later
// write reads the set, or between its set read and its engine read. Rule B (set
// first) and rule A (engine first) keep p present in both cases.
func TestRegistrationRacesWithObserve(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name          string
		observeBefore bool // observe before the decision, else between the set and the engine read
	}{
		{"observe before the set read", true},
		{"observe between the set read and the engine read", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, err := NewAuthority("v1")
			require.NoError(t, err)
			hook := &hookAuthority{Authority: a}
			d := NewDecider(hook)
			defer d.Close()

			first, _ := decideInsert(t, d, 1)
			first.Apply(ctx, Applied{TimeTick: 10})

			observe := func() { require.NoError(t, d.ObserveForTest(ctx, 10, 500, int64PKs(1), nil)) }
			if c.observeBefore {
				observe()
			} else {
				// key 2 misses the set, so the decision probes the engine.
				hook.beforeMultiGet = observe
			}
			_, companion := decideInsert(t, d, 1, 2)
			require.Equal(t, int64PKs(1), companion)
		})
	}
}

// The time tick fill-in races with observe. Observe may apply a message before
// its append returns to the writer, because the append is acknowledged first.
// Observe then skips the entry (not filled in yet), and the fill-in removes it.
func TestFillInRacesWithObserve(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)

	intent, _ := decideInsert(t, d, 1)
	// the append was acknowledged with tt 10 and observe applied it.
	require.NoError(t, d.ObserveForTest(ctx, 10, 500, int64PKs(1), nil))
	require.Equal(t, 1, d.InflightLenForTest(), "observe skips a write that is not filled in")

	intent.Apply(ctx, Applied{TimeTick: 10})
	require.Equal(t, 0, d.InflightLenForTest(), "the fill-in removes a write that observe passed")
	requireSegment(t, a, 1, 500)

	_, companion := decideInsert(t, d, 1)
	require.Equal(t, int64PKs(1), companion)
}

// Observe and the fill-in can run in either order. The write must leave the set
// in both orders, and so must a write with an unknown outcome.
func TestFillInAndObserveOrder(t *testing.T) {
	pks := int64PKs(1, 2)
	t.Run("fill-in after observe passed its time tick", func(t *testing.T) {
		var s inflightSet
		w, _ := s.register(pks)
		s.observe(10, pks)
		require.Equal(t, 2, s.len(), "observe skips a write that is not filled in")
		s.fill(w, 10)
		require.Equal(t, 0, s.len())
		require.Nil(t, s.keys)
	})
	t.Run("observe after the fill-in", func(t *testing.T) {
		var s inflightSet
		w, _ := s.register(pks)
		s.fill(w, 10)
		require.Equal(t, 2, s.len())
		s.observe(10, pks)
		require.Equal(t, 0, s.len())
		require.Nil(t, s.keys)
	})
	t.Run("uncertain mark after observe passed its time tick", func(t *testing.T) {
		var s inflightSet
		w, _ := s.register(pks)
		s.observeTimeTick(10)
		s.markUncertain(w, 10)
		require.Equal(t, 0, s.len())
		require.Empty(t, s.uncertain)
	})
	t.Run("observe after the uncertain mark", func(t *testing.T) {
		var s inflightSet
		w, _ := s.register(pks)
		s.markUncertain(w, 10)
		require.Equal(t, 2, s.len())
		require.Len(t, s.uncertain, 1)
		s.observeTimeTick(10)
		require.Equal(t, 0, s.len())
		require.Empty(t, s.uncertain)
	})
}

// The keys of one write are deduplicated after they are mapped to lock keys.
func TestRegisterDeduplicatesKeys(t *testing.T) {
	var s inflightSet
	w, _ := s.register(int64PKs(3, 1, 3, 2, 1))
	require.Len(t, w.keys, 3)
	require.Equal(t, 3, s.len())

	r := rand.New(rand.NewSource(1))
	values := make([]int64, 1000)
	for i := range values {
		values[i] = int64(r.Intn(300))
	}
	keys := lockKeys(int64PKs(values...))
	distinct := make(map[int64]struct{})
	for _, v := range values {
		distinct[v] = struct{}{}
	}
	require.Len(t, keys, len(distinct))
	require.True(t, slices.IsSorted(keys))
	for i := 1; i < len(keys); i++ {
		require.NotEqual(t, keys[i-1], keys[i])
	}
}

// Two keys with the same lock key share one entry: the second writer waits for
// the first and gets a companion delete for its own key, which may not exist.
// Both are extra work only. A real collision needs two varchar keys with the
// same FNV-1a hash, so the test pairs a varchar key with the int64 key of the
// same value: the set sees only lock keys.
func TestLockKeyCollisionOnlyAddsWork(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	a := VarCharPK("a")
	twin := Int64PK(int64(a.lockKey()))

	first, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: []PK{twin}})
	require.NoError(t, err)

	decided := make(chan []PK)
	go func() {
		second, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: []PK{a}})
		if err != nil {
			close(decided)
			return
		}
		decided <- second.Decision().CompanionDeletePKs
		second.Apply(ctx, Applied{TimeTick: 20})
	}()
	select {
	case <-decided:
		t.Fatal("the second writer did not wait for the first one")
	case <-time.After(50 * time.Millisecond):
	}
	first.Apply(ctx, Applied{TimeTick: 10})
	require.Equal(t, []PK{a}, <-decided)
}

// A simulated WAL with an observer that applies each record as soon as it is in
// the log, often before the writer fills in the time tick. Across the run each
// key misses exactly once (its first insert), and no entry is left behind.
func TestInflightStressWithObserve(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	type record struct {
		tt  uint64
		pks []PK
	}
	var (
		logMu sync.Mutex
		log   []record
		tt    uint64
	)
	appendToLog := func(pks []PK) uint64 {
		logMu.Lock()
		defer logMu.Unlock()
		tt++
		log = append(log, record{tt: tt, pks: pks})
		return tt
	}

	stop := make(chan struct{})
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		next := 0
		for {
			logMu.Lock()
			pending := log[next:]
			logMu.Unlock()
			for _, r := range pending {
				if err := d.ObserveForTest(ctx, r.tt, int64(r.tt), r.pks, r.pks); err != nil {
					t.Error(err)
					return
				}
				next++
			}
			select {
			case <-stop:
				if len(pending) == 0 {
					return
				}
			default:
			}
		}
	}()

	const (
		writers = 16
		rounds  = 300
		keys    = 64
	)
	var misses atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < rounds; i++ {
				values := randomKeys(r, 1+r.Intn(4), keys)
				intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(values...)})
				if err != nil {
					t.Error(err)
					return
				}
				misses.Add(int64(len(values) - len(intent.Decision().CompanionDeletePKs)))
				tt := appendToLog(int64PKs(values...))
				if r.Intn(4) == 0 {
					time.Sleep(time.Microsecond) // let observe overtake the fill-in
				}
				intent.Apply(ctx, Applied{TimeTick: tt})
			}
		}(int64(w))
	}
	wg.Wait()
	close(stop)
	<-observerDone

	require.LessOrEqual(t, misses.Load(), int64(keys))
	distinct := map[int64]struct{}{}
	for _, r := range log {
		for _, pk := range r.pks {
			distinct[pk.Int64()] = struct{}{}
		}
	}
	require.Equal(t, int64(len(distinct)), misses.Load(), "every key misses exactly once")
	require.Equal(t, 0, d.InflightLenForTest(), "no entry outlives observe")
	require.Nil(t, d.inflight.keys, "an empty map is dropped")
}

// The second writer of a key waits until the first one filled in its time tick,
// and then sees the key.
func TestTwoWritersOnTheSamePK(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	first, _ := decideInsert(t, d, 1)
	second := make(chan []PK)
	go func() {
		intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
		if err != nil {
			t.Error(err)
			close(second)
			return
		}
		second <- intent.Decision().CompanionDeletePKs
		intent.Apply(ctx, Applied{TimeTick: 11})
	}()
	select {
	case <-second:
		t.Fatal("the second writer did not wait for the first one")
	case <-time.After(50 * time.Millisecond):
	}
	first.Apply(ctx, Applied{TimeTick: 10})
	require.Equal(t, int64PKs(1), <-second)
}

// Writes of other keys do not wait for each other.
func TestWritersOnOtherPKsDoNotWait(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	first, _ := decideInsert(t, d, 1, 2)
	second, companion := decideInsert(t, d, 3, 4)
	require.Empty(t, companion)
	second.Apply(ctx, Applied{TimeTick: 11})
	first.Apply(ctx, Applied{TimeTick: 10})
}

// A writer whose context ends while it waits keeps its place until the writes it
// waited for are done. The writer after it waits only for it, so it must not run
// while the first write is still pending.
func TestCanceledWaiterKeepsItsPlace(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	first, _ := decideInsert(t, d, 1)

	canceledCtx, cancel := context.WithCancel(ctx)
	canceled := make(chan error)
	go func() {
		_, err := d.Decide(canceledCtx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
		canceled <- err
	}()
	require.Eventually(t, func() bool { return d.InflightLenForTest() == 2 }, 5*time.Second, time.Millisecond)

	third := make(chan []PK)
	go func() {
		intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
		if err != nil {
			t.Error(err)
			close(third)
			return
		}
		third <- intent.Decision().CompanionDeletePKs
		intent.Apply(ctx, Applied{TimeTick: 12})
	}()
	require.Eventually(t, func() bool { return d.InflightLenForTest() == 3 }, 5*time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-canceled, context.Canceled)
	select {
	case <-third:
		t.Fatal("the third writer ran while the first write was pending")
	case <-time.After(50 * time.Millisecond):
	}
	first.Apply(ctx, Applied{TimeTick: 10})
	require.Equal(t, int64PKs(1), <-third)
	require.Equal(t, 2, d.InflightLenForTest(), "the canceled write left the set")
}

// Writes of many overlapping keys, registered in random key orders, with some
// contexts canceled while waiting and some appends failing, all finish.
func TestMultiKeyWritesDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	const (
		writers = 32
		rounds  = 100
		keys    = 200
	)
	var tt atomic.Uint64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < rounds; i++ {
				values := randomKeys(r, 1+r.Intn(40), keys)
				r.Shuffle(len(values), func(a, b int) { values[a], values[b] = values[b], values[a] })
				writeCtx, cancel := context.WithCancel(ctx)
				if r.Intn(10) == 0 {
					cancel()
				}
				intent, err := d.Decide(writeCtx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(values...)})
				cancel()
				if err != nil {
					continue
				}
				switch r.Intn(10) {
				case 0:
					intent.Discard(ctx, nil)
				case 1:
					intent.DiscardUncertain(ctx, nil, tt.Add(1))
				default:
					intent.Apply(ctx, Applied{TimeTick: tt.Add(1)})
				}
			}
		}(int64(w))
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("writes deadlocked")
	}
}

func randomKeys(r *rand.Rand, n int, space int) []int64 {
	seen := map[int64]struct{}{}
	for len(seen) < n {
		seen[int64(r.Intn(space))] = struct{}{}
	}
	values := make([]int64, 0, n)
	for v := range seen {
		values = append(values, v)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

// BenchmarkInflightLock measures the lock of the in-flight set with 32 writers
// that hold it for a simulated 2 ms append, as the stripe lock comparison in the
// design did. Each write is observed right after it fills in its time tick, so
// the set stays small.
//
//	go test -tags dynamic,test -run '^$' -bench BenchmarkInflightLock -benchtime 2000x
//
// TODO: measure the throughput with the real append round trip of a WAL backend,
// not a sleep.
func BenchmarkInflightLock(b *testing.B) {
	const (
		writers   = 32
		appendRTT = 2 * time.Millisecond
	)
	for _, c := range []struct {
		name string
		keys func(r *rand.Rand, i int) []int64
	}{
		{"batch_1", func(r *rand.Rand, _ int) []int64 { return randomKeys(r, 1, 1<<30) }},
		{"batch_16", func(r *rand.Rand, _ int) []int64 { return randomKeys(r, 16, 1<<30) }},
		{"batch_100", func(r *rand.Rand, _ int) []int64 { return randomKeys(r, 100, 1<<30) }},
		{"batch_1000", func(r *rand.Rand, _ int) []int64 { return randomKeys(r, 1000, 1<<30) }},
		{"hot_pk", func(*rand.Rand, int) []int64 { return []int64{1} }},
		{"mixed_batches", func(r *rand.Rand, i int) []int64 {
			return randomKeys(r, []int{1, 16, 100, 1000}[i%4], 1<<30)
		}},
		{"mixed_batches_with_hot_pk", func(r *rand.Rand, i int) []int64 {
			return append(randomKeys(r, []int{1, 16, 100, 1000}[i%4], 1<<30), -1)
		}},
	} {
		b.Run(c.name, func(b *testing.B) {
			ctx := context.Background()
			a, err := NewAuthority("v1")
			require.NoError(b, err)
			d := NewDecider(a)
			defer d.Close()

			var next, tt atomic.Uint64
			var lockWait atomic.Int64
			var wg sync.WaitGroup
			b.ResetTimer()
			start := time.Now()
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for {
						i := next.Add(1)
						if i > uint64(b.N) {
							return
						}
						pks := int64PKs(c.keys(r, int(i))...)
						intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: pks})
						if err != nil {
							b.Error(err)
							return
						}
						lockWait.Add(int64(intent.LockWait()))
						time.Sleep(appendRTT)
						t := tt.Add(1)
						intent.Apply(ctx, Applied{TimeTick: t})
						_ = d.ObserveForTest(ctx, t, int64(t), pks, nil)
					}
				}(int64(w))
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/s")
			b.ReportMetric(float64(lockWait.Load())/float64(b.N)/1e6, "ms-lock-wait/op")
		})
	}
}

// BenchmarkInflightLockOnly measures only the lock part of the set: register,
// wait, a simulated 2 ms append, fill-in and the removal by observe. It matches
// the stripe lock and RowLock comparison of the design, which measured the locks
// alone; BenchmarkInflightLock adds the decision and the engine.
func BenchmarkInflightLockOnly(b *testing.B) {
	const (
		writers   = 32
		appendRTT = 2 * time.Millisecond
	)
	for _, n := range []int{1, 16, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", n), func(b *testing.B) {
			var s inflightSet
			var next, tt atomic.Uint64
			var wg sync.WaitGroup
			b.ResetTimer()
			start := time.Now()
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					// keys are generated outside the timed loop body, so the
					// benchmark measures the lock and not the generator.
					batches := make([][]PK, 64)
					for i := range batches {
						batches[i] = int64PKs(randomKeys(r, n, 1<<30)...)
					}
					for i := 0; next.Add(1) <= uint64(b.N); i++ {
						pks := batches[i%len(batches)]
						w, earlier := s.register(pks)
						_ = s.wait(context.Background(), w, earlier)
						time.Sleep(appendRTT)
						t := tt.Add(1)
						s.fill(w, t)
						s.observe(t, pks)
					}
				}(int64(w))
			}
			wg.Wait()
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "msgs/s")
		})
	}
}

// BenchmarkWritePathOf1000Keys splits the work of one write of 1000 new keys
// without contention: the lock alone (register, wait, fill-in and the removal by
// observe), the engine probe alone, and the whole decision (lock, set check and
// probe). The engine is the in-memory test engine, so the probe here is a lower
// bound of a persistent engine probe.
func BenchmarkWritePathOf1000Keys(b *testing.B) {
	const n = 1000
	batches := make([][]PK, 256)
	for i := range batches {
		values := make([]int64, n)
		for j := range values {
			values[j] = int64(i*n + j)
		}
		batches[i] = int64PKs(values...)
	}

	b.Run("lock", func(b *testing.B) {
		var s inflightSet
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pks := batches[i%len(batches)]
			w, earlier := s.register(pks)
			_ = s.wait(context.Background(), w, earlier)
			tt := uint64(i + 1)
			s.fill(w, tt)
			s.observe(tt, pks)
		}
	})
	b.Run("probe", func(b *testing.B) {
		a, err := NewAuthority("v1")
		require.NoError(b, err)
		defer a.Close()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := a.MultiGet(context.Background(), batches[i%len(batches)]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decision", func(b *testing.B) {
		a, err := NewAuthority("v1")
		require.NoError(b, err)
		d := NewDecider(a)
		defer d.Close()
		ctx := context.Background()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pks := batches[i%len(batches)]
			intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: pks})
			if err != nil {
				b.Fatal(err)
			}
			tt := uint64(i + 1)
			intent.Apply(ctx, Applied{TimeTick: tt})
			d.inflight.observe(tt, pks)
		}
	})
}
