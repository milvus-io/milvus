package roaringfilter

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"
)

// buildLegacy is Build as it was before the ordering/allocation fix: insert one
// value at a time in caller order, serialize into a fresh body, then copy that
// body into the envelope. Kept so the benchmark measures the delta rather than
// asserting it, and so the byte-for-byte equivalence of the new Build can be
// pinned against it.
func buildLegacy(members []int64) ([]byte, error) {
	bitmap := roaring64.New()
	for _, member := range members {
		bitmap.Add(uint64(member))
	}
	bitmap.RunOptimize()
	body, err := bitmap.ToBytes()
	if err != nil {
		return nil, err
	}
	blob := make([]byte, HeaderSize+len(body))
	copy(blob[0:4], Magic)
	binary.LittleEndian.PutUint16(blob[4:6], Version)
	binary.LittleEndian.PutUint16(blob[6:8], FormatPortableRoaring64)
	binary.LittleEndian.PutUint64(blob[8:16], bitmap.GetCardinality())
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(body)))
	binary.LittleEndian.PutUint64(blob[24:32], 0)
	copy(blob[HeaderSize:], body)
	return blob, nil
}

func heapLive() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

type peakSampler struct {
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

func startPeak() *peakSampler {
	p := &peakSampler{stop: make(chan struct{}), done: make(chan struct{})}
	p.peak.Store(heapLive())
	go func() {
		defer close(p.done)
		t := time.NewTicker(200 * time.Microsecond)
		defer t.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-t.C:
				if v := heapLive(); v > p.peak.Load() {
					p.peak.Store(v)
				}
			}
		}
	}()
	return p
}

func (p *peakSampler) finish() uint64 { close(p.stop); <-p.done; return p.peak.Load() }

// peakDelta is the live heap attributable to the call, i.e. the sampled peak
// above whatever was already live before it.
func peakDelta(peak, before uint64) uint64 {
	if peak < before {
		return 0
	}
	return peak - before
}

type shape struct {
	name string
	gen  func(n int, rng *rand.Rand) []int64
}

var buildShapes = []shape{
	{"contiguous", func(n int, _ *rand.Rand) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i)
		}
		return out
	}},
	{"int32-range shuffled", func(n int, rng *rand.Rand) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(rng.Uint32())
		}
		return out
	}},
	{"snowflake-like", func(n int, rng *rand.Rand) []int64 {
		base := int64(1) << 62
		out := make([]int64, n)
		for i := range out {
			out[i] = base + rng.Int63n(int64(n)*64)
		}
		return out
	}},
	{"int64 full-range shuffled", func(n int, rng *rand.Rand) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(rng.Uint64())
		}
		return out
	}},
}

// TestBuildMatchesLegacyByteForByte is the gate: the ordering and allocation
// changes must not alter a single emitted byte, or every already-published blob
// and the C++ prober's golden vectors would disagree with the builder.
func TestBuildMatchesLegacyByteForByte(t *testing.T) {
	rng := rand.New(rand.NewSource(20260728))
	for _, sh := range buildShapes {
		for _, n := range []int{0, 1, 2, 1000, 100_000} {
			members := sh.gen(n, rng)
			want, err := buildLegacy(members)
			require.NoError(t, err)
			got, err := Build(members)
			require.NoError(t, err)
			require.Equalf(t, want, got, "shape=%s n=%d", sh.name, n)
		}
	}
	// Duplicates and adversarial orderings must also round-trip identically.
	dupes := []int64{5, 5, -1, -1, 0, 1 << 40, -(1 << 40), 5}
	want, err := buildLegacy(dupes)
	require.NoError(t, err)
	got, err := Build(dupes)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestBuildDoesNotMutateCallerSlice pins that sorting happens on a copy: the
// caller's membership slice is theirs, and reordering it under them would be a
// silent side effect.
func TestBuildDoesNotMutateCallerSlice(t *testing.T) {
	members := []int64{9, 3, 7, 1, -5}
	original := append([]int64(nil), members...)
	_, err := Build(members)
	require.NoError(t, err)
	require.Equal(t, original, members, "Build must not reorder the caller's slice")
}

// TestBuildCostReport is an opt-in diagnostic, not an assertion. It prints the
// build time and peak heap of the old and new paths so the tradeoff can be
// measured manually without putting the pathological legacy builder on the
// default unit-test path. The pathological case is an int64 set spread over
// many 2^32 buckets in random order: roaring64.Add binary-searches and inserts
// into a sorted slice of high containers, so unsorted input degenerates into
// O(n^2) memmove.
func TestBuildCostReport(t *testing.T) {
	if testing.Short() {
		t.Skip("cost report is slow by design")
	}
	if os.Getenv("MILVUS_RUN_ROARING_BUILD_COST_REPORT") != "1" {
		t.Skip("set MILVUS_RUN_ROARING_BUILD_COST_REPORT=1 to run the slow cost report")
	}
	rng := rand.New(rand.NewSource(20260728))
	fmt.Printf("\n%-28s %9s %12s %12s %9s %11s %11s\n",
		"shape", "n", "legacy", "new", "speedup", "legacyPeak", "newPeak")
	for _, sh := range buildShapes {
		for _, n := range []int{200_000, 1_000_000} {
			members := sh.gen(n, rng)

			runtime.GC()
			before := heapLive()
			p := startPeak()
			t0 := time.Now()
			oldBlob, err := buildLegacy(members)
			dOld := time.Since(t0)
			oldPeak := peakDelta(p.finish(), before)
			require.NoError(t, err)

			runtime.GC()
			before = heapLive()
			p = startPeak()
			t0 = time.Now()
			newBlob, err := Build(members)
			dNew := time.Since(t0)
			newPeak := peakDelta(p.finish(), before)
			require.NoError(t, err)
			require.Equal(t, oldBlob, newBlob)

			fmt.Printf("%-28s %9d %12v %12v %8.1fx %9.1fMiB %9.1fMiB\n",
				sh.name, n, dOld.Round(time.Millisecond), dNew.Round(time.Millisecond),
				float64(dOld)/float64(dNew),
				float64(oldPeak)/(1<<20), float64(newPeak)/(1<<20))
		}
	}
}

// TestBuildResourceReport is an opt-in client resource report for the admitted
// and rejected shapes covered by the standard benchmarks. Input slices are
// prepared before measurement, so input_B is reported separately while
// alloc_B, mallocs and peakBuildHeap cover Build itself.
func TestBuildResourceReport(t *testing.T) {
	if os.Getenv("MILVUS_RUN_ROARING_RESOURCE_REPORT") != "1" {
		t.Skip("set MILVUS_RUN_ROARING_RESOURCE_REPORT=1 to print client build resources")
	}
	cases := []struct {
		shapeIndex   int
		n            int
		wantRejected bool
	}{
		{shapeIndex: 0, n: 1_000_000},
		{shapeIndex: 1, n: 1_000_000},
		{shapeIndex: 2, n: 1_000_000},
		{shapeIndex: 3, n: 200_000},
		{shapeIndex: 3, n: 300_000, wantRejected: true},
	}
	fmt.Printf("\n%-28s %9s %11s %11s %10s %12s %12s %10s\n",
		"shape", "n", "input_B", "blob_B", "time", "alloc_B", "peakBuild", "mallocs")
	for _, test := range cases {
		sh := buildShapes[test.shapeIndex]
		members := sh.gen(test.n, rand.New(rand.NewSource(20260728)))
		runtime.GC()
		beforeHeap := heapLive()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		peak := startPeak()
		started := time.Now()
		blob, err := Build(members)
		elapsed := time.Since(started)
		peakBuild := peakDelta(peak.finish(), beforeHeap)
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if test.wantRejected {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		fmt.Printf("%-28s %9d %11d %11d %10v %12d %9.1fMiB %10d\n",
			sh.name, test.n, len(members)*8, len(blob), elapsed.Round(time.Microsecond),
			after.TotalAlloc-before.TotalAlloc, float64(peakBuild)/(1<<20),
			after.Mallocs-before.Mallocs)
		runtime.KeepAlive(members)
		runtime.KeepAlive(blob)
	}
}

var benchmarkBlobSink []byte

func benchmarkBuildShape(b *testing.B, sh shape, n int, wantRejected bool) {
	b.Helper()
	members := sh.gen(n, rand.New(rand.NewSource(20260728)))
	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	var blob []byte
	for i := 0; i < b.N; i++ {
		var err error
		blob, err = Build(members)
		if wantRejected {
			if err == nil {
				b.Fatal("expected decoded-resource admission rejection")
			}
		} else if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkBlobSink = blob
	if !wantRejected {
		b.ReportMetric(float64(len(blob)), "blob_B")
		b.ReportMetric(float64(len(blob))/float64(n), "blob_B/member")
	}
}

func BenchmarkBuildContiguous1M(b *testing.B) {
	benchmarkBuildShape(b, buildShapes[0], 1_000_000, false)
}

func BenchmarkBuildInt32Shuffled1M(b *testing.B) {
	benchmarkBuildShape(b, buildShapes[1], 1_000_000, false)
}

func BenchmarkBuildSnowflakeLike1M(b *testing.B) {
	benchmarkBuildShape(b, buildShapes[2], 1_000_000, false)
}

func BenchmarkBuildFullRangeAccepted200K(b *testing.B) {
	benchmarkBuildShape(b, buildShapes[3], 200_000, false)
}

func BenchmarkBuildFullRangeRejected300K(b *testing.B) {
	benchmarkBuildShape(b, buildShapes[3], 300_000, true)
}
