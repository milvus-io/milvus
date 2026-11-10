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

package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tklauser/go-sysconf"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestThreadWatcher(t *testing.T) {
	watcher := NewThreadWatcher()

	watcher.Start()

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		watcher.Stop()
	}()
	select {
	case <-ch:
	case <-time.After(time.Second * 5):
		require.FailNow(t, "watcher failed to close after 5 seconds")
	}
}

func TestClassifyThreadName(t *testing.T) {
	group, ok := classifyThreadName("knowhere_search")
	require.True(t, ok)
	require.Equal(t, "knowhere_search", group)

	group, ok = classifyThreadName("MILVUS_FL_WR_0")
	require.True(t, ok)
	require.Equal(t, "file_write", group)

	group, ok = classifyThreadName("knowhere_fetch_")
	require.True(t, ok)
	require.Equal(t, "knowhere_fetch", group)

	group, ok = classifyThreadName("rocksdb:high")
	require.True(t, ok)
	require.Equal(t, "rocksdb_high", group)

	group, ok = classifyThreadName("rocksdb:low")
	require.True(t, ok)
	require.Equal(t, "rocksdb_low", group)

	group, ok = classifyThreadName("rocksdb:bottom")
	require.True(t, ok)
	require.Equal(t, "rocksdb_bottom", group)

	group, ok = classifyThreadName("grpc_global_tim")
	require.False(t, ok)
	require.Equal(t, unclassifiedThreadPool, group)
}

func TestParseThreadStat(t *testing.T) {
	stat, err := parseThreadStat("123 (knowhere search) R 1 2 3 4 5 6 7 8 9 10 34 56 0 0 0 0 0 0 789")
	require.NoError(t, err)
	require.Equal(t, byte('R'), stat.state)
	require.Equal(t, threadCPU{user: 34, system: 56}, stat.cpu)
	require.Equal(t, uint64(789), stat.startTime)
	require.Equal(t, "knowhere search", stat.name)
}

func TestCollectActiveThreadGroups(t *testing.T) {
	current := []threadStat{
		{tid: 1, name: "rocksdb:high", state: 'S', cpu: threadCPU{user: 11}},
		{tid: 2, name: "arrow-worker", state: 'S', cpu: threadCPU{user: 6}},
		{tid: 3, name: "rocksdb:low", state: 'R', cpu: threadCPU{user: 1}},
		{tid: 4, name: "knowhere_search", state: 'S', cpu: threadCPU{user: 20}},
		{tid: 5, name: "rocksdb:bottom", state: 'D', cpu: threadCPU{user: 30}},
	}
	previous := map[int32]threadSample{
		1: {group: "rocksdb_high", cpu: threadCPU{user: 10}},
		2: {group: unclassifiedThreadPool, cpu: threadCPU{user: 5}},
		4: {group: "knowhere_fetch", cpu: threadCPU{user: 19}},
		5: {group: "rocksdb_bottom", cpu: threadCPU{user: 30}},
	}

	activeByGroup, _, nextSamples := collectThreadGroups(current, previous, 0)

	require.Equal(t, 1, activeByGroup["rocksdb_high"])
	require.Equal(t, 1, activeByGroup[unclassifiedThreadPool])
	require.Equal(t, 0, activeByGroup["rocksdb_low"])
	require.Equal(t, 0, activeByGroup["knowhere_search"])
	require.Equal(t, 1, activeByGroup["rocksdb_bottom"])
	require.Equal(t, threadSample{group: unclassifiedThreadPool, cpu: threadCPU{user: 6}}, nextSamples[2])
	require.Len(t, nextSamples, len(current))
}

func TestAdditionalThreadPools(t *testing.T) {
	for name, want := range map[string]string{
		"HIGH_SEGC_POOL0":  "segcore_high",
		"MIDD_SEGC_POOL0":  "segcore_middle",
		"LOW_SEGC_POOL0":   "segcore_low",
		"MILVUS_ASYNC_LOA": "async_load",
		"MILVUS_LF_IO_0":   "local_file_io",
		"MILVUS_PREFETCH":  "prefetch",
		"AwsEventLoop1":    "aws_crt_event_loop",
		"AwsEventLoop 1":   "aws_crt_event_loop",
		"AwsHostResolver":  "aws_crt_resolver",
		"EvntLoopCleanup":  "aws_crt_cleanup",
		"knowhere_build0":  "knowhere_build",
		"knowhere_search":  "knowhere_search",
		"knowhere_fetch_":  "knowhere_fetch",
	} {
		group, ok := classifyThreadName(name)
		require.True(t, ok, name)
		require.Equal(t, want, group, name)
	}
}

func TestThreadCPUAccounting(t *testing.T) {
	previous := map[int32]threadSample{1: {group: "segcore_high", cpu: threadCPU{user: 100}, startTime: 10}}
	cases := []struct {
		name       string
		stat       threadStat
		since      uint64
		wantGroup  string
		wantCPU    uint64
		wantActive int
	}{
		{"delta", threadStat{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 125}, startTime: 10}, 50, "segcore_high", 25, 1},
		{"reused tid", threadStat{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 130}, startTime: 60}, 50, "segcore_high", 130, 0},
		{"reused old tid", threadStat{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 200}, startTime: 30}, 50, "segcore_high", 0, 0},
		{"old unseen thread", threadStat{tid: 2, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 130}, startTime: 10}, 50, "segcore_high", 0, 0},
		{"new thread", threadStat{tid: 2, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 30}, startTime: 60}, 50, "segcore_high", 30, 0},
		{"ambiguous birth tick", threadStat{tid: 2, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 30}, startTime: 50}, 50, "segcore_high", 0, 0},
		{"renamed", threadStat{tid: 1, name: "LOW_SEGC_POOL", cpu: threadCPU{user: 125}, startTime: 10}, 50, "segcore_high", 25, 0},
		{"rollback", threadStat{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 99}, startTime: 10}, 50, "segcore_high", 0, 0},
		{"io wait", threadStat{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 100}, startTime: 10, state: 'D'}, 50, "segcore_high", 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			active, cpu, next := collectThreadGroups([]threadStat{tc.stat}, previous, tc.since)
			require.Equal(t, tc.wantCPU, cpu[tc.wantGroup].user)
			require.Equal(t, tc.wantActive, active[tc.wantGroup])
			require.Len(t, next, 1)
			require.Equal(t, tc.stat.startTime, next[tc.stat.tid].startTime)
		})
	}
	_, cpu, next := collectThreadGroups([]threadStat{{tid: 1, cpu: threadCPU{user: 100}, startTime: 60}}, nil, 0)
	require.Empty(t, cpu, "initial snapshot must not import historic CPU")
	require.Len(t, next, 1)
	_, cpu, next = collectThreadGroups([]threadStat{{tid: 1, unreadable: true}}, previous, 50)
	require.Empty(t, cpu)
	require.Equal(t, previous, next)
	_, cpu, next = collectThreadGroups([]threadStat{{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 150}, startTime: 10}}, next, 60)
	require.Equal(t, uint64(50), cpu["segcore_high"].user, "recover all ticks after a failed read")
	_, cpu, next = collectThreadGroups(nil, next, 70)
	require.Empty(t, next, "retire exited threads")
	require.Empty(t, cpu, "exit must not subtract counter values")
}

func threadStatFixture(tid int, name string, cpu, start uint64) string {
	return fmt.Sprintf("%d (%s) S 1 2 3 4 5 6 7 8 9 10 %d 0 0 0 0 0 0 0 %d", tid, name, cpu, start)
}

func TestThreadStatValidation(t *testing.T) {
	for _, raw := range []string{"", "1 no comm", "1 ()", "x (name) S", "1 (name) S 0", threadStatFixture(1, "name", 1, 1)[:30]} {
		_, err := parseThreadStat(raw)
		require.Error(t, err, raw)
	}
	stat, err := parseThreadStat(threadStatFixture(42, "name ) with (", 123, 456))
	require.NoError(t, err)
	require.Equal(t, threadStat{tid: 42, name: "name ) with (", state: 'S', cpu: threadCPU{user: 123}, startTime: 456}, stat)
}

func TestCollectThreadStatsFailures(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "self", "task", "42")
	require.NoError(t, os.MkdirAll(taskDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "uptime"), []byte("123.45 100.00\n"), 0o600))
	stats, start, err := collectNamedThreadStats(root, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), start)
	require.Equal(t, []threadStat{{tid: 42, unreadable: true}}, stats)
	for _, raw := range []string{"invalid", threadStatFixture(99, "pool", 10, 1)} {
		require.NoError(t, os.WriteFile(filepath.Join(taskDir, "stat"), []byte(raw), 0o600))
		stats, _, err = collectNamedThreadStats(root, 100)
		require.NoError(t, err)
		require.True(t, stats[0].unreadable)
	}
	require.NoError(t, os.WriteFile(filepath.Join(taskDir, "stat"), []byte(threadStatFixture(42, "pool", 10, 1)), 0o600))
	stats, _, err = collectNamedThreadStats(root, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(10), stats[0].cpu.user)
	require.False(t, stats[0].unreadable)
	for _, uptime := range []string{"", "bad", "NaN", "+Inf", "-1", "1e30"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, "uptime"), []byte(uptime), 0o600))
		current, boundary, err := collectNamedThreadStats(root, 100)
		require.NoError(t, err, uptime)
		require.Zero(t, boundary, uptime)
		require.Equal(t, stats, current, uptime)
	}
	_, _, err = collectNamedThreadStats(root, 0)
	require.NoError(t, err, "thread counts still work without CPU frequency")
	require.NoError(t, os.RemoveAll(filepath.Join(root, "self", "task")))
	_, _, err = collectNamedThreadStats(root, 0)
	require.Error(t, err)
}

func TestThreadSamplingWithoutUptime(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux thread sampling")
	}
	root := t.TempDir()
	taskDir := filepath.Join(root, "self", "task", "42")
	require.NoError(t, os.MkdirAll(taskDir, 0o700))
	writeStat := func(user, system uint64) {
		raw := fmt.Sprintf("42 (HIGH_SEGC_POOL) S 1 2 3 4 5 6 7 8 9 10 %d %d 0 0 0 0 0 0 200", user, system)
		require.NoError(t, os.WriteFile(filepath.Join(taskDir, "stat"), []byte(raw), 0o600))
	}
	collect := collectNamedThreadStats
	patch := mockey.Mock(collectNamedThreadStats).To(func(_ string, ticks float64) ([]threadStat, uint64, error) {
		return collect(root, ticks)
	}).Origin(&collect).Build()
	defer patch.UnPatch()
	metrics.ThreadPoolCPUSeconds.Reset()
	metrics.ThreadCPUActiveNumByPool.Reset()
	t.Cleanup(func() { metrics.ThreadPoolCPUSeconds.Reset(); metrics.ThreadCPUActiveNumByPool.Reset() })
	watcher := &threadWatcher{clockTicks: 100}
	user := metrics.ThreadPoolCPUSeconds.WithLabelValues("segcore_high", "user")
	system := metrics.ThreadPoolCPUSeconds.WithLabelValues("segcore_high", "system")
	active := metrics.ThreadCPUActiveNumByPool.WithLabelValues("segcore_high")

	writeStat(100, 50)
	watcher.updateNamedThreadMetrics()
	require.Len(t, watcher.samples, 1)
	require.Zero(t, testutil.ToFloat64(user))
	require.Zero(t, testutil.ToFloat64(system))
	writeStat(130, 70)
	watcher.updateNamedThreadMetrics()
	require.InDelta(t, 0.3, testutil.ToFloat64(user), 1e-9)
	require.InDelta(t, 0.2, testutil.ToFloat64(system), 1e-9)
	require.Equal(t, 1.0, testutil.ToFloat64(active))

	// Recovering uptime must not import CPU already present in the baseline.
	require.NoError(t, os.WriteFile(filepath.Join(root, "uptime"), []byte("3 0"), 0o600))
	watcher.updateNamedThreadMetrics()
	require.Equal(t, uint64(300), watcher.scanStart)
	require.InDelta(t, 0.3, testutil.ToFloat64(user), 1e-9)
	require.InDelta(t, 0.2, testutil.ToFloat64(system), 1e-9)
	require.NoError(t, os.Remove(filepath.Join(root, "uptime")))
	watcher.updateNamedThreadMetrics()
	require.Zero(t, watcher.scanStart, "do not retain a stale birth-time boundary")

	// A directory in place of uptime also causes a read error, even as root.
	require.NoError(t, os.Mkdir(filepath.Join(root, "uptime"), 0o700))
	writeStat(140, 80)
	watcher.updateNamedThreadMetrics()
	require.InDelta(t, 0.4, testutil.ToFloat64(user), 1e-9)
	require.InDelta(t, 0.3, testutil.ToFloat64(system), 1e-9)
	require.Equal(t, 1.0, testutil.ToFloat64(active))
}

func TestThreadCPUCounterPublication(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux thread sampling")
	}
	metrics.ThreadPoolCPUSeconds.Reset()
	metrics.ThreadCPUActiveNumByPool.Reset()
	t.Cleanup(func() { metrics.ThreadPoolCPUSeconds.Reset(); metrics.ThreadCPUActiveNumByPool.Reset() })
	watcher := &threadWatcher{clockTicks: 250}
	current := []threadStat{{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 1000}, startTime: 1}}
	var collectErr error
	scan := uint64(10)
	patch := mockey.Mock(collectNamedThreadStats).To(func(string, float64) ([]threadStat, uint64, error) {
		return current, scan, collectErr
	}).Build()
	defer patch.UnPatch()
	counter := metrics.ThreadPoolCPUSeconds.WithLabelValues("segcore_high", "user")
	systemCounter := metrics.ThreadPoolCPUSeconds.WithLabelValues("segcore_high", "system")
	watcher.updateNamedThreadMetrics()
	require.Zero(t, testutil.ToFloat64(counter))
	current[0].cpu.user += 500
	current[0].cpu.system += 125
	watcher.updateNamedThreadMetrics()
	require.Equal(t, 2.0, testutil.ToFloat64(counter), "use runtime clock frequency")
	require.Equal(t, 0.5, testutil.ToFloat64(systemCounter))
	collectErr = os.ErrPermission
	current[0].cpu.user += 250
	current[0].cpu.system += 250
	watcher.updateNamedThreadMetrics()
	require.Equal(t, uint64(1500), watcher.samples[1].cpu.user)
	require.Equal(t, 2.0, testutil.ToFloat64(counter))
	collectErr = nil
	watcher.updateNamedThreadMetrics()
	require.Equal(t, 3.0, testutil.ToFloat64(counter))
	require.Equal(t, 1.5, testutil.ToFloat64(systemCounter))
	current = nil
	watcher.updateNamedThreadMetrics()
	require.Empty(t, watcher.samples)
	require.Equal(t, 3.0, testutil.ToFloat64(counter), "exit keeps cumulative CPU")
	require.Equal(t, 1.5, testutil.ToFloat64(systemCounter))
	require.Zero(t, testutil.ToFloat64(metrics.ThreadCPUActiveNumByPool.WithLabelValues("segcore_high")))

	registry := prometheus.NewRegistry()
	metrics.Register(registry)
	families, err := registry.Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		if family.GetName() != "milvus_thread_pool_cpu_seconds_total" {
			continue
		}
		found = true
		require.Len(t, family.Metric, 2*(len(threadNameGroupRules)+1))
		for _, metric := range family.Metric {
			require.Len(t, metric.Label, 2)
			require.Equal(t, "cpu_mode", metric.Label[0].GetName())
			require.Contains(t, []string{"user", "system"}, metric.Label[0].GetValue())
			require.Equal(t, "thread_pool", metric.Label[1].GetName())
		}
	}
	require.True(t, found)
}

func TestCollectRealThreadStats(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux procfs")
	}
	watcher := NewThreadWatcher()
	require.Positive(t, watcher.clockTicks)
	stats, start, err := collectNamedThreadStats("/proc", watcher.clockTicks)
	require.NoError(t, err)
	require.NotEmpty(t, stats)
	require.Positive(t, start)
	found := false
	for _, stat := range stats {
		if stat.tid == int32(os.Getpid()) {
			found = true
			require.False(t, stat.unreadable)
			require.NotEmpty(t, stat.name)
			require.LessOrEqual(t, stat.startTime, start)
		}
	}
	require.True(t, found)
}

func BenchmarkCollectThreadStats(b *testing.B) {
	root := b.TempDir()
	if err := os.WriteFile(filepath.Join(root, "uptime"), []byte("123.45 100"), 0o600); err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= 1000; i++ {
		dir := filepath.Join(root, "self", "task", strconv.Itoa(i))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(threadStatFixture(i, "knowhere_search", 1000, 1)), 0o600); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := collectNamedThreadStats(root, 100); err != nil {
			b.Fatal(err)
		}
	}
}

func TestThreadCPUAgainstKernelRuntime(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux procfs")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	readRuntime := func() uint64 {
		data, err := os.ReadFile("/proc/thread-self/schedstat")
		require.NoError(t, err)
		fields := strings.Fields(string(data))
		require.NotEmpty(t, fields)
		ns, err := strconv.ParseUint(fields[0], 10, 64)
		require.NoError(t, err)
		return ns
	}
	readStat := func() threadStat {
		data, err := os.ReadFile("/proc/thread-self/stat")
		require.NoError(t, err)
		stat, err := parseThreadStat(string(data))
		require.NoError(t, err)
		return stat
	}
	watcher := NewThreadWatcher()
	before := readStat()
	startRuntime := readRuntime()
	deadline := time.Now().Add(5 * time.Second)
	var elapsed uint64
	for elapsed < uint64(100*time.Millisecond) && time.Now().Before(deadline) {
		until := time.Now().Add(10 * time.Millisecond)
		for time.Now().Before(until) {
		}
		elapsed = readRuntime() - startRuntime
	}
	require.GreaterOrEqual(t, elapsed, uint64(100*time.Millisecond), "could not acquire 100ms CPU within 5s")
	after := readStat()
	_, _, previous := collectThreadGroups([]threadStat{before}, nil, 0)
	_, cpu, _ := collectThreadGroups([]threadStat{after}, previous, 0)
	group, _ := classifyThreadName(before.name)
	seconds := (float64(cpu[group].user) + float64(cpu[group].system)) / watcher.clockTicks
	require.Positive(t, seconds)
	// User/system tick quantization can differ from scheduler runtime by two ticks.
	require.InDelta(t, float64(elapsed)/1e9, seconds, 2/watcher.clockTicks+0.005)
	t.Logf("sampled CPU %.3fs, kernel scheduler runtime %.3fs", seconds, float64(elapsed)/1e9)
}

func TestThreadClockUnavailable(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux thread sampling")
	}
	for _, clockErr := range []error{nil, os.ErrPermission} {
		func() {
			patch := mockey.Mock(sysconf.Sysconf).Return(int64(0), clockErr).Build()
			defer patch.UnPatch()
			watcher := NewThreadWatcher()
			require.Zero(t, watcher.clockTicks)
			metrics.ThreadPoolCPUSeconds.Reset()
			defer metrics.ThreadPoolCPUSeconds.Reset()
			watcher.updateNamedThreadMetrics()
			require.NotEmpty(t, watcher.samples, "active thread sampling survives clock failure")
			require.Zero(t, testutil.CollectAndCount(metrics.ThreadPoolCPUSeconds))
		}()
	}
}

func TestThreadCPUModes(t *testing.T) {
	previous := map[int32]threadSample{1: {group: "segcore_high", cpu: threadCPU{user: 100, system: 40}, startTime: 10}}
	for _, tc := range []struct {
		name    string
		current threadCPU
		want    threadCPU
	}{
		{"both modes", threadCPU{user: 125, system: 50}, threadCPU{user: 25, system: 10}},
		{"only system", threadCPU{user: 100, system: 50}, threadCPU{system: 10}},
		{"user rollback", threadCPU{user: 90, system: 50}, threadCPU{system: 10}},
		{"system rollback", threadCPU{user: 125, system: 30}, threadCPU{user: 25}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			active, cpu, _ := collectThreadGroups([]threadStat{{tid: 1, name: "HIGH_SEGC_POOL", cpu: tc.current, startTime: 10}}, previous, 50)
			require.Equal(t, tc.want, cpu["segcore_high"])
			require.Equal(t, 1, active["segcore_high"])
		})
	}
}

func TestParseThreadCPUModesWithNice(t *testing.T) {
	for _, nice := range []int{-10, 0, 10} {
		raw := fmt.Sprintf("123 (worker) S 1 2 3 4 5 6 7 8 9 10 34 56 0 0 20 %d 1 0 789", nice)
		stat, err := parseThreadStat(raw)
		require.NoError(t, err)
		require.Equal(t, threadCPU{user: 34, system: 56}, stat.cpu, "nice=%d", nice)
	}
}

func TestThreadCPUModeLifecycle(t *testing.T) {
	before := threadStat{tid: 1, name: "HIGH_SEGC_POOL", cpu: threadCPU{user: 100, system: 40}, startTime: 10}
	_, cpu, previous := collectThreadGroups([]threadStat{before}, nil, 0)
	require.Empty(t, cpu)
	after := before
	after.name = "LOW_SEGC_POOL"
	after.cpu = threadCPU{user: 125, system: 50}
	_, cpu, next := collectThreadGroups([]threadStat{after}, previous, 50)
	require.Equal(t, threadCPU{user: 25, system: 10}, cpu["segcore_high"])
	require.Zero(t, cpu["segcore_low"])
	_, cpu, next = collectThreadGroups([]threadStat{{tid: 1, unreadable: true}}, next, 60)
	require.Empty(t, cpu)
	after.cpu = threadCPU{user: 150, system: 70}
	_, cpu, next = collectThreadGroups([]threadStat{after}, next, 70)
	require.Equal(t, threadCPU{user: 25, system: 20}, cpu["segcore_low"])
	after.startTime = 90
	after.cpu = threadCPU{user: 10, system: 5}
	_, cpu, _ = collectThreadGroups([]threadStat{after}, next, 80)
	require.Equal(t, threadCPU{user: 10, system: 5}, cpu["segcore_low"], "reused TID must not subtract old mode counters")
}
