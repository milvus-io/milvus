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
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/tklauser/go-sysconf"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// theadWatcher is the utility to update milvus process thread number metrics.
// the os thread number metrics is not accurate since it only returns thread number used by golang "normal" runtime
// and the crucial threads number in cpp side is not included.
type threadWatcher struct {
	startOnce  sync.Once
	stopOnce   sync.Once
	wg         sync.WaitGroup
	ch         chan struct{}
	samples    map[int32]threadSample
	clockTicks float64
	scanStart  uint64
}

type threadCPU struct {
	user   uint64
	system uint64
}

type threadSample struct {
	group     string
	cpu       threadCPU
	startTime uint64
}

type threadStat struct {
	tid        int32
	name       string
	state      byte
	cpu        threadCPU
	startTime  uint64
	unreadable bool
}

var threadNameGroupRules = []struct {
	prefix string
	group  string
}{
	{prefix: "knowhere_build", group: "knowhere_build"},
	{prefix: "knowhere_search", group: "knowhere_search"},
	{prefix: "knowhere_fetch", group: "knowhere_fetch"},
	{prefix: "rocksdb:high", group: "rocksdb_high"},
	{prefix: "rocksdb:low", group: "rocksdb_low"},
	{prefix: "rocksdb:bottom", group: "rocksdb_bottom"},
	{prefix: "AwsEventLoop", group: "aws_crt_event_loop"},
	{prefix: "AwsHostResolver", group: "aws_crt_resolver"},
	{prefix: "EvntLoopCleanup", group: "aws_crt_cleanup"},
	{prefix: "MILVUS_FL_WR", group: "file_write"},
	{prefix: "MILVUS_LF_IO", group: "local_file_io"},
	{prefix: "MILVUS_ASYNC_LOA", group: "async_load"},
	{prefix: "MILVUS_PREFETCH", group: "prefetch"},
	{prefix: "MILVUS_SEARCH", group: "milvus_search"},
	{prefix: "MILVUS_LOAD", group: "milvus_load"},
	{prefix: "HIGH_SEGC_POOL", group: "segcore_high"},
	{prefix: "MIDD_SEGC_POOL", group: "segcore_middle"},
	{prefix: "LOW_SEGC_POOL", group: "segcore_low"},
	{prefix: "CGO_SQ", group: "cgo_sq"},
	{prefix: "CGO_LOAD", group: "cgo_load"},
	{prefix: "CGO_DYN", group: "cgo_dynamic"},
	{prefix: "CGO_WARMUP", group: "cgo_warmup"},
}

const (
	threadWatcherInterval  = 5 * time.Second
	unclassifiedThreadPool = "unclassified"
)

func NewThreadWatcher() *threadWatcher {
	var clockTicks int64
	if runtime.GOOS == "linux" {
		var err error
		clockTicks, err = sysconf.Sysconf(sysconf.SC_CLK_TCK)
		if err != nil || clockTicks <= 0 {
			mlog.Warn(context.TODO(), "thread watcher cannot obtain clock frequency; CPU counters disabled", mlog.Err(err))
			clockTicks = 0
		}
	}
	return &threadWatcher{
		clockTicks: float64(clockTicks),
		ch:         make(chan struct{}),
		samples:    make(map[int32]threadSample),
	}
}

func (thw *threadWatcher) Start() {
	thw.startOnce.Do(func() {
		thw.wg.Add(1)
		go func() {
			defer thw.wg.Done()
			thw.watchThreadNum()
		}()
	})
}

func (thw *threadWatcher) watchThreadNum() {
	ticker := time.NewTicker(threadWatcherInterval)
	defer ticker.Stop()
	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		mlog.Warn(context.TODO(), "thread watcher failed to get milvus process info, quit", mlog.Int("pid", pid), mlog.Err(err))
		return
	}
	for {
		select {
		case <-ticker.C:
			threadNum, err := p.NumThreads()
			if err != nil {
				mlog.Warn(context.TODO(), "thread watcher failed to get process", mlog.Int("pid", pid), mlog.Err(err))
				continue
			}
			mlog.Debug(context.TODO(), "thread watcher observe thread num", mlog.Int32("threadNum", threadNum))
			metrics.ThreadNum.Set(float64(threadNum))
			thw.updateNamedThreadMetrics()
		case <-thw.ch:
			mlog.Info(context.TODO(), "thread watcher exit")
			return
		}
	}
}

func (thw *threadWatcher) updateNamedThreadMetrics() {
	if runtime.GOOS != "linux" {
		return
	}

	current, scanStart, err := collectNamedThreadStats("/proc", thw.clockTicks)
	if err != nil {
		mlog.Warn(context.TODO(), "thread watcher failed to collect named thread stats", mlog.Err(err))
		return
	}

	activeByGroup, cpuByGroup, nextSamples := collectThreadGroups(current, thw.samples, thw.scanStart)

	publish := func(group string) {
		metrics.ThreadCPUActiveNumByPool.WithLabelValues(group).Set(float64(activeByGroup[group]))
		if thw.clockTicks > 0 {
			cpu := cpuByGroup[group]
			metrics.ThreadPoolCPUSeconds.WithLabelValues(group, "user").Add(float64(cpu.user) / thw.clockTicks)
			metrics.ThreadPoolCPUSeconds.WithLabelValues(group, "system").Add(float64(cpu.system) / thw.clockTicks)
		}
	}
	for _, rule := range threadNameGroupRules {
		publish(rule.group)
	}
	publish(unclassifiedThreadPool)
	thw.samples = nextSamples
	thw.scanStart = scanStart
}

func collectThreadGroups(current []threadStat, previous map[int32]threadSample, since uint64) (map[string]int, map[string]threadCPU, map[int32]threadSample) {
	cpuByGroup := make(map[string]threadCPU)
	activeByGroup := make(map[string]int)
	nextSamples := make(map[int32]threadSample, len(current))
	for _, stat := range current {
		if stat.unreadable {
			if sample, ok := previous[stat.tid]; ok {
				nextSamples[stat.tid] = sample
			}
			continue
		}
		group, classified := classifyThreadName(stat.name)
		if !classified {
			group = unclassifiedThreadPool
		}
		nextSamples[stat.tid] = threadSample{
			group:     group,
			cpu:       stat.cpu,
			startTime: stat.startTime,
		}

		previousSample, existed := previous[stat.tid]
		if !existed || previousSample.startTime != stat.startTime {
			// A birth on the boundary tick is ambiguous; do not import old CPU.
			if since > 0 && stat.startTime > since {
				cpu := cpuByGroup[group]
				cpu.user += stat.cpu.user
				cpu.system += stat.cpu.system
				cpuByGroup[group] = cpu
			}
			continue
		}
		// Attribute the interval to its original pool; never move historic CPU.
		cpu := cpuByGroup[previousSample.group]
		if stat.cpu.user >= previousSample.cpu.user {
			cpu.user += stat.cpu.user - previousSample.cpu.user
		}
		if stat.cpu.system >= previousSample.cpu.system {
			cpu.system += stat.cpu.system - previousSample.cpu.system
		}
		cpuByGroup[previousSample.group] = cpu
		if previousSample.group == group && (stat.cpu.user > previousSample.cpu.user || stat.cpu.system > previousSample.cpu.system || stat.state == 'R' || stat.state == 'D') {
			activeByGroup[group]++
		}
	}
	return activeByGroup, cpuByGroup, nextSamples
}

func collectNamedThreadStats(procRoot string, clockTicks float64) ([]threadStat, uint64, error) {
	var scanStart uint64
	if clockTicks > 0 {
		// Uptime may be hidden by subset=pid; only new-thread lifetime CPU needs it.
		if uptime, err := os.ReadFile(filepath.Join(procRoot, "uptime")); err == nil {
			if fields := strings.Fields(string(uptime)); len(fields) > 0 {
				seconds, err := strconv.ParseFloat(fields[0], 64)
				if err == nil && seconds >= 0 && !math.IsNaN(seconds) && !math.IsInf(seconds, 0) && seconds*clockTicks < math.Exp2(64) {
					// Proc uptime is rounded to centiseconds. Exclude the boundary tick.
					scanStart = uint64(math.Ceil(seconds * clockTicks))
				}
			}
		}
	}
	taskDir := filepath.Join(procRoot, "self", "task")
	entries, err := os.ReadDir(taskDir)
	if err != nil {
		return nil, 0, err
	}
	stats := make([]threadStat, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		tid64, err := strconv.ParseInt(entry.Name(), 10, 32)
		if err != nil {
			continue
		}
		stat := threadStat{tid: int32(tid64), unreadable: true}
		statBytes, err := os.ReadFile(filepath.Join(taskDir, entry.Name(), "stat"))
		if err == nil {
			parsed, err := parseThreadStat(string(statBytes))
			if err == nil && parsed.tid == stat.tid {
				stat = parsed
			}
		}
		// Keep failed reads in the snapshot so an existing baseline survives.
		stats = append(stats, stat)
	}
	return stats, scanStart, nil
}

func parseThreadStat(raw string) (threadStat, error) {
	startComm := strings.IndexByte(raw, '(')
	endComm := strings.LastIndexByte(raw, ')')
	if startComm < 1 || endComm <= startComm || endComm+2 >= len(raw) {
		return threadStat{}, merr.WrapErrServiceInternalMsg("invalid proc thread stat")
	}
	tid, err := strconv.ParseInt(strings.TrimSpace(raw[:startComm]), 10, 32)
	if err != nil {
		return threadStat{}, err
	}
	fields := strings.Fields(raw[endComm+2:])
	if len(fields) <= 19 || len(fields[0]) != 1 {
		return threadStat{}, merr.WrapErrServiceInternalMsg("incomplete proc thread stat")
	}
	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return threadStat{}, err
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return threadStat{}, err
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return threadStat{}, err
	}
	return threadStat{tid: int32(tid), name: raw[startComm+1 : endComm], state: fields[0][0], cpu: threadCPU{user: utime, system: stime}, startTime: startTime}, nil
}

func classifyThreadName(name string) (string, bool) {
	for _, rule := range threadNameGroupRules {
		if strings.HasPrefix(name, rule.prefix) {
			return rule.group, true
		}
	}
	return unclassifiedThreadPool, false
}

func (thw *threadWatcher) Stop() {
	thw.stopOnce.Do(func() {
		close(thw.ch)
		thw.wg.Wait()
	})
}
