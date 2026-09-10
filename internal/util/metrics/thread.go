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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// theadWatcher is the utility to update milvus process thread number metrics.
// the os thread number metrics is not accurate since it only returns thread number used by golang "normal" runtime
// and the crucial threads number in cpp side is not included.
type threadWatcher struct {
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	samples   map[int32]threadSample
}

type threadSample struct {
	group string
	cpu   uint64
}

type threadStat struct {
	tid   int32
	name  string
	state byte
	cpu   uint64
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
	{prefix: "MILVUS_FL_WR", group: "file_write"},
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
	return &threadWatcher{
		ch:      make(chan struct{}),
		samples: make(map[int32]threadSample),
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
			thw.updateNamedThreadCPUActiveNum()
		case <-thw.ch:
			mlog.Info(context.TODO(), "thread watcher exit")
			return
		}
	}
}

func (thw *threadWatcher) updateNamedThreadCPUActiveNum() {
	if runtime.GOOS != "linux" {
		return
	}

	current, err := collectNamedThreadStats()
	if err != nil {
		mlog.Warn(context.TODO(), "thread watcher failed to collect named thread stats", mlog.Err(err))
		return
	}

	activeByGroup, nextSamples := collectActiveThreadGroups(current, thw.samples)

	for _, rule := range threadNameGroupRules {
		metrics.ThreadCPUActiveNumByPool.WithLabelValues(rule.group).Set(float64(activeByGroup[rule.group]))
	}
	metrics.ThreadCPUActiveNumByPool.WithLabelValues(unclassifiedThreadPool).Set(float64(activeByGroup[unclassifiedThreadPool]))
	thw.samples = nextSamples
}

func collectActiveThreadGroups(current []threadStat, previous map[int32]threadSample) (map[string]int, map[int32]threadSample) {
	activeByGroup := make(map[string]int)
	nextSamples := make(map[int32]threadSample, len(current))
	for _, stat := range current {
		group, classified := classifyThreadName(stat.name)
		if !classified {
			group = unclassifiedThreadPool
		}
		nextSamples[stat.tid] = threadSample{
			group: group,
			cpu:   stat.cpu,
		}

		previousSample, existed := previous[stat.tid]
		if !existed {
			continue
		}
		if previousSample.group == group && (stat.cpu > previousSample.cpu || stat.state == 'R' || stat.state == 'D') {
			activeByGroup[group]++
		}
	}
	return activeByGroup, nextSamples
}

func collectNamedThreadStats() ([]threadStat, error) {
	taskDir := "/proc/self/task"
	entries, err := os.ReadDir(taskDir)
	if err != nil {
		return nil, err
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
		tid := int32(tid64)
		nameBytes, err := os.ReadFile(filepath.Join(taskDir, entry.Name(), "comm"))
		if err != nil {
			continue
		}
		statBytes, err := os.ReadFile(filepath.Join(taskDir, entry.Name(), "stat"))
		if err != nil {
			continue
		}
		state, cpu, err := parseThreadStat(string(statBytes))
		if err != nil {
			continue
		}
		stats = append(stats, threadStat{
			tid:   tid,
			name:  strings.TrimSpace(string(nameBytes)),
			state: state,
			cpu:   cpu,
		})
	}
	return stats, nil
}

func parseThreadStat(stat string) (byte, uint64, error) {
	endComm := strings.LastIndexByte(stat, ')')
	if endComm < 0 || endComm+2 >= len(stat) {
		return 0, 0, strconv.ErrSyntax
	}

	fields := strings.Fields(stat[endComm+2:])
	if len(fields) <= 12 || len(fields[0]) == 0 {
		return 0, 0, strconv.ErrSyntax
	}
	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return fields[0][0], utime + stime, nil
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
