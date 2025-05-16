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
	"os"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
)

// theadWatcher is the utility to update milvus process thread number metrics.
// the os thread number metrics is not accurate since it only returns thread number used by golang "normal" runtime
// and the crucial threads number in cpp side is not included.
type threadWatcher struct {
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
}

func NewThreadWatcher() *threadWatcher {
	return &threadWatcher{
		ch: make(chan struct{}),
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
	ticker := time.NewTicker(time.Second * 30)
	defer ticker.Stop()
	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		log.Warn("thread watcher failed to get milvus process info, quit", zap.Int("pid", pid), zap.Error(err))
		return
	}
	for {
		select {
		case <-ticker.C:
			threadNum, err := p.NumThreads()
			if err != nil {
				log.Warn("thread watcher failed to get process", zap.Int("pid", pid), zap.Error(err))
				continue
			}
			log.Debug("thread watcher observe thread num", zap.Int32("threadNum", threadNum))
			metrics.ThreadNum.Set(float64(threadNum))
		case <-thw.ch:
			log.Info("thread watcher exit")
			return
		}
	}
}

func (thw *threadWatcher) Stop() {
	thw.stopOnce.Do(func() {
		close(thw.ch)
		thw.wg.Wait()
	})
}
