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

package task

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"go.uber.org/zap"
)

// Merger merges tasks with the same mergeID.
const waitQueueCap = 256

type Merger[K comparable, R any] struct {
	stopCh    chan struct{}
	wg        sync.WaitGroup
	queues    map[K][]MergeableTask[K, R] // TaskID -> Queue
	waitQueue chan MergeableTask[K, R]
	outCh     chan MergeableTask[K, R]

	stopOnce sync.Once
}

func NewMerger[K comparable, R any]() *Merger[K, R] {
	return &Merger[K, R]{
		stopCh:    make(chan struct{}),
		queues:    make(map[K][]MergeableTask[K, R]),
		waitQueue: make(chan MergeableTask[K, R], waitQueueCap),
		outCh:     make(chan MergeableTask[K, R], Params.QueryCoordCfg.TaskMergeCap.GetAsInt()),
	}
}

func (merger *Merger[K, R]) Start(ctx context.Context) {
	merger.schedule(ctx)
}

func (merger *Merger[K, R]) Stop() {
	merger.stopOnce.Do(func() {
		close(merger.stopCh)
		merger.wg.Wait()
	})
}

func (merger *Merger[K, R]) Chan() <-chan MergeableTask[K, R] {
	return merger.outCh
}

func (merger *Merger[K, R]) schedule(ctx context.Context) {
	merger.wg.Add(1)
	go func() {
		defer merger.wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				close(merger.outCh)
				log.Info("Merger stopped due to context canceled")
				return

			case <-merger.stopCh:
				close(merger.outCh)
				log.Info("Merger stopped")
				return

			case <-ticker.C:
				merger.drain()
				for id := range merger.queues {
					merger.triggerExecution(id)
				}
			}
		}
	}()
}

func (merger *Merger[K, R]) Add(task MergeableTask[K, R]) {
	merger.waitQueue <- task
}

func (merger *Merger[K, R]) drain() {
	for {
		select {
		case task := <-merger.waitQueue:
			queue, ok := merger.queues[task.ID()]
			if !ok {
				queue = []MergeableTask[K, R]{}
			}
			queue = append(queue, task)
			merger.queues[task.ID()] = queue
		default:
			return
		}
	}
}

func (merger *Merger[K, R]) triggerExecution(id K) {
	tasks := merger.queues[id]
	delete(merger.queues, id)

	var task MergeableTask[K, R]
	merged := 0
	for i := 0; i < len(tasks); i++ {
		if merged == 0 {
			task = tasks[i]
		} else {
			task.Merge(tasks[i])
		}
		merged++
		if merged >= Params.QueryCoordCfg.TaskMergeCap.GetAsInt() {
			merger.outCh <- task
			merged = 0
		}
	}

	if merged != 0 {
		merger.outCh <- task
	}

	log.Info("merge tasks done, trigger execution", zap.Any("mergeID", task.ID()))
}
