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
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// Merger merges tasks with the same mergeID.
const waitQueueCap = 256

type Merger[K comparable, R any] struct {
	stopCh chan struct{}
	wg     sync.WaitGroup

	processors *typeutil.ConcurrentSet[K]     // Tasks of having processor
	queues     map[K]chan MergeableTask[K, R] // TaskID -> Queue
	waitQueue  chan MergeableTask[K, R]
	outCh      chan MergeableTask[K, R]

	stopOnce sync.Once
}

func NewMerger[K comparable, R any]() *Merger[K, R] {
	return &Merger[K, R]{
		stopCh:     make(chan struct{}),
		processors: typeutil.NewConcurrentSet[K](),
		queues:     make(map[K]chan MergeableTask[K, R]),
		waitQueue:  make(chan MergeableTask[K, R], waitQueueCap),
		outCh:      make(chan MergeableTask[K, R], Params.QueryCoordCfg.TaskMergeCap),
	}
}

func (merger *Merger[K, R]) Start(ctx context.Context) {
	merger.schedule(ctx)
}

func (merger *Merger[K, R]) Stop() {
	merger.stopOnce.Do(func() {
		close(merger.stopCh)
		merger.wg.Wait()
		close(merger.outCh)
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
		for {
			select {
			case <-ctx.Done():
				log.Info("Merger stopped due to context canceled")
				return

			case <-merger.stopCh:
				log.Info("Merger stopped")
				return

			case task := <-merger.waitQueue:
				queue, ok := merger.queues[task.ID()]
				if !ok {
					queue = make(chan MergeableTask[K, R], Params.QueryCoordCfg.TaskMergeCap)
					merger.queues[task.ID()] = queue
				}
			outer:
				for {
					select {
					case queue <- task:
						break outer
					default: // Queue full, flush and retry
						merger.merge(task.ID(), queue)
					}
				}

			case <-ticker.C:
				for id, queue := range merger.queues {
					if len(queue) > 0 {
						merger.merge(id, queue)
					} else {
						// Release resource if no task for the queue
						delete(merger.queues, id)
					}
				}
			}
		}
	}()
}

func (merger *Merger[K, R]) isStopped() bool {
	select {
	case <-merger.stopCh:
		return true
	default:
		return false
	}
}

func (merger *Merger[K, R]) Add(task MergeableTask[K, R]) {
	merger.waitQueue <- task
}

func (merger *Merger[K, R]) merge(id K, queue chan MergeableTask[K, R]) {
	if merger.isStopped() {
		return
	}
	if !merger.processors.Insert(id) {
		return
	}

	merger.wg.Add(1)
	go merger.mergeQueue(id, queue)
}

// mergeQueue merges tasks in the given queue,
// it only processes tasks with the number of the length of queue at the time,
// to avoid leaking goroutines
func (merger *Merger[K, R]) mergeQueue(id K, queue chan MergeableTask[K, R]) {
	defer merger.wg.Done()
	defer merger.processors.Remove(id)

	len := len(queue)
	task := <-queue
	for i := 1; i < len; i++ {
		task.Merge(<-queue)
	}

	log.Info("merge tasks done",
		zap.Any("mergeID", task.ID()))
	merger.outCh <- task
}
