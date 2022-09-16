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

package querynode

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTaskReadCounter(t *testing.T) {
	t.Run("test search nq queue", func(t *testing.T) {
		counter := newReadTaskCounter()
		counter.add(&searchTask{NQ: 100}, unsolvedQueueType)
		counter.add(&searchTask{NQ: 50}, unsolvedQueueType)
		counter.sub(&searchTask{NQ: 20}, unsolvedQueueType)

		counter.add(&searchTask{NQ: -100}, readyQueueType)
		counter.add(&searchTask{NQ: 20}, readyQueueType)
		counter.sub(&searchTask{NQ: 1000}, readyQueueType)
		counter.sub(&searchTask{NQ: -10000}, readyQueueType)

		counter.sub(&searchTask{NQ: 100}, receiveQueueType)

		counter.sub(&searchTask{NQ: 10}, executeQueueType)
		counter.add(&searchTask{NQ: math.MaxInt64}, executeQueueType)
		counter.add(&searchTask{NQ: math.MaxInt64}, executeQueueType)
		counter.sub(&searchTask{NQ: math.MaxInt64}, executeQueueType)

		searchNQMetric := counter.getSearchNQInQueue()
		assert.Equal(t, int64(100+50-20), searchNQMetric.UnsolvedQueue)
		assert.Equal(t, int64(-100+20-1000-(-10000)), searchNQMetric.ReadyQueue)
		assert.Equal(t, int64(-100), searchNQMetric.ReceiveChan)
		assert.Equal(t, int64(-10+math.MaxInt64), searchNQMetric.ExecuteChan)
	})

	t.Run("test query tasks", func(t *testing.T) {
		counter := newReadTaskCounter()
		counter.add(&queryTask{}, unsolvedQueueType)
		counter.add(&queryTask{}, unsolvedQueueType)
		counter.sub(&queryTask{}, unsolvedQueueType)

		counter.add(&queryTask{}, readyQueueType)
		counter.add(&queryTask{}, readyQueueType)
		counter.sub(&queryTask{}, readyQueueType)
		counter.sub(&queryTask{}, readyQueueType)

		counter.sub(&queryTask{}, receiveQueueType)

		for i := 0; i < 200; i++ {
			counter.sub(&queryTask{}, executeQueueType)
		}
		for i := 0; i < 1000; i++ {
			counter.add(&queryTask{}, executeQueueType)
		}

		queryTasksMetric := counter.getQueryTasksInQueue()
		assert.Equal(t, int64(1), queryTasksMetric.UnsolvedQueue)
		assert.Equal(t, int64(0), queryTasksMetric.ReadyQueue)
		assert.Equal(t, int64(-1), queryTasksMetric.ReceiveChan)
		assert.Equal(t, int64(-200+1000), queryTasksMetric.ExecuteChan)
	})

	t.Run("test queueTime", func(t *testing.T) {
		getBaseTask := func(queueDur, waitTSDur time.Duration) baseReadTask {
			return baseReadTask{
				queueDur:  queueDur,
				waitTsDur: waitTSDur,
			}
		}

		counter := newReadTaskCounter()
		increaseNum := 100
		expected := time.Duration(0)
		for i := 0; i < increaseNum; i++ {
			queueDur := time.Duration(rand.Int63n(100000))
			waitDur := time.Duration(rand.Int63n(100000))
			counter.increaseQueueTime(&searchTask{baseReadTask: getBaseTask(queueDur, waitDur)})
			expected += queueDur - waitDur
		}
		assert.Equal(t, expected/time.Duration(increaseNum), counter.getSearchNQInQueue().AvgQueueDuration)

		expected = time.Duration(0)
		for i := 0; i < increaseNum; i++ {
			queueDur := time.Duration(rand.Int63n(100000))
			waitDur := time.Duration(rand.Int63n(100000))
			counter.increaseQueueTime(&queryTask{baseReadTask: getBaseTask(queueDur, waitDur)})
			expected += queueDur - waitDur
		}
		assert.Equal(t, expected/time.Duration(increaseNum), counter.getQueryTasksInQueue().AvgQueueDuration)

		counter.resetQueueTime()
		assert.Equal(t, time.Duration(0), counter.getSearchNQInQueue().AvgQueueDuration)
		assert.Equal(t, time.Duration(0), counter.getQueryTasksInQueue().AvgQueueDuration)
	})
}
