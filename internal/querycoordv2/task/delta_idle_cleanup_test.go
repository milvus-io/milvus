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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type idleDeltaOperations struct {
	newTask func(id, collectionID int64) Task
	add     func(Task)
	sub     func(Task)
	get     func(collectionID int64) int
	empty   func() bool
}

func newIdleDeltaOperations(t *testing.T, kind string) idleDeltaOperations {
	t.Helper()
	newReplica := func(collectionID int64) *meta.Replica {
		return meta.NewReplica(&querypb.Replica{ID: collectionID, CollectionID: collectionID})
	}
	if kind == "channel" {
		delta := NewChannelTaskDelta()
		return idleDeltaOperations{
			newTask: func(id, collectionID int64) Task {
				task, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1),
					collectionID, newReplica(collectionID), NewChannelAction(1, ActionTypeGrow, "idle-cleanup"))
				require.NoError(t, err)
				task.SetID(id)
				t.Cleanup(func() { task.Cancel(nil) })
				return task
			},
			add: func(task Task) { delta.Add(task.(*ChannelTask)) },
			sub: func(task Task) { delta.Sub(task.(*ChannelTask)) },
			get: func(collectionID int64) int { return delta.Get(1, collectionID) },
			empty: func() bool {
				return delta.taskIDRecords.Len() == 0 && len(delta.data) == 0 && len(delta.nodeTotalDelta) == 0
			},
		}
	}
	delta := NewSegmentTaskDelta()
	return idleDeltaOperations{
		newTask: func(id, collectionID int64) Task {
			task, err := NewSegmentTask(context.Background(), time.Minute, WrapIDSource(1),
				collectionID, newReplica(collectionID), commonpb.LoadPriority_LOW,
				NewSegmentActionWithScope(1, ActionTypeGrow, "idle-cleanup", id, querypb.DataScope_Historical, 1))
			require.NoError(t, err)
			task.SetID(id)
			t.Cleanup(func() { task.Cancel(nil) })
			return task
		},
		add: func(task Task) { delta.Add(task.(*SegmentTask)) },
		sub: func(task Task) { delta.Sub(task.(*SegmentTask)) },
		get: func(collectionID int64) int {
			return delta.GetSegmentSnapshot([]int64{1}, collectionID, nil).GetByNodeInCollection(1)
		},
		empty: func() bool { return delta.records == nil },
	}
}

func TestTaskDeltaSubReclaimsIdleCache(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			delta := newIdleDeltaOperations(t, kind)
			old := delta.newTask(1, 100)
			other := delta.newTask(2, 200)
			delta.add(old)
			delta.add(other)
			delta.sub(old)
			require.Equal(t, 1, delta.get(200), "idle cleanup must preserve another collection's reservation")

			delta.add(other)
			require.Equal(t, 1, delta.get(200), "duplicate registration must not double-count delta")
			delta.sub(other)
			delta.sub(other)
			require.True(t, delta.empty())
			require.Zero(t, delta.get(100))
			require.Zero(t, delta.get(200))

			// A reclaimed cache must still accept and retire new reservations.
			delta.add(other)
			require.Equal(t, 1, delta.get(200))
			delta.sub(other)
			require.True(t, delta.empty())
		})
	}
}

func TestTaskDeltaSubConcurrentRegistration(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			delta := newIdleDeltaOperations(t, kind)
			const workers, iterations = 8, 32
			tasks := make([][]Task, workers)
			for worker := range workers {
				for iteration := range iterations {
					id := int64(worker*iterations + iteration + 1)
					tasks[worker] = append(tasks[worker], delta.newTask(id, int64(worker+100)))
				}
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			for worker := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for _, task := range tasks[worker] {
						delta.add(task)
						// Only this worker owns this collection, and it has not
						// retired the task yet, regardless of other retirements.
						if got := delta.get(task.CollectionID()); got != 1 {
							t.Errorf("registered delta disappeared: task=%d, got=%d", task.ID(), got)
						}
						delta.sub(task)
					}
				}()
			}
			close(start)
			wg.Wait()
			require.True(t, delta.empty())
		})
	}
}
