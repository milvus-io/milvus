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

package datacoord

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCompactionQueue(t *testing.T) {
	t1 := &mixCompactionTask{}
	t1.SetTask(&datapb.CompactionTask{
		PlanID: 3,
		Type:   datapb.CompactionType_MixCompaction,
	})

	t2 := &l0CompactionTask{}
	t2.SetTask(&datapb.CompactionTask{
		PlanID: 1,
		Type:   datapb.CompactionType_Level0DeleteCompaction,
	})

	t3 := &clusteringCompactionTask{}
	t3.SetTask(&datapb.CompactionTask{
		PlanID: 2,
		Type:   datapb.CompactionType_ClusteringCompaction,
	})

	t.Run("default prioritizer", func(t *testing.T) {
		cq := NewCompactionQueue(3, DefaultPrioritizer)
		err := cq.Enqueue(t1)
		assert.NoError(t, err)
		err = cq.Enqueue(t2)
		assert.NoError(t, err)
		err = cq.Enqueue(t3)
		assert.NoError(t, err)
		err = cq.Enqueue(&mixCompactionTask{})
		assert.Error(t, err)

		task, err := cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), task.GetTaskProto().GetPlanID())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), task.GetTaskProto().GetPlanID())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, int64(3), task.GetTaskProto().GetPlanID())
	})

	t.Run("level prioritizer", func(t *testing.T) {
		cq := NewCompactionQueue(3, LevelPrioritizer)
		err := cq.Enqueue(t1)
		assert.NoError(t, err)
		err = cq.Enqueue(t2)
		assert.NoError(t, err)
		err = cq.Enqueue(t3)
		assert.NoError(t, err)
		err = cq.Enqueue(&mixCompactionTask{})
		assert.Error(t, err)

		task, err := cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_Level0DeleteCompaction, task.GetTaskProto().GetType())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_MixCompaction, task.GetTaskProto().GetType())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_ClusteringCompaction, task.GetTaskProto().GetType())
	})

	t.Run("mix first prioritizer", func(t *testing.T) {
		cq := NewCompactionQueue(3, MixFirstPrioritizer)
		err := cq.Enqueue(t1)
		assert.NoError(t, err)
		err = cq.Enqueue(t2)
		assert.NoError(t, err)
		err = cq.Enqueue(t3)
		assert.NoError(t, err)
		err = cq.Enqueue(&mixCompactionTask{})
		assert.Error(t, err)

		task, err := cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_MixCompaction, task.GetTaskProto().GetType())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_Level0DeleteCompaction, task.GetTaskProto().GetType())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_ClusteringCompaction, task.GetTaskProto().GetType())
	})

	t.Run("update prioritizer", func(t *testing.T) {
		cq := NewCompactionQueue(3, LevelPrioritizer)
		err := cq.Enqueue(t1)
		assert.NoError(t, err)
		err = cq.Enqueue(t2)
		assert.NoError(t, err)
		err = cq.Enqueue(t3)
		assert.NoError(t, err)
		err = cq.Enqueue(&mixCompactionTask{})
		assert.Error(t, err)

		task, err := cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, datapb.CompactionType_Level0DeleteCompaction, task.GetTaskProto().GetType())

		cq.UpdatePrioritizer(DefaultPrioritizer)
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), task.GetTaskProto().GetPlanID())
		task, err = cq.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, int64(3), task.GetTaskProto().GetPlanID())
	})
}

// TestSortCompactionPrioritizer pins sort ahead of the compaction each
// prioritizer would otherwise run next. A sort task is what takes its input off
// the growing query path, and a snapshot cannot be captured until it lands, so
// leaving it at the default rank of 1000 put it behind every other compaction
// under both prioritizers.
func TestSortCompactionPrioritizer(t *testing.T) {
	sortTask := &mixCompactionTask{}
	sortTask.SetTask(&datapb.CompactionTask{
		PlanID: 4,
		Type:   datapb.CompactionType_SortCompaction,
	})

	assert.Equal(t, sortCompactionPriority, LevelPrioritizer(sortTask))
	assert.Equal(t, sortCompactionPriority, MixFirstPrioritizer(sortTask))

	mixTask := &mixCompactionTask{}
	mixTask.SetTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction})
	l0Task := &l0CompactionTask{}
	l0Task.SetTask(&datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_Level0DeleteCompaction})
	clusteringTask := &clusteringCompactionTask{}
	clusteringTask.SetTask(&datapb.CompactionTask{PlanID: 3, Type: datapb.CompactionType_ClusteringCompaction})

	// The queue is a heap, so enqueue order must not decide the outcome: sort
	// goes in last here, after every task it is expected to overtake.
	dequeueTypes := func(prioritizer Prioritizer) []datapb.CompactionType {
		cq := NewCompactionQueue(4, prioritizer)
		for _, task := range []CompactionTask{mixTask, l0Task, clusteringTask, sortTask} {
			assert.NoError(t, cq.Enqueue(task))
		}
		types := make([]datapb.CompactionType, 0, 4)
		for {
			task, err := cq.Dequeue()
			if err != nil {
				break
			}
			types = append(types, task.GetTaskProto().GetType())
		}
		return types
	}

	assert.Equal(t, []datapb.CompactionType{
		datapb.CompactionType_Level0DeleteCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_ClusteringCompaction,
	}, dequeueTypes(LevelPrioritizer))

	assert.Equal(t, []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_Level0DeleteCompaction,
		datapb.CompactionType_ClusteringCompaction,
	}, dequeueTypes(MixFirstPrioritizer))
}

func TestBumpSchemaVersionPrioritizer(t *testing.T) {
	task := &bumpSchemaVersionTask{}
	task.SetTask(&datapb.CompactionTask{Type: datapb.CompactionType_BumpSchemaVersionCompaction})

	assert.Equal(t, 10, LevelPrioritizer(task))
	assert.Equal(t, 1, MixFirstPrioritizer(task))
}

func TestConcurrency(t *testing.T) {
	c := 10

	cq := NewCompactionQueue(c, LevelPrioritizer)

	wg := sync.WaitGroup{}
	wg.Add(c)
	for i := 0; i < c; i++ {
		t1 := &mixCompactionTask{}
		t1.SetTask(&datapb.CompactionTask{
			PlanID: int64(i),
			Type:   datapb.CompactionType_MixCompaction,
		})
		go func() {
			err := cq.Enqueue(t1)
			assert.NoError(t, err)
			wg.Done()
		}()
	}

	wg.Wait()

	wg.Add(c)
	for i := 0; i < c; i++ {
		go func() {
			_, err := cq.Dequeue()
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

// TestCompactionQueue_SyncPrioritizer guards against re-prioritizing the whole
// queue on every scheduling tick.
//
// Prioritizer is a func value and cannot be compared with ==. The previous code
// compared &q.prioritizer (address of a struct field) against &p (address of a
// local variable); those addresses are never equal, so the guard was always
// true and the entire queue was re-prioritized and re-heapified every 500ms.
func TestCompactionQueue_SyncPrioritizer(t *testing.T) {
	t1 := &mixCompactionTask{}
	t1.SetTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction})
	t2 := &mixCompactionTask{}
	t2.SetTask(&datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_MixCompaction})

	t.Run("same name does not re-prioritize", func(t *testing.T) {
		// Observe re-prioritization through the stored priorities rather than
		// through an instrumented Prioritizer: SyncPrioritizer resolves the
		// prioritizer by name, so any injected closure is discarded on the very
		// first call and a call counter would stay at zero either way.
		//
		// DefaultPrioritizer is int(PlanID), so mutating PlanID after the queue
		// has been primed makes a recompute observable: the stored priority only
		// changes if updatePrioritizerLocked runs again.
		task := &mixCompactionTask{}
		task.SetTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction})

		cq := NewCompactionQueue(10, DefaultPrioritizer)
		assert.NoError(t, cq.Enqueue(task))

		// First sync adopts the configured prioritizer and primes the priority.
		cq.SyncPrioritizer("default")
		cq.lock.RLock()
		primed := cq.pq[0].priority
		cq.lock.RUnlock()
		assert.EqualValues(t, 1, primed)

		task.SetTask(&datapb.CompactionTask{PlanID: 42, Type: datapb.CompactionType_MixCompaction})

		// Subsequent syncs with an unchanged configuration must be no-ops.
		for i := 0; i < 100; i++ {
			cq.SyncPrioritizer("default")
		}
		cq.lock.RLock()
		after := cq.pq[0].priority
		cq.lock.RUnlock()
		assert.EqualValues(t, primed, after, "queue was re-prioritized despite unchanged configuration")

		// A changed configuration must still recompute, proving the assertion
		// above is not vacuous. It has to switch to "level": MixFirstPrioritizer
		// also returns 1 for a MixCompaction task, so asserting against "mix"
		// would pass whether or not updatePrioritizerLocked ran. LevelPrioritizer
		// returns 10, which makes the three values 1 / 42 / 10 mutually distinct.
		cq.SyncPrioritizer("level")
		cq.lock.RLock()
		recomputed := cq.pq[0].priority
		cq.lock.RUnlock()
		assert.EqualValues(t, 10, LevelPrioritizer(task))
		assert.EqualValues(t, LevelPrioritizer(task), recomputed)
	})

	// The empty string is a settable configuration value, not "unset": the
	// RESTful alterConfig contract treats a present value as a set and only
	// null as a reset, and dataCoord.compaction.taskPrioritizer has no
	// Formatter, so getWithRaw returns "" verbatim. Holding the name in a
	// plain string made its zero value collide with that, and a queue that had
	// not yet adopted a name would silently no-op on SyncPrioritizer("") and
	// keep the prioritizer it was constructed with.
	t.Run("empty configuration value is adopted rather than read as unset", func(t *testing.T) {
		cq := NewCompactionQueue(10, LevelPrioritizer)
		cq.lock.RLock()
		assert.Nil(t, cq.prioritizerName, "constructor takes a func, not a configuration name")
		cq.lock.RUnlock()

		cq.SyncPrioritizer("")

		task := &mixCompactionTask{}
		task.SetTask(&datapb.CompactionTask{PlanID: 7, Type: datapb.CompactionType_MixCompaction})
		assert.NoError(t, cq.Enqueue(task))

		cq.lock.RLock()
		adopted := cq.pq[0].priority
		cq.lock.RUnlock()
		// "" resolves to DefaultPrioritizer, i.e. int(PlanID) == 7. Keeping the
		// constructor's LevelPrioritizer would give 10.
		assert.EqualValues(t, DefaultPrioritizer(task), adopted,
			"empty configuration value must resolve to the default prioritizer")
		assert.NotEqualValues(t, LevelPrioritizer(task), adopted)
	})

	// UpdatePrioritizer sets the func out of band, so the queue must forget
	// which configuration value it corresponds to; otherwise the next sync for
	// that same name would be skipped.
	t.Run("update forgets the configuration name", func(t *testing.T) {
		cq := NewCompactionQueue(10, DefaultPrioritizer)
		cq.SyncPrioritizer("level")
		cq.UpdatePrioritizer(DefaultPrioritizer)
		cq.lock.RLock()
		assert.Nil(t, cq.prioritizerName)
		cq.lock.RUnlock()

		cq.SyncPrioritizer("level")
		task := &mixCompactionTask{}
		task.SetTask(&datapb.CompactionTask{PlanID: 7, Type: datapb.CompactionType_MixCompaction})
		assert.NoError(t, cq.Enqueue(task))
		cq.lock.RLock()
		adopted := cq.pq[0].priority
		cq.lock.RUnlock()
		assert.EqualValues(t, LevelPrioritizer(task), adopted)
	})

	t.Run("changed name does re-prioritize", func(t *testing.T) {
		cq := NewCompactionQueue(10, DefaultPrioritizer)
		assert.NoError(t, cq.Enqueue(t1))

		cq.SyncPrioritizer("level")
		cq.lock.RLock()
		assert.Equal(t, "level", *cq.prioritizerName)
		cq.lock.RUnlock()

		cq.SyncPrioritizer("mix")
		cq.lock.RLock()
		assert.Equal(t, "mix", *cq.prioritizerName)
		cq.lock.RUnlock()
	})

	// UpdatePrioritizer used to assign q.prioritizer before acquiring the lock,
	// racing with concurrent Enqueue/Dequeue which read it under the lock.
	// Run with -race.
	t.Run("no data race with concurrent enqueue", func(t *testing.T) {
		cq := NewCompactionQueue(0, DefaultPrioritizer)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				cq.UpdatePrioritizer(LevelPrioritizer)
				cq.SyncPrioritizer("mix")
				cq.SyncPrioritizer("level")
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				task := &mixCompactionTask{}
				task.SetTask(&datapb.CompactionTask{PlanID: int64(i), Type: datapb.CompactionType_MixCompaction})
				_ = cq.Enqueue(task)
				_, _ = cq.Dequeue()
			}
		}()
		wg.Wait()
	})
}
