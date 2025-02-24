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

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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
