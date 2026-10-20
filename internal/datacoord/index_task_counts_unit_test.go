// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestTaskCountAccumulatorAgainstScan(t *testing.T) {
	var counts indexTaskCounts
	tasks := make(map[int64]*model.SegmentIndex)
	indexes := make(map[[2]int64]*model.Index)
	rng := rand.New(rand.NewSource(12))
	for range 1000 {
		id := int64(rng.Intn(32))
		key := [2]int64{id % 3, id % 5}
		switch rng.Intn(4) {
		case 0:
			current := &model.Index{CollectionID: key[0], IndexID: key[1], IsDeleted: rng.Intn(2) == 0}
			counts.replaceIndex(indexes[key], current)
			indexes[key] = current
		case 1:
			counts.replaceIndex(indexes[key], nil)
			delete(indexes, key)
		case 2:
			current := &model.SegmentIndex{
				CollectionID: key[0], IndexID: key[1], BuildID: id,
				IndexState: commonpb.IndexState(rng.Intn(8)), IsDeleted: rng.Intn(4) == 0,
			}
			counts.replaceTask(tasks[id], current)
			tasks[id] = current
		case 3:
			counts.replaceTask(tasks[id], nil)
			delete(tasks, id)
		}
		var expected indexTaskStateCounts
		for _, task := range tasks {
			idx := indexes[[2]int64{task.CollectionID, task.IndexID}]
			if idx != nil && !idx.IsDeleted && !task.IsDeleted && int(task.IndexState) >= 0 && int(task.IndexState) < len(expected) {
				expected[int(task.IndexState)]++
			}
		}
		require.Equal(t, expected, counts.snapshot())
	}
}

func TestTaskCountAccumulatorConcurrentPublication(t *testing.T) {
	var counts indexTaskCounts
	index := &model.Index{CollectionID: 1, IndexID: 2}
	counts.replaceIndex(nil, index)
	const workers = 24
	var wg sync.WaitGroup
	for id := range workers {
		wg.Go(func() {
			var previous *model.SegmentIndex
			for i := range 100 {
				current := &model.SegmentIndex{CollectionID: 1, IndexID: 2, BuildID: int64(id), IndexState: commonpb.IndexState(i % 6)}
				counts.replaceTask(previous, current)
				previous = current
				for _, count := range counts.snapshot() {
					if count < 0 {
						t.Error("negative count")
					}
				}
			}
			counts.replaceTask(previous, nil)
		})
	}
	wg.Go(func() {
		for range 100 {
			counts.replaceIndex(index, nil)
			counts.replaceIndex(nil, index)
		}
	})
	wg.Wait()
	require.Equal(t, indexTaskStateCounts{}, counts.snapshot())
	require.Empty(t, counts.byIndex)
	require.Zero(t, testing.AllocsPerRun(100, func() { counts.snapshot() }))
}
