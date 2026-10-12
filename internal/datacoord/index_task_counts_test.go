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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type taskCountsCatalog struct {
	metastore.DataCoordCatalog
	indexes []*model.Index
	tasks   []*model.SegmentIndex
	err     error
}

func (c *taskCountsCatalog) ListIndexes(context.Context) ([]*model.Index, error) {
	return c.indexes, c.err
}

func (c *taskCountsCatalog) ListSegmentIndexes(_ context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
	var tasks []*model.SegmentIndex
	for _, task := range c.tasks {
		if task.CollectionID == collectionID {
			tasks = append(tasks, task)
		}
	}
	return tasks, c.err
}
func (c *taskCountsCatalog) CreateIndex(context.Context, *model.Index) error    { return c.err }
func (c *taskCountsCatalog) AlterIndexes(context.Context, []*model.Index) error { return c.err }
func (c *taskCountsCatalog) CreateSegmentIndex(context.Context, *model.SegmentIndex) error {
	return c.err
}

func (c *taskCountsCatalog) AlterSegmentIndexes(context.Context, []*model.SegmentIndex) error {
	return c.err
}

func (c *taskCountsCatalog) DropSegmentIndex(context.Context, int64, int64, int64, int64) error {
	return c.err
}

func (c *taskCountsCatalog) DropSegmentIndexes(context.Context, []*model.SegmentIndex) error {
	return c.err
}
func (c *taskCountsCatalog) DropIndex(context.Context, int64, int64) error     { return c.err }
func (c *taskCountsCatalog) DropIndexes(context.Context, []*model.Index) error { return c.err }

// The reference retains the old full-scan predicate. Tests compare after every
// publication, including failed persistence, not just the final aggregate.
func scanIndexTaskCounts(m *indexMeta) indexTaskStateCounts {
	var counts indexTaskStateCounts
	for _, task := range m.segmentBuildInfo.List() {
		if task.IsDeleted || !m.IsIndexExist(task.CollectionID, task.IndexID) {
			continue
		}
		// These are the existing wire enum values, independently of the new mapper.
		if state := int(task.IndexState); state >= 0 && state < len(counts) {
			counts[state]++
		}
	}
	return counts
}

func assertIndexTaskCounts(t *testing.T, m *indexMeta) {
	t.Helper()
	require.Equal(t, scanIndexTaskCounts(m), m.indexTaskCountsSnapshot())
	for _, counts := range m.taskCounts.byIndex {
		for _, count := range counts {
			require.GreaterOrEqual(t, count, int64(0))
		}
		require.NotEqual(t, indexTaskStateCounts{}, counts, "empty buckets must be reclaimed")
	}
}

func TestIndexTaskCountsRecovery(t *testing.T) {
	catalog := &taskCountsCatalog{indexes: []*model.Index{
		{CollectionID: 1, IndexID: 10},
		{CollectionID: 2, IndexID: 10, IsDeleted: true},
	}}
	for state := 0; state < 6; state++ {
		for _, coll := range []int64{1, 2, 3} {
			catalog.tasks = append(catalog.tasks, &model.SegmentIndex{
				CollectionID: coll, IndexID: 10, SegmentID: int64(state)*10 + coll,
				BuildID: int64(state)*10 + coll, IndexState: commonpb.IndexState(state),
			})
		}
	}
	// Deleted and unknown-state tasks must not contribute.
	catalog.tasks = append(catalog.tasks,
		&model.SegmentIndex{CollectionID: 1, IndexID: 10, BuildID: 100, IndexState: commonpb.IndexState_Finished, IsDeleted: true},
		&model.SegmentIndex{CollectionID: 1, IndexID: 10, BuildID: 101, IndexState: commonpb.IndexState(100)},
	)
	m, err := newIndexMeta(context.Background(), catalog, []int64{1, 2, 3})
	require.NoError(t, err)
	assertIndexTaskCounts(t, m)
	require.Equal(t, indexTaskStateCounts{1, 1, 1, 1, 1, 1}, m.indexTaskCountsSnapshot())
	m.updateIndexTasksMetrics()
	for _, label := range []string{"JobStateNone", "JobStateInit", "JobStateInProgress", "JobStateFinished", "JobStateFailed", "JobStateRetry"} {
		require.Equal(t, float64(1), testutil.ToFloat64(metrics.IndexStatsTaskNum.WithLabelValues("JobTypeIndexJob", label)))
	}
	// Creating the missing field index activates its existing raw counts.
	require.NoError(t, m.CreateIndex(context.Background(), &model.Index{CollectionID: 3, IndexID: 10}))
	assertIndexTaskCounts(t, m)
	require.Equal(t, indexTaskStateCounts{2, 2, 2, 2, 2, 2}, m.indexTaskCountsSnapshot())
	require.NoError(t, m.MarkIndexAsDeleted(context.Background(), 1, nil))
	require.NoError(t, m.MarkIndexAsDeleted(context.Background(), 3, nil))
	m.updateIndexTasksMetrics()
	for _, state := range indexTaskMetricStates {
		require.Zero(t, testutil.ToFloat64(metrics.IndexStatsTaskNum.WithLabelValues(indexpb.JobType_JobTypeIndexJob.String(), state.String())))
	}
	assertIndexTaskCounts(t, m)
}

func TestIndexTaskCountsLifecycle(t *testing.T) {
	ctx := context.Background()
	catalog := &taskCountsCatalog{}
	m, err := newIndexMeta(ctx, catalog, []int64{1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, m.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10}))
	task := &model.SegmentIndex{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: 30}
	require.NoError(t, m.AddSegmentIndex(ctx, task))
	assertIndexTaskCounts(t, m)
	// An identical ID retry replaces rather than increments.
	require.NoError(t, m.AddSegmentIndex(ctx, model.CloneSegmentIndex(task)))
	require.Equal(t, indexTaskStateCounts{0, 1}, m.indexTaskCountsSnapshot())
	require.NoError(t, m.BuildIndex(30))
	require.NoError(t, m.UpdateVersion(30, 100))
	assertIndexTaskCounts(t, m)
	for _, state := range []commonpb.IndexState{commonpb.IndexState_Failed, commonpb.IndexState_Retry, commonpb.IndexState_InProgress} {
		require.NoError(t, m.UpdateIndexState(30, state, ""))
		assertIndexTaskCounts(t, m)
	}
	finish := &workerpb.IndexTaskInfo{BuildID: 30, State: commonpb.IndexState_Finished}
	require.NoError(t, m.FinishTask(finish))
	require.NoError(t, m.FinishTask(finish))
	require.Equal(t, indexTaskStateCounts{0, 0, 0, 1}, m.indexTaskCountsSnapshot())
	require.NoError(t, m.MarkIndexAsDeleted(ctx, 1, []int64{10}))
	require.NoError(t, m.MarkIndexAsDeleted(ctx, 1, []int64{10}))
	require.NoError(t, m.FinishTask(finish))
	assertIndexTaskCounts(t, m)
	require.Equal(t, indexTaskStateCounts{}, m.indexTaskCountsSnapshot(), "late completion cannot reactivate a dropped index")
	// Replacing field metadata must re-activate retained counts.
	require.NoError(t, m.AlterIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10}))
	assertIndexTaskCounts(t, m)
	require.NoError(t, m.DeleteTask(30))
	require.NoError(t, m.DeleteTask(30))
	require.NoError(t, m.RemoveSegmentIndex(ctx, 30))
	require.NoError(t, m.RemoveSegmentIndex(ctx, 30))
	assertIndexTaskCounts(t, m)
	require.Empty(t, m.taskCounts.byIndex)

	// Copy paths can create already-finished tasks, including two build IDs
	// for the same (segment,index): the metric has always counted build IDs.
	for _, id := range []int64{40, 41} {
		require.NoError(t, m.AddSegmentIndex(ctx, &model.SegmentIndex{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: id, IndexState: commonpb.IndexState_Finished}))
	}
	require.Equal(t, indexTaskStateCounts{0, 0, 0, 2}, m.indexTaskCountsSnapshot())
	require.NoError(t, m.RemoveSegmentIndex(ctx, 40))
	assertIndexTaskCounts(t, m)
	require.NoError(t, m.RemoveIndex(ctx, 1, 10))
	assertIndexTaskCounts(t, m)
	require.NoError(t, m.FinishTask(&workerpb.IndexTaskInfo{BuildID: 41, State: commonpb.IndexState_Finished}))
	require.Equal(t, indexTaskStateCounts{}, m.indexTaskCountsSnapshot())
	require.NoError(t, m.RemoveSegmentIndex(ctx, 41))
	require.Empty(t, m.taskCounts.byIndex)
}

func TestIndexTaskCountsRecoveryReplacement(t *testing.T) {
	catalog := &taskCountsCatalog{
		indexes: []*model.Index{{CollectionID: 1, IndexID: 10}},
		tasks: []*model.SegmentIndex{
			{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: 30, IndexState: commonpb.IndexState_Unissued},
			{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: 30, IndexState: commonpb.IndexState_Finished},
			{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: 30, IndexState: commonpb.IndexState_Finished},
		},
	}
	m, err := newIndexMeta(context.Background(), catalog, []int64{1, 2, 3})
	require.NoError(t, err)
	assertIndexTaskCounts(t, m)
	require.Equal(t, indexTaskStateCounts{0, 0, 0, 1}, m.indexTaskCountsSnapshot())
}

func TestIndexTaskCountsPersistenceFailures(t *testing.T) {
	ctx := context.Background()
	operations := map[string]func(*indexMeta) error{
		"create": func(m *indexMeta) error { return m.CreateIndex(ctx, &model.Index{CollectionID: 2, IndexID: 11}) },
		"alter-index": func(m *indexMeta) error {
			return m.AlterIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, IsDeleted: true})
		},
		"add": func(m *indexMeta) error {
			return m.AddSegmentIndex(ctx, &model.SegmentIndex{CollectionID: 1, IndexID: 10, SegmentID: 21, BuildID: 31})
		},
		"state":   func(m *indexMeta) error { return m.UpdateIndexState(30, commonpb.IndexState_Retry, "") },
		"version": func(m *indexMeta) error { return m.UpdateVersion(30, 100) },
		"build":   func(m *indexMeta) error { return m.BuildIndex(30) },
		"finish": func(m *indexMeta) error {
			return m.FinishTask(&workerpb.IndexTaskInfo{BuildID: 30, State: commonpb.IndexState_Finished})
		},
		"delete-task":  func(m *indexMeta) error { return m.DeleteTask(30) },
		"mark-deleted": func(m *indexMeta) error { return m.MarkIndexAsDeleted(ctx, 1, nil) },
		"remove-task":  func(m *indexMeta) error { return m.RemoveSegmentIndex(ctx, 30) },
		"remove-index": func(m *indexMeta) error { return m.RemoveIndex(ctx, 1, 10) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			catalog := &taskCountsCatalog{
				indexes: []*model.Index{{CollectionID: 1, IndexID: 10, IsDeleted: name == "remove-indexes"}},
				tasks:   []*model.SegmentIndex{{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: 30, IndexState: commonpb.IndexState_Unissued}},
			}
			m, err := newIndexMeta(ctx, catalog, []int64{1, 2, 3})
			require.NoError(t, err)
			before := m.indexTaskCountsSnapshot()
			catalog.err = merr.ErrServiceUnavailable
			require.ErrorIs(t, operation(m), catalog.err)
			require.Equal(t, before, m.indexTaskCountsSnapshot())
			assertIndexTaskCounts(t, m)
			catalog.err = nil
			require.NoError(t, operation(m))
			assertIndexTaskCounts(t, m)
		})
	}
}

func TestIndexTaskCountsRandomized(t *testing.T) {
	ctx := context.Background()
	m, err := newIndexMeta(ctx, &taskCountsCatalog{}, nil)
	require.NoError(t, err)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 500; i++ {
		buildID := int64(rng.Intn(24))
		collID := buildID % 3
		indexID := buildID % 2
		switch rng.Intn(7) {
		case 0:
			require.NoError(t, m.CreateIndex(ctx, &model.Index{CollectionID: collID, IndexID: indexID}))
		case 1:
			require.NoError(t, m.AddSegmentIndex(ctx, &model.SegmentIndex{CollectionID: collID, IndexID: indexID, SegmentID: buildID, BuildID: buildID, IndexState: commonpb.IndexState(rng.Intn(7))}))
		case 2:
			if _, ok := m.GetIndexJob(buildID); ok {
				require.NoError(t, m.UpdateIndexState(buildID, commonpb.IndexState(rng.Intn(7)), ""))
			}
		case 3:
			require.NoError(t, m.DeleteTask(buildID))
		case 4:
			require.NoError(t, m.RemoveSegmentIndex(ctx, buildID))
		case 5:
			require.NoError(t, m.MarkIndexAsDeleted(ctx, collID, []int64{indexID}))
		case 6:
			require.NoError(t, m.RemoveIndex(ctx, collID, indexID))
		}
		assertIndexTaskCounts(t, m)
	}
}

func TestIndexTaskCountsConcurrent(t *testing.T) {
	ctx := context.Background()
	m, err := newIndexMeta(ctx, &taskCountsCatalog{}, nil)
	require.NoError(t, err)
	const builds = 16
	require.NoError(t, m.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10}))
	for id := int64(0); id < builds; id++ {
		require.NoError(t, m.AddSegmentIndex(ctx, &model.SegmentIndex{CollectionID: 1, IndexID: 10, SegmentID: id, BuildID: id}))
	}
	var wg sync.WaitGroup
	for id := int64(0); id < builds; id++ {
		wg.Go(func() {
			for i := 0; i < 20; i++ {
				if err := m.UpdateIndexState(id, commonpb.IndexState(i%6), ""); err != nil {
					t.Error(err)
				}
				snapshot := m.indexTaskCountsSnapshot()
				var total int64
				for _, n := range snapshot {
					if n < 0 {
						t.Error("negative task count")
					}
					total += n
				}
				if total != 0 && total != builds {
					t.Errorf("torn index publication: %d", total)
				}
			}
		})
	}
	wg.Go(func() {
		for i := 0; i < 20; i++ {
			if err := m.MarkIndexAsDeleted(ctx, 1, nil); err != nil {
				t.Error(err)
			}
			if err := m.AlterIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10}); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	assertIndexTaskCounts(t, m)
	require.Zero(t, testing.AllocsPerRun(100, func() { m.indexTaskCountsSnapshot() }))
}

func BenchmarkIndexTaskCounts(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			m := &indexMeta{
				ctx:              context.Background(),
				indexes:          make(map[int64]map[int64]*model.Index),
				segmentBuildInfo: newSegmentIndexBuildInfo(),
				segmentIndexes:   typeutil.NewConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *model.SegmentIndex]](),
			}
			m.updateCollectionIndex(&model.Index{CollectionID: 1, IndexID: 10})
			for i := 0; i < size; i++ {
				m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, IndexID: 10, SegmentID: int64(i), BuildID: int64(i), IndexState: commonpb.IndexState_Finished})
			}
			b.Run("full-scan", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = scanIndexTaskCounts(m)
				}
			})
			b.Run("snapshot", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = m.indexTaskCountsSnapshot()
				}
			})
			b.Run("publish", func(b *testing.B) {
				m.updateIndexTasksMetrics()
				b.ReportAllocs()
				for b.Loop() {
					m.updateIndexTasksMetrics()
				}
			})
		})
	}
}

func TestIndexTaskCountsManifestInstallAvoidsDDLLock(t *testing.T) {
	ctx := context.Background()
	m, err := newIndexMeta(ctx, &taskCountsCatalog{}, nil)
	require.NoError(t, err)
	require.NoError(t, m.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10}))
	require.NoError(t, m.AddSegmentIndex(ctx, &model.SegmentIndex{CollectionID: 1, IndexID: 10, SegmentID: 20, BuildID: 30}))
	m.keyLock.Lock(30)
	defer m.keyLock.Unlock(30)
	staged, err := m.stageSegmentIndexMutation(SegmentIndexMutation{
		Type: SegmentIndexUpsert, BuildID: 30,
		FinishedTask: &workerpb.IndexTaskInfo{BuildID: 30, State: commonpb.IndexState_Finished},
	})
	require.NoError(t, err)
	before := m.indexTaskCountsSnapshot()
	require.Equal(t, indexTaskStateCounts{0, 1}, before, "staging must not publish counts")
	m.fieldIndexLock.Lock()
	done := make(chan func(), 1)
	go func() { done <- staged.install() }()
	var metrics func()
	select {
	case metrics = <-done:
		m.fieldIndexLock.Unlock()
	case <-time.After(5 * time.Second):
		m.fieldIndexLock.Unlock()
		<-done
		t.Fatal("manifest install waited for the DDL lock")
	}
	metrics()
	assertIndexTaskCounts(t, m)
	require.Equal(t, indexTaskStateCounts{0, 0, 0, 1}, m.indexTaskCountsSnapshot())
	removed, err := m.stageSegmentIndexMutation(SegmentIndexMutation{Type: SegmentIndexRemove, BuildID: 30})
	require.NoError(t, err)
	require.Equal(t, indexTaskStateCounts{0, 0, 0, 1}, m.indexTaskCountsSnapshot())
	deferred := removed.install()
	deferred()
	require.Equal(t, indexTaskStateCounts{}, m.indexTaskCountsSnapshot())
	assertIndexTaskCounts(t, m)
}
