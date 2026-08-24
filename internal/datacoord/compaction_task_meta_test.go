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
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

func TestCompactionTaskMetaMetricsUsePersistedState(t *testing.T) {
	ctx := context.Background()
	m := newTestCompactionTaskMeta(t)
	task := &datapb.CompactionTask{
		TriggerID: 99530,
		PlanID:    99530,
		NodeID:    99530,
		Type:      datapb.CompactionType_SortCompaction,
		State:     datapb.CompactionTaskState_meta_saved,
	}
	executing := metrics.DataCoordCompactionTaskNum.WithLabelValues("99530", task.Type.String(), metrics.Executing)
	done := metrics.DataCoordCompactionTaskNum.WithLabelValues("99530", task.Type.String(), metrics.Done)
	initialExecuting, initialDone := testutil.ToFloat64(executing), testutil.ToFloat64(done)
	t.Cleanup(func() {
		executing.Set(initialExecuting)
		done.Set(initialDone)
	})

	// The first save does not admit a task to the inspector.
	require.NoError(t, m.SaveCompactionTask(ctx, task))
	require.Equal(t, initialExecuting, testutil.ToFloat64(executing))
	incCompactionTaskMetric(task)

	// Two callers can both hold the same old task snapshot. The second save
	// must compare with the first persisted result, not that caller's snapshot.
	first := proto.Clone(task).(*datapb.CompactionTask)
	second := proto.Clone(task).(*datapb.CompactionTask)
	first.State, second.State = datapb.CompactionTaskState_completed, datapb.CompactionTaskState_completed
	require.NoError(t, m.SaveCompactionTask(ctx, first))
	require.NoError(t, m.SaveCompactionTask(ctx, second))
	require.Equal(t, initialExecuting, testutil.ToFloat64(executing))
	require.Equal(t, initialDone+1, testutil.ToFloat64(done))
}

func TestCompactionTaskMetaMetricsTransitions(t *testing.T) {
	for _, taskType := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_Level0DeleteCompaction,
		datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		for _, terminal := range []datapb.CompactionTaskState{
			datapb.CompactionTaskState_completed,
			datapb.CompactionTaskState_failed,
			datapb.CompactionTaskState_timeout,
		} {
			t.Run(taskType.String()+"/"+terminal.String(), func(t *testing.T) {
				ctx := context.Background()
				var saveErr error
				catalog := mocks.NewDataCoordCatalog(t)
				catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Once()
				catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).RunAndReturn(
					func(context.Context, *datapb.CompactionTask) error { return saveErr },
				)
				catalog.EXPECT().DropCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
				m, err := newCompactionTaskMeta(ctx, catalog)
				require.NoError(t, err)

				type labels struct {
					node   int64
					status string
				}
				gauges := map[labels]float64{}
				gauge := func(key labels) prometheus.Gauge {
					return metrics.DataCoordCompactionTaskNum.WithLabelValues(strconv.FormatInt(key.node, 10), taskType.String(), key.status)
				}
				for _, node := range []int64{NullNodeID, 99540, 99541} {
					for _, status := range []string{metrics.Pending, metrics.Executing, metrics.Done} {
						key := labels{node, status}
						gauges[key] = testutil.ToFloat64(gauge(key))
					}
				}
				t.Cleanup(func() {
					for key, initial := range gauges {
						gauge(key).Set(initial)
					}
				})
				check := func(current labels) {
					t.Helper()
					for key, initial := range gauges {
						want := initial
						if key == current {
							want++
						}
						got := testutil.ToFloat64(gauge(key))
						require.Equal(t, want, got, "node=%d status=%s", key.node, key.status)
					}
				}

				p := &datapb.CompactionTask{TriggerID: 99540, PlanID: 99540, Type: taskType, State: datapb.CompactionTaskState_pipelining, NodeID: NullNodeID}
				require.NoError(t, m.SaveCompactionTask(ctx, p))
				incCompactionTaskMetric(p)
				check(labels{NullNodeID, metrics.Pending})
				steps := []struct {
					state  datapb.CompactionTaskState
					node   int64
					metric labels
				}{
					{datapb.CompactionTaskState_executing, 99540, labels{99540, metrics.Executing}},
					{datapb.CompactionTaskState_executing, 99541, labels{99541, metrics.Executing}},
					{datapb.CompactionTaskState_pipelining, NullNodeID, labels{NullNodeID, metrics.Pending}},
					{datapb.CompactionTaskState_pipelining, 99540, labels{NullNodeID, metrics.Pending}},
					{datapb.CompactionTaskState_executing, 99541, labels{99541, metrics.Executing}},
					{datapb.CompactionTaskState_analyzing, 99541, labels{99541, metrics.Executing}},
					{datapb.CompactionTaskState_indexing, 99541, labels{99541, metrics.Executing}},
					{datapb.CompactionTaskState_statistic, 99541, labels{99541, metrics.Executing}},
					{datapb.CompactionTaskState_meta_saved, 99541, labels{99541, metrics.Executing}},
					{terminal, 99541, labels{99541, metrics.Done}},
					{datapb.CompactionTaskState_cleaned, 99541, labels{99541, metrics.Done}},
				}
				previous := labels{NullNodeID, metrics.Pending}
				for _, step := range steps {
					next := proto.Clone(p).(*datapb.CompactionTask)
					next.State, next.NodeID = step.state, step.node
					// Failed persistence must leave both metadata and metrics intact.
					saveErr = errors.New("catalog unavailable")
					require.Error(t, m.SaveCompactionTask(ctx, next))
					check(previous)
					require.True(t, proto.Equal(p, m.GetCompactionTasksByTriggerID(p.TriggerID)[0]))
					saveErr = nil
					require.NoError(t, m.SaveCompactionTask(ctx, next))
					require.NoError(t, m.SaveCompactionTask(ctx, next))
					check(step.metric)
					p, previous = next, step.metric
				}
				require.NoError(t, m.DropCompactionTask(ctx, p))
				check(labels{99541, metrics.Done})
			})
		}
	}
}

func TestCompactionTaskMetaSuite(t *testing.T) {
	suite.Run(t, new(CompactionTaskMetaSuite))
}

type CompactionTaskMetaSuite struct {
	suite.Suite
	catalog *mocks.DataCoordCatalog
	meta    *compactionTaskMeta
}

func (suite *CompactionTaskMetaSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog = catalog
	meta, err := newCompactionTaskMeta(context.TODO(), catalog)
	suite.NoError(err)
	suite.meta = meta
}

func newTestCompactionTaskMeta(t *testing.T) *compactionTaskMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	meta, _ := newCompactionTaskMeta(context.TODO(), catalog)
	return meta
}

func (suite *CompactionTaskMetaSuite) TestGetCompactionTasksByCollection() {
	suite.meta.SaveCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
	})
	res := suite.meta.GetCompactionTasksByCollection(100)
	suite.Equal(1, len(res))
}

func (suite *CompactionTaskMetaSuite) TestGetCompactionTasksByCollectionAbnormal() {
	suite.meta.SaveCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
	})
	suite.meta.SaveCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:    2,
		PlanID:       11,
		CollectionID: 101,
	})
	res := suite.meta.GetCompactionTasksByCollection(101)
	suite.Equal(1, len(res))
}

func (suite *CompactionTaskMetaSuite) TestTaskStatsJSON() {
	task1 := &datapb.CompactionTask{
		PlanID:         1,
		CollectionID:   100,
		Type:           datapb.CompactionType_MergeCompaction,
		State:          datapb.CompactionTaskState_completed,
		FailReason:     "",
		StartTime:      time.Now().Unix(),
		EndTime:        time.Now().Add(time.Hour).Unix(),
		TotalRows:      1000,
		InputSegments:  []int64{1, 2},
		ResultSegments: []int64{3},
	}
	task2 := &datapb.CompactionTask{
		PlanID:         2,
		CollectionID:   101,
		Type:           datapb.CompactionType_MergeCompaction,
		State:          datapb.CompactionTaskState_completed,
		FailReason:     "",
		StartTime:      time.Now().Unix(),
		EndTime:        time.Now().Add(time.Hour).Unix(),
		TotalRows:      2000,
		InputSegments:  []int64{4, 5},
		ResultSegments: []int64{6},
	}

	// testing return empty string
	actualJSON := suite.meta.TaskStatsJSON()
	suite.Equal("[]", actualJSON)

	err := suite.meta.SaveCompactionTask(context.TODO(), task1)
	suite.NoError(err)
	err = suite.meta.SaveCompactionTask(context.TODO(), task2)
	suite.NoError(err)

	expectedTasks := []*metricsinfo.CompactionTask{
		newCompactionTaskStats(task1),
		newCompactionTaskStats(task2),
	}
	expectedJSON, err := json.Marshal(expectedTasks)
	suite.NoError(err)

	actualJSON = suite.meta.TaskStatsJSON()
	suite.JSONEq(string(expectedJSON), actualJSON)
}

// TestReloadFromKV_PreAllocatedSegmentIDsCompatibility verifies that compatibility
// logic in reloadFromKV does NOT mark Level0DeleteCompaction tasks as failed when
// PreAllocatedSegmentIDs is nil, while still failing other unfinished tasks that
// require pre-allocated segment IDs.
func (suite *CompactionTaskMetaSuite) TestReloadFromKV_PreAllocatedSegmentIDsCompatibility() {
	// L0 delete compaction task does not use PreAllocatedSegmentIDs.
	l0Task := &datapb.CompactionTask{
		PlanID:    1,
		TriggerID: 1,
		Type:      datapb.CompactionType_Level0DeleteCompaction,
		State:     datapb.CompactionTaskState_executing,
	}

	// Clustering compaction task should require PreAllocatedSegmentIDs and be
	// marked as failed when the field is nil.
	clusteringTask := &datapb.CompactionTask{
		PlanID:    2,
		TriggerID: 2,
		Type:      datapb.CompactionType_ClusteringCompaction,
		State:     datapb.CompactionTaskState_executing,
	}

	catalog := mocks.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return([]*datapb.CompactionTask{l0Task, clusteringTask}, nil).Once()

	meta, err := newCompactionTaskMeta(context.TODO(), catalog)
	suite.NoError(err)

	l0Tasks := meta.GetCompactionTasksByTriggerID(1)
	suite.Equal(1, len(l0Tasks))
	suite.Equal(datapb.CompactionTaskState_executing, l0Tasks[0].State)

	clusteringTasks := meta.GetCompactionTasksByTriggerID(2)
	suite.Equal(1, len(clusteringTasks))
	suite.Equal(datapb.CompactionTaskState_failed, clusteringTasks[0].State)
}

// TestReloadFromKV_BumpSchemaVersionTaskSurvives verifies that an in-progress schema bump compaction
func (suite *CompactionTaskMetaSuite) TestReloadFromKV_BumpSchemaVersionTaskSurvives() {
	bumpSchemaVersionTask := &datapb.CompactionTask{
		PlanID:                 10,
		TriggerID:              10,
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		State:                  datapb.CompactionTaskState_executing,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 100, End: 101},
	}

	catalog := mocks.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return([]*datapb.CompactionTask{bumpSchemaVersionTask}, nil).Once()

	meta, err := newCompactionTaskMeta(context.TODO(), catalog)
	suite.NoError(err)

	tasks := meta.GetCompactionTasksByTriggerID(10)
	suite.Equal(1, len(tasks))
	suite.Equal(datapb.CompactionTaskState_executing, tasks[0].State,
		"schema bump task must survive reload even with nil PreAllocatedSegmentIDs")
}
