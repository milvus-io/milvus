// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestPreImportV3TaskQueryTaskOnWorker pins the DataCoord side of the Import V3
// count-only preimport query: the worker answers with the V3 file stats type
// directly (no ImportFileStats projection), and an InProgress/Completed answer
// is stored on the task record as-is.
func TestPreImportV3TaskQueryTaskOnWorker(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SavePreImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	require.NoError(t, err)

	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 2, CollectionId: 3, NodeId: 7, State: datapb.ImportTaskStateV2_InProgress,
	}, im)
	require.NoError(t, im.AddTask(context.TODO(), task))

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryPreImportV3(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportV3Response{
		State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportV3FileStats{
			{FileId: 11, FileSize: 100, TotalRows: 5, TotalMemorySize: 42},
		},
	}, nil).Once()

	task.QueryTaskOnWorker(cluster)

	require.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
	stats := task.GetV3FileStats()
	require.Len(t, stats, 1)
	require.Equal(t, int64(11), stats[0].GetFileId())
	require.Equal(t, int64(100), stats[0].GetFileSize())
	require.Equal(t, int64(5), stats[0].GetTotalRows())
	require.Equal(t, int64(42), stats[0].GetTotalMemorySize())
}

// TestPreImportV3TaskQueryTaskOnWorkerResetsOnError pins the retry path: an RPC
// error resets the task to Pending without touching its stored stats.
func TestPreImportV3TaskQueryTaskOnWorkerResetsOnError(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SavePreImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	require.NoError(t, err)

	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 2, CollectionId: 3, NodeId: 7, State: datapb.ImportTaskStateV2_InProgress,
	}, im)
	require.NoError(t, im.AddTask(context.TODO(), task))

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryPreImportV3(mock.Anything, mock.Anything).
		Return(&datapb.QueryPreImportV3Response{}, context.DeadlineExceeded).Once()

	task.QueryTaskOnWorker(cluster)

	require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
	require.Empty(t, task.GetV3FileStats())
}

// newPreImportV3TestImportMeta returns an ImportMeta whose catalog serves the
// given jobs, for tests that need the job (slot calculation, worker request).
func newPreImportV3TestImportMeta(t *testing.T, jobs ...*datapb.ImportJob) ImportMeta {
	t.Helper()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(jobs, nil).Maybe()
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SavePreImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()
	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	require.NoError(t, err)
	return im
}

// TestPreImportV3TaskCreateTaskOnWorkerSendsImportFiles pins the request the
// count-only preimport worker receives: the V3 record stores only file IDs, so
// CreateTaskOnWorker must resolve them against the frozen job. Sending an empty
// import_files list would make the worker return empty stats, which DataCoord
// would store and then block the whole ordinary-import flow until timeout.
func TestPreImportV3TaskCreateTaskOnWorkerSendsImportFiles(t *testing.T) {
	jobProto := &datapb.ImportJob{
		JobID: 1, CollectionID: 2,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		}},
		Files: []*internalpb.ImportFile{{Id: 11, Paths: []string{"a.csv"}}, {Id: 12, Paths: []string{"b.csv"}}},
	}
	im := newPreImportV3TestImportMeta(t, jobProto)

	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 3, CollectionId: 2,
		FileStats: []*datapb.ImportV3FileStats{{FileId: 11}, {FileId: 12}},
	}, im)
	require.NoError(t, im.AddTask(context.TODO(), task))

	var captured *datapb.PreImportRequest
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreatePreImportV3(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ int64, req *datapb.PreImportRequest, _ int64) error {
			captured = req
			return nil
		}).Once()

	task.CreateTaskOnWorker(7, cluster)

	require.NotNil(t, captured)
	require.Len(t, captured.GetImportFiles(), 2, "the V3 preimport request must carry the task's files")
	require.Equal(t, int64(11), captured.GetImportFiles()[0].GetId())
	require.Equal(t, int64(12), captured.GetImportFiles()[1].GetId())
}

// TestPreImportV3TaskDropTaskOnWorkerUnbindsWhenNodeGone pins the GC unblock:
// when the bound DataNode already left, the drop is vacuously done and the task
// must be unbound, or the terminal-job GC never reaches the delete phase.
func TestPreImportV3TaskDropTaskOnWorkerUnbindsWhenNodeGone(t *testing.T) {
	im := newPreImportV3TestImportMeta(t)

	task := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 2, CollectionId: 3, NodeId: 7, State: datapb.ImportTaskStateV2_Completed,
	}, im)
	require.NoError(t, im.AddTask(context.TODO(), task))

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().DropPreImportV3(int64(7), int64(2)).Return(merr.ErrNodeNotFound).Once()

	task.DropTaskOnWorker(cluster)

	require.Equal(t, int64(NullNodeID), task.GetNodeID())
}
