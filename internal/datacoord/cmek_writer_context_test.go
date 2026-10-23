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
	"encoding/base64"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type writerContextCipher struct {
	hook.Cipher
	key []byte
}

func (c *writerContextCipher) GetUnsafeKey(int64, int64) []byte { return c.key }

func newPreImportWriterContextTask(t *testing.T, job *datapb.ImportJob) (ImportTask, ImportMeta) {
	t.Helper()
	im, catalog := newTestImportMeta(t)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SavePreImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()
	require.NoError(t, im.AddJob(context.Background(), &importJob{
		ImportJob: job,
		tr:        timerecord.NewTimeRecorder("preimport key test"),
	}))

	var task ImportTask
	if job.GetVersion() == internalpb.ImportVersion_ImportVersionV3 {
		stats := make([]*datapb.ImportV3FileStats, 0, len(job.GetFiles()))
		for _, file := range job.GetFiles() {
			stats = append(stats, &datapb.ImportV3FileStats{FileId: file.GetId()})
		}
		task = newPreImportTaskV3(&datapb.PreImportTaskV3{
			JobId: job.GetJobID(), TaskId: 31, CollectionId: job.GetCollectionID(),
			NodeId: NullNodeID, State: datapb.ImportTaskStateV2_Pending, FileStats: stats,
		}, im)
	} else {
		stats := make([]*datapb.ImportFileStats, 0, len(job.GetFiles()))
		for _, file := range job.GetFiles() {
			stats = append(stats, &datapb.ImportFileStats{ImportFile: file})
		}
		p := &preImportTask{importMeta: im, tr: timerecord.NewTimeRecorder("preimport key test"), times: taskcommon.NewTimes()}
		p.task.Store(&datapb.PreImportTask{
			JobID: job.GetJobID(), TaskID: 31, CollectionID: job.GetCollectionID(),
			NodeID: NullNodeID, State: datapb.ImportTaskStateV2_Pending, FileStats: stats,
		})
		task = p
	}
	require.NoError(t, im.AddTask(context.Background(), task))
	return task, im
}

func preImportWriterContextJob(version internalpb.ImportVersion) *datapb.ImportJob {
	return &datapb.ImportJob{
		JobID: 11, CollectionID: 23, Version: version, State: internalpb.ImportJobState_PreImporting,
		Schema: &schemapb.CollectionSchema{Properties: []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "17"},
		}},
		Files: []*internalpb.ImportFile{{Id: 41, Paths: []string{"input.parquet"}}},
	}
}

func TestPreImportWriterContext(t *testing.T) {
	key := []byte("\x00\xff0123456789abcdef0123456789abcd")
	for _, version := range []internalpb.ImportVersion{internalpb.ImportVersion_ImportVersionV2, internalpb.ImportVersion_ImportVersionV3} {
		t.Run(version.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				cipher    hook.Cipher
				wantError string
			}{
				{name: "empty key", cipher: &writerContextCipher{}, wantError: "cannot get ez key"},
				{name: "missing plugin", wantError: "cipher plugin is unavailable"},
				{name: "available key", cipher: &writerContextCipher{key: key}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					patch := mockey.Mock(hookutil.GetCipher).To(func() hook.Cipher { return tc.cipher }).Build()
					t.Cleanup(func() { patch.UnPatch() })
					job := preImportWriterContextJob(version)
					task, im := newPreImportWriterContextTask(t, job)
					cluster := session.NewMockCluster(t)

					if tc.wantError != "" {
						task.CreateTaskOnWorker(42, cluster)
						failed := im.GetJob(context.Background(), job.GetJobID())
						require.Equal(t, internalpb.ImportJobState_Failed, failed.GetState())
						require.Contains(t, failed.GetReason(), tc.wantError)
						require.Equal(t, int64(NullNodeID), task.GetNodeID())
						cluster.AssertNotCalled(t, "CreatePreImport", mock.Anything, mock.Anything, mock.Anything)
						cluster.AssertNotCalled(t, "CreatePreImportV3", mock.Anything, mock.Anything, mock.Anything)
						return
					}

					create := func(nodeID int64, req *datapb.PreImportRequest, _ int64) error {
						require.Equal(t, int64(42), nodeID)
						require.Equal(t, task.GetTaskID(), req.GetTaskID())
						require.Equal(t, job.GetFiles(), req.GetImportFiles())
						require.Len(t, req.GetPluginContext(), 1)
						require.Equal(t, hookutil.CipherConfigUnsafeEZK, req.GetPluginContext()[0].GetKey())
						require.Equal(t, "17:"+base64.StdEncoding.EncodeToString(key), req.GetPluginContext()[0].GetValue())
						return nil
					}
					if version == internalpb.ImportVersion_ImportVersionV3 {
						cluster.EXPECT().CreatePreImportV3(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(create).Once()
					} else {
						cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(create).Once()
					}
					task.CreateTaskOnWorker(42, cluster)
					require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
					require.Equal(t, int64(42), task.GetNodeID())
					require.Equal(t, internalpb.ImportJobState_PreImporting, im.GetJob(context.Background(), job.GetJobID()).GetState())
				})
			}
		})
	}
}

func TestCompactionRejectsUnavailableWriterContext(t *testing.T) {
	cipher := &writerContextCipher{}
	patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
	t.Cleanup(func() { patch.UnPatch() })
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			cipher.key = nil
			meta := NewMockCompactionMeta(t)
			meta.EXPECT().GetHealthySegment(mock.Anything, int64(31)).Return(&SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{ID: 31, CollectionID: 23},
			})
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil)
			taskProto := &datapb.CompactionTask{
				PlanID: 1, CollectionID: 23, Type: kind,
				State:         datapb.CompactionTaskState_pipelining,
				InputSegments: []int64{31},
				Schema: &schemapb.CollectionSchema{Properties: []*commonpb.KeyValuePair{
					{Key: common.EncryptionEzIDKey, Value: "17"},
				}},
			}
			var task CompactionTask
			if kind == datapb.CompactionType_BumpSchemaVersionCompaction {
				task = newBumpSchemaVersionTask(taskProto, alloc, meta, newMockVersionManager())
			} else {
				task = newMixCompactionTask(taskProto, alloc, meta, newMockVersionManager())
			}
			plan, err := task.BuildCompactionRequest()
			require.Nil(t, plan)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.True(t, merr.IsRetryableErr(err))
			cluster := session.NewMockCluster(t)
			// Keep the task's existing assemble-error transition. The key
			// failure must prevent dispatch, without adding a retry policy.
			meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, saved *datapb.CompactionTask) error {
					require.Equal(t, datapb.CompactionTaskState_failed, saved.GetState())
					return nil
				}).Once()
			task.CreateTaskOnWorker(42, cluster)
			require.Equal(t, datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
		})
	}
}
