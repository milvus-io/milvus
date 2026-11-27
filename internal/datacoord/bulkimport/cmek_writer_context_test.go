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

package bulkimport

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type writerContextCipher struct {
	hook.Cipher
	key []byte
}

func (c *writerContextCipher) GetUnsafeKey(int64, int64) []byte { return c.key }

func newWriterContextImportMeta(t *testing.T, job *datapb.ImportJob) ImportMeta {
	t.Helper()
	paramtable.Init()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	im, err := NewImportMeta(context.Background(), catalog, allocator.NewMockAllocator(t), nil)
	require.NoError(t, err)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SavePreImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()
	require.NoError(t, im.AddJob(context.Background(), &Job{
		ImportJob: job,
		Tr:        timerecord.NewTimeRecorder("import key test"),
	}))
	return im
}

func newPreImportWriterContextTask(t *testing.T, job *datapb.ImportJob) (ImportTask, ImportMeta) {
	t.Helper()
	im := newWriterContextImportMeta(t, job)
	var task ImportTask
	if job.GetVersion() == internalpb.ImportVersion_ImportVersionV3 {
		stats := make([]*datapb.ImportV3FileStats, 0, len(job.GetFiles()))
		for _, file := range job.GetFiles() {
			stats = append(stats, &datapb.ImportV3FileStats{FileId: file.GetId()})
		}
		task = newPreImportTaskV3(&datapb.PreImportTaskV3{
			JobId: job.GetJobID(), TaskId: 31, CollectionId: job.GetCollectionID(),
			NodeId: meta.NullNodeID, State: datapb.ImportTaskStateV2_Pending, FileStats: stats,
		}, im)
	} else {
		stats := make([]*datapb.ImportFileStats, 0, len(job.GetFiles()))
		for _, file := range job.GetFiles() {
			stats = append(stats, &datapb.ImportFileStats{ImportFile: file})
		}
		p := &PreImportTask{ImportMeta: im, Tr: timerecord.NewTimeRecorder("preimport key test"), times: taskcommon.NewTimes()}
		p.Task.Store(&datapb.PreImportTask{
			JobID: job.GetJobID(), TaskID: 31, CollectionID: job.GetCollectionID(),
			NodeID: meta.NullNodeID, State: datapb.ImportTaskStateV2_Pending, FileStats: stats,
		})
		task = p
	}
	require.NoError(t, im.AddTask(context.Background(), task))
	return task, im
}

func newImportWriterContextTask(t *testing.T, job *datapb.ImportJob) (ImportTask, ImportMeta) {
	t.Helper()
	job.State = internalpb.ImportJobState_Importing
	job.DataTs = 1000
	im := newWriterContextImportMeta(t, job)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).Return(1000, 2000, nil).Maybe()
	task := &Task{ImportMeta: im, Alloc: alloc, Tr: timerecord.NewTimeRecorder("import key test"), times: taskcommon.NewTimes()}
	task.Task.Store(&datapb.ImportTaskV2{
		JobID: job.GetJobID(), TaskID: 32, CollectionID: job.GetCollectionID(),
		NodeID: meta.NullNodeID, State: datapb.ImportTaskStateV2_Pending,
		FileStats: []*datapb.ImportFileStats{{ImportFile: job.GetFiles()[0], TotalRows: 1}},
	})
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
				retryable bool
			}{
				{name: "empty key", cipher: &writerContextCipher{}, retryable: true},
				{name: "missing plugin", retryable: true},
				{name: "available key", cipher: &writerContextCipher{key: key}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					patch := mockey.Mock(hookutil.GetCipher).To(func() hook.Cipher { return tc.cipher }).Build()
					t.Cleanup(func() { patch.UnPatch() })
					job := preImportWriterContextJob(version)
					task, im := newPreImportWriterContextTask(t, job)
					cluster := session.NewMockCluster(t)

					if tc.retryable {
						// Assemble key errors keep the task Pending and increment retryTimes.
						for i := int64(1); i <= 4; i++ {
							task.CreateTaskOnWorker(42, cluster, task.GetTaskResource())
							require.Equal(t, i, task.GetTaskVersion())
						}
						got := im.GetJob(context.Background(), job.GetJobID())
						require.Equal(t, internalpb.ImportJobState_PreImporting, got.GetState())
						require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
						require.Equal(t, int64(meta.NullNodeID), task.GetNodeID())
						cluster.AssertNotCalled(t, "CreatePreImport", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
						cluster.AssertNotCalled(t, "CreatePreImportV3", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
						return
					}

					create := func(nodeID int64, req *datapb.PreImportRequest, _ int64, _ taskcommon.Resource) error {
						require.Equal(t, int64(42), nodeID)
						require.Equal(t, task.GetTaskID(), req.GetTaskID())
						require.Equal(t, job.GetFiles(), req.GetImportFiles())
						require.Len(t, req.GetPluginContext(), 1)
						require.Equal(t, hookutil.CipherConfigUnsafeEZK, req.GetPluginContext()[0].GetKey())
						require.Equal(t, "17:"+base64.StdEncoding.EncodeToString(key), req.GetPluginContext()[0].GetValue())
						return nil
					}
					if version == internalpb.ImportVersion_ImportVersionV3 {
						cluster.EXPECT().CreatePreImportV3(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(create).Once()
					} else {
						cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(create).Once()
					}
					task.CreateTaskOnWorker(42, cluster, task.GetTaskResource())
					require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
					require.Equal(t, int64(42), task.GetNodeID())
					require.Equal(t, internalpb.ImportJobState_PreImporting, im.GetJob(context.Background(), job.GetJobID()).GetState())
				})
			}
		})
	}
}

type backupContextCipher struct {
	writerContextCipher
	artifacts []string
	targets   [][2]int64
	sourceErr error
}

func (c *backupContextCipher) GetUnsafeKey(ezID, collectionID int64) []byte {
	c.targets = append(c.targets, [2]int64{ezID, collectionID})
	return c.key
}

func (c *backupContextCipher) Init(map[string]string) error { return errors.New("legacy import init") }

func (c *backupContextCipher) Backup(int64) (string, error) { return backupArtifact, nil }

func (c *backupContextCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	c.artifacts = append(c.artifacts, artifact)
	return 17, []byte("historical"), c.sourceErr
}

func (*backupContextCipher) GetDecryptorForKeyset(int64, int64, []byte, []byte) (hook.Decryptor, error) {
	panic("dispatch does not decrypt binlogs")
}

const backupArtifact = "eyJlel9pZCI6MTd9" // base64 of {"ez_id":17}

func backupContextOptions() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: importutilv2.BackupFlag, Value: "true"},
		{Key: importutilv2.StorageVersion, Value: "2"},
		{Key: importutilv2.EZK, Value: backupArtifact},
	}
}

type importContextFixture func(*testing.T, *datapb.ImportJob) (ImportTask, ImportMeta)

func TestPreImportResolvesSourceOnEachDispatch(t *testing.T) {
	testBackupSourceOnEachDispatch(t, newPreImportWriterContextTask, "CreatePreImport")
}

func TestImportResolvesSourceOnEachDispatch(t *testing.T) {
	testBackupSourceOnEachDispatch(t, newImportWriterContextTask, "CreateImport")
}

func requireImportKeysets(t *testing.T, want, got []*commonpb.KeyValuePair) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, entry := range want {
		require.Equal(t, entry.GetKey(), got[i].GetKey())
		require.Equal(t, entry.GetValue(), got[i].GetValue())
	}
}

func testBackupSourceOnEachDispatch(t *testing.T, newTask importContextFixture, rpc string) {
	t.Helper()
	version := internalpb.ImportVersion_ImportVersionV2
	if rpc == "CreatePreImportV3" {
		version = internalpb.ImportVersion_ImportVersionV3
	}
	job := preImportWriterContextJob(version)
	job.Options = backupContextOptions()
	cipher := &backupContextCipher{writerContextCipher: writerContextCipher{key: []byte("ordinary")}}
	patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
	t.Cleanup(func() { patch.UnPatch() })
	task, im := newTask(t, job)
	cluster := session.NewMockCluster(t)
	cluster.On(rpc, int64(41), mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req := args.Get(1).(interface {
			GetPluginContext() []*commonpb.KeyValuePair
		})
		requireImportKeysets(t, []*commonpb.KeyValuePair{
			{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:aGlzdG9yaWNhbA=="},
			{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:b3JkaW5hcnk="},
		}, req.GetPluginContext())
	}).Return(merr.ErrServiceUnavailable).Once()

	task.CreateTaskOnWorker(41, cluster, task.GetTaskResource())
	require.Equal(t, int64(1), task.GetTaskVersion())
	cipher.sourceErr = errors.New("source KMS denied Decrypt")
	task.CreateTaskOnWorker(42, cluster, task.GetTaskResource())
	require.Equal(t, int64(2), task.GetTaskVersion())
	require.Len(t, cipher.artifacts, 2)
	require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
	require.NotEqual(t, internalpb.ImportJobState_Failed, im.GetJob(context.Background(), job.GetJobID()).GetState())
	cluster.AssertNotCalled(t, rpc, int64(42), mock.Anything, mock.Anything, mock.Anything)
}

func TestPreImportV3ResolvesSourceOnEachDispatch(t *testing.T) {
	testBackupSourceOnEachDispatch(t, newPreImportWriterContextTask, "CreatePreImportV3")
}

func TestImportV3FinalMergeSendsTargetContextOnly(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV3)
	job.Options = backupContextOptions()
	job.State = internalpb.ImportJobState_Importing
	im := newWriterContextImportMeta(t, job)
	task := NewImportTaskV3(&datapb.ImportTaskV3{
		JobId: job.JobID, TaskId: 4, CollectionId: job.CollectionID, RunId: 1,
		State:     datapb.ImportTaskStateV2_Pending,
		SegmentId: 100, PartitionId: 3, Vchannel: "v0",
		LogRange:  &datapb.IDRange{Begin: 1, End: 100},
		Rows:      5,
		Fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 5}},
	}, im, mt, nil)
	require.NoError(t, im.AddTask(context.Background(), task))
	cipher := &backupContextCipher{
		writerContextCipher: writerContextCipher{key: []byte("ordinary")},
		sourceErr:           errors.New("source KMS must not be called for final merge"),
	}
	patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
	defer patch.UnPatch()

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreateImportV3(int64(7), mock.Anything, job.CollectionID, mock.Anything).RunAndReturn(
		func(_ int64, req *datapb.ImportTaskV3Request, _ int64, _ taskcommon.Resource) error {
			requireImportKeysets(t, []*commonpb.KeyValuePair{
				{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:b3JkaW5hcnk="},
			}, req.GetPluginContext())
			return nil
		}).Once()
	task.CreateTaskOnWorker(7, cluster, task.GetTaskResource())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	require.Empty(t, cipher.artifacts, "final merge must not resolve backup source EZK")
	require.Equal(t, [][2]int64{{17, job.CollectionID}}, cipher.targets)
}

func TestV3ReshardKeepsPendingOnSourceKeyError(t *testing.T) {
	job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV3)
	job.Options = backupContextOptions()
	job.State = internalpb.ImportJobState_Resharding
	im := newWriterContextImportMeta(t, job)
	task := newReshardTask(&datapb.ReshardTask{JobId: job.JobID, TaskId: 4, RunId: 1, State: datapb.ImportTaskStateV2_Pending}, im, nil, nil)
	cipher := &backupContextCipher{sourceErr: errors.New("source KMS denied Decrypt")}
	patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
	defer patch.UnPatch()
	cluster := session.NewMockCluster(t)
	task.CreateTaskOnWorker(1, cluster, task.GetTaskResource())
	require.Equal(t, internalpb.ImportJobState_Resharding, im.GetJob(context.Background(), job.JobID).GetState())
	require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
	require.Len(t, cipher.artifacts, 1)
	require.Empty(t, cipher.targets)
}

func TestImportDispatchResolvesCipherBeforeAllocatingResources(t *testing.T) {
	t.Run("v2_import_skips_ts_and_id_allocation", func(t *testing.T) {
		job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV2)
		job.State = internalpb.ImportJobState_Importing
		im := newWriterContextImportMeta(t, job)
		alloc := allocator.NewMockAllocator(t)
		task := &Task{ImportMeta: im, Alloc: alloc, Tr: timerecord.NewTimeRecorder("v2 alloc"), times: taskcommon.NewTimes()}
		task.Task.Store(&datapb.ImportTaskV2{
			JobID: job.GetJobID(), TaskID: 32, CollectionID: job.GetCollectionID(),
			NodeID: meta.NullNodeID, State: datapb.ImportTaskStateV2_Pending,
			FileStats: []*datapb.ImportFileStats{{ImportFile: job.GetFiles()[0], TotalRows: 1}},
		})
		cipher := &writerContextCipher{} // nil key -> ServiceUnavailable
		patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
		defer patch.UnPatch()

		req, err := AssembleImportRequest(task, im.GetJob(context.Background(), job.GetJobID()), nil, alloc)
		require.Nil(t, req)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		alloc.AssertNotCalled(t, "AllocTimestamp", mock.Anything)
		alloc.AssertNotCalled(t, "AllocN", mock.Anything)
	})

	t.Run("v3_import_skips_segment_mutation", func(t *testing.T) {
		ctx := context.Background()
		mt, err := meta.NewMemoryMeta(t)
		require.NoError(t, err)
		_, err = addImportSegment(ctx, mt, 101, 11, 4, 23, 3, "v0", datapb.SegmentLevel_L1, 1, 0)
		require.NoError(t, err)

		job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV3)
		job.State = internalpb.ImportJobState_Importing
		im := newWriterContextImportMeta(t, job)
		task := NewImportTaskV3(&datapb.ImportTaskV3{
			JobId: job.JobID, TaskId: 4, CollectionId: job.CollectionID, RunId: 1,
			State:     datapb.ImportTaskStateV2_Pending,
			SegmentId: 100, OldSegmentIds: []int64{101}, PartitionId: 3, Vchannel: "v0",
			LogRange:  &datapb.IDRange{Begin: 1, End: 100},
			Rows:      5,
			Fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 5}},
		}, im, mt, nil)
		cipher := &writerContextCipher{} // nil key -> ServiceUnavailable
		patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
		defer patch.UnPatch()

		cluster := session.NewMockCluster(t)
		task.CreateTaskOnWorker(7, cluster, task.GetTaskResource())
		require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		require.Nil(t, mt.GetSegment(ctx, 100), "must not create target segment before cipher context succeeds")
		require.Equal(t, commonpb.SegmentState_Importing, mt.GetSegment(ctx, 101).GetState(), "must not drop old segment before cipher context succeeds")
	})
}

func TestImportV2DispatchKeepsPendingAndIncrementsRetryOnWorkerError(t *testing.T) {
	for _, stage := range []struct {
		rpc     string
		newTask importContextFixture
		state   internalpb.ImportJobState
	}{
		{"CreatePreImport", newPreImportWriterContextTask, internalpb.ImportJobState_PreImporting},
		{"CreateImport", newImportWriterContextTask, internalpb.ImportJobState_Importing},
	} {
		t.Run(stage.rpc, func(t *testing.T) {
			job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV2)
			job.Options = backupContextOptions()
			cipher := &backupContextCipher{writerContextCipher: writerContextCipher{key: []byte("ordinary")}}
			patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
			t.Cleanup(func() { patch.UnPatch() })
			task, im := stage.newTask(t, job)
			cluster := session.NewMockCluster(t)
			workerErr := merr.CheckRPCCall(merr.Status(merr.ErrServiceUnavailable), nil)
			cluster.On(stage.rpc, int64(41), mock.Anything, mock.Anything, mock.Anything).Return(workerErr).Once()
			task.CreateTaskOnWorker(41, cluster, task.GetTaskResource())
			require.Equal(t, int64(1), task.GetTaskVersion())
			require.Equal(t, stage.state, im.GetJob(context.Background(), job.GetJobID()).GetState())
			require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())

			cluster.On(stage.rpc, int64(42), mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			task.CreateTaskOnWorker(42, cluster, task.GetTaskResource())
			require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
		})
	}
}

func TestBackupImportPluginContexts(t *testing.T) {
	source := &commonpb.KeyValuePair{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:aGlzdG9yaWNhbA=="}
	target := &commonpb.KeyValuePair{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:b3JkaW5hcnk="}
	for _, stage := range []struct {
		rpc     string
		newTask importContextFixture
	}{
		{"CreatePreImport", newPreImportWriterContextTask},
		{"CreateImport", newImportWriterContextTask},
	} {
		t.Run(stage.rpc, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				source bool
				target bool
				want   []*commonpb.KeyValuePair
			}{
				{"plaintext", false, false, nil},
				{"source only", true, false, []*commonpb.KeyValuePair{source}},
				{"target only", false, true, []*commonpb.KeyValuePair{target}},
				{"same EZ different keysets", true, true, []*commonpb.KeyValuePair{source, target}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV2)
					job.Options = []*commonpb.KeyValuePair{
						{Key: importutilv2.BackupFlag, Value: "true"},
						{Key: importutilv2.StorageVersion, Value: "2"},
					}
					if tc.source {
						job.Options = append(job.Options, &commonpb.KeyValuePair{Key: importutilv2.EZK, Value: backupArtifact})
					}
					if !tc.target {
						job.Schema.Properties = nil
					}
					cipher := &backupContextCipher{writerContextCipher: writerContextCipher{key: []byte("ordinary")}}
					patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
					t.Cleanup(func() { patch.UnPatch() })
					task, _ := stage.newTask(t, job)
					cluster := session.NewMockCluster(t)
					cluster.On(stage.rpc, int64(42), mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
						req := args.Get(1).(interface {
							GetPluginContext() []*commonpb.KeyValuePair
						})
						requireImportKeysets(t, tc.want, req.GetPluginContext())
					}).Return(nil).Once()

					task.CreateTaskOnWorker(42, cluster, task.GetTaskResource())
					require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
					if tc.source {
						require.Equal(t, []string{backupArtifact}, cipher.artifacts)
					} else {
						require.Empty(t, cipher.artifacts)
					}
					if tc.target {
						require.Equal(t, [][2]int64{{17, job.GetCollectionID()}}, cipher.targets)
					} else {
						require.Empty(t, cipher.targets)
					}
				})
			}
		})
	}
}
