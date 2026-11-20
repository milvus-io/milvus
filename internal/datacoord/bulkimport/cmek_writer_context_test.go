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
	"errors"
	"net"
	"testing"

	"github.com/bytedance/mockey"
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
						require.Equal(t, int64(meta.NullNodeID), task.GetNodeID())
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

func (c *backupContextCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	c.artifacts = append(c.artifacts, artifact)
	return 17, []byte("historical"), c.sourceErr
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
	cluster.On(rpc, int64(41), mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req := args.Get(1).(interface {
			GetPluginContext() []*commonpb.KeyValuePair
		})
		requireImportKeysets(t, []*commonpb.KeyValuePair{
			{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:aGlzdG9yaWNhbA=="},
			{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:b3JkaW5hcnk="},
		}, req.GetPluginContext())
	}).Return(errors.New("worker unavailable")).Once()

	task.CreateTaskOnWorker(41, cluster)
	cipher.sourceErr = errors.New("source KMS denied Decrypt")
	task.CreateTaskOnWorker(42, cluster)

	require.Equal(t, []string{backupArtifact, backupArtifact}, cipher.artifacts)
	failed := im.GetJob(context.Background(), job.GetJobID())
	require.Equal(t, internalpb.ImportJobState_Failed, failed.GetState())
	require.Contains(t, failed.GetReason(), cipher.sourceErr.Error())
	cluster.AssertNotCalled(t, rpc, int64(42), mock.Anything, mock.Anything)
}

type temporarySourceError struct{ error }

func (temporarySourceError) Temporary() bool { return true }

var backupDispatchStages = []struct {
	rpc     string
	newTask importContextFixture
	state   internalpb.ImportJobState
}{
	{"CreatePreImport", newPreImportWriterContextTask, internalpb.ImportJobState_PreImporting},
	{"CreateImport", newImportWriterContextTask, internalpb.ImportJobState_Importing},
}

func TestImportRetriesTransientSourceError(t *testing.T) {
	for _, stage := range backupDispatchStages {
		t.Run(stage.rpc, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				err   error
				retry bool
			}{
				{"typed unavailable", merr.ErrServiceUnavailable, true},
				{"temporary plugin error", temporarySourceError{errors.New("retry decrypt")}, true},
				{"deadline", context.DeadlineExceeded, true},
				{"revoked", errors.New("source permission revoked"), false},
				{"network timeout", &net.DNSError{Err: "timeout", IsTimeout: true}, true},
				{"untyped error text", errors.New("denied even though the message mentions HTTP 503"), false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV2)
					job.Options = backupContextOptions()
					cipher := &backupContextCipher{
						writerContextCipher: writerContextCipher{key: []byte("ordinary")},
						sourceErr:           errors.Join(errors.New("KMS failed to decrypt"), tc.err),
					}
					patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
					t.Cleanup(func() { patch.UnPatch() })
					task, im := stage.newTask(t, job)
					cluster := session.NewMockCluster(t)
					task.CreateTaskOnWorker(41, cluster)

					got := im.GetJob(context.Background(), job.GetJobID())
					cluster.AssertNotCalled(t, stage.rpc, int64(41), mock.Anything, mock.Anything)
					if !tc.retry {
						require.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
						require.Equal(t, "resolve backup EZK: "+cipher.sourceErr.Error(), got.GetReason())
						return
					}
					require.Equal(t, stage.state, got.GetState())
					require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
					cipher.sourceErr = nil
					cluster.On(stage.rpc, int64(42), mock.Anything, mock.Anything).Return(nil).Once()
					task.CreateTaskOnWorker(42, cluster)
					require.Equal(t, []string{backupArtifact, backupArtifact}, cipher.artifacts)
					require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
				})
			}
		})
	}
}

func TestPreImportV3ResolvesSourceOnEachDispatch(t *testing.T) {
	testBackupSourceOnEachDispatch(t, newPreImportWriterContextTask, "CreatePreImportV3")
}

func TestV3RejectsRevokedBackupBeforeDispatch(t *testing.T) {
	for _, stage := range []string{"reshard", "import"} {
		t.Run(stage, func(t *testing.T) {
			job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV3)
			job.Options = backupContextOptions()
			job.State = internalpb.ImportJobState_Resharding
			if stage == "import" {
				job.State = internalpb.ImportJobState_Importing
			}
			im := newWriterContextImportMeta(t, job)
			var task ImportTask
			if stage == "reshard" {
				task = newReshardTask(&datapb.ReshardTask{JobId: job.JobID, TaskId: 4, RunId: 1}, im, nil, nil)
			} else {
				task = NewImportTaskV3(&datapb.ImportTaskV3{JobId: job.JobID, TaskId: 4, RunId: 1, LogRange: &datapb.IDRange{Begin: 1, End: 5}}, im, nil, nil)
			}
			cipher := &backupContextCipher{sourceErr: temporarySourceError{errors.New("temporary decrypt error")}}
			patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
			defer patch.UnPatch()
			cluster := session.NewMockCluster(t)
			task.CreateTaskOnWorker(1, cluster)
			require.Equal(t, job.State, im.GetJob(context.Background(), job.JobID).GetState())
			cipher.sourceErr = errors.New("source identity denied Decrypt")
			task.CreateTaskOnWorker(2, cluster)
			got := im.GetJob(context.Background(), job.JobID)
			require.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
			require.Contains(t, got.GetReason(), cipher.sourceErr.Error())
			require.Len(t, cipher.artifacts, 2)
			require.Empty(t, cipher.targets)
		})
	}
}

func TestImportWorkerCipherRejections(t *testing.T) {
	backup := []*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}}
	malformed := merr.WrapErrImportSysFailedMsg("import task has an invalid keyset for EZ 17")
	for _, stage := range backupDispatchStages {
		t.Run(stage.rpc, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				options  []*commonpb.KeyValuePair
				err      error
				terminal bool
			}{
				{"conflicting read keyset", backupContextOptions(), merr.WrapErrImportSysFailedMsg("conflicting read keyset"), true},
				{"malformed context", backup, malformed, true},
				{"L0 with malformed context", []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}}, malformed, true},
				{"source artifact only", []*commonpb.KeyValuePair{{Key: importutilv2.EZK, Value: backupArtifact}}, malformed, true},
				{"ordinary import", nil, malformed, false},
				{"worker not ready", backup, merr.ErrServiceNotReady, false},
				{"old worker unimplemented", backup, merr.ErrServiceUnimplemented, false},
				{"other worker internal error", backup, merr.ErrServiceInternal, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					job := preImportWriterContextJob(internalpb.ImportVersion_ImportVersionV2)
					job.Options = tc.options
					cipher := &backupContextCipher{writerContextCipher: writerContextCipher{key: []byte("ordinary")}}
					patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
					t.Cleanup(func() { patch.UnPatch() })
					task, im := stage.newTask(t, job)
					cluster := session.NewMockCluster(t)
					// The coordinator sees the decoded application status from CreateTask.
					workerErr := merr.CheckRPCCall(merr.Status(tc.err), nil)
					cluster.On(stage.rpc, int64(41), mock.Anything, mock.Anything).Return(workerErr).Once()
					task.CreateTaskOnWorker(41, cluster)
					got := im.GetJob(context.Background(), job.GetJobID())
					if tc.terminal {
						require.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
						require.Equal(t, workerErr.Error(), got.GetReason())
						return
					}
					require.Equal(t, stage.state, got.GetState())
					cluster.On(stage.rpc, int64(42), mock.Anything, mock.Anything).Return(nil).Once()
					task.CreateTaskOnWorker(42, cluster)
					require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
				})
			}
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
					cluster.On(stage.rpc, int64(42), mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
						req := args.Get(1).(interface {
							GetPluginContext() []*commonpb.KeyValuePair
						})
						requireImportKeysets(t, tc.want, req.GetPluginContext())
					}).Return(nil).Once()

					task.CreateTaskOnWorker(42, cluster)
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
