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
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mocks/github.com/milvus-io/milvus-proto/go-api/v2/mock_hook"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type importRequestCipher struct {
	*mock_hook.MockCipher
	resolve func(artifact string) (int64, []byte, error)
}

func (*importRequestCipher) Backup(int64) (string, error) {
	panic("dispatch should resolve the supplied backup")
}

func (c *importRequestCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	return c.resolve(artifact)
}

type temporaryImportTestError struct{ error }

func (temporaryImportTestError) Temporary() bool { return true }

func TestImportRequestsCarrySourceAndTargetEZKs(t *testing.T) {
	backup := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":73}`))
	for _, tc := range []struct {
		name     string
		source   bool
		targetID int64
		want     []string
	}{
		{"plaintext", false, 0, nil},
		{"source only", true, 0, []string{"73:b2xk"}},
		{"target only", false, 91, []string{"91:bmV3"}},
		{"different zones", true, 91, []string{"73:b2xk", "91:bmV3"}},
		{"same zone", true, 73, []string{"73:b2xk", "73:bmV3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cipher := &importRequestCipher{MockCipher: mock_hook.NewMockCipher(t)}
			t.Cleanup(hookutil.SetTestCipher(cipher))
			job := &importJob{ImportJob: &datapb.ImportJob{
				JobID: 1, CollectionID: 7, DataTs: 100, Schema: &schemapb.CollectionSchema{},
			}}
			if tc.source {
				job.Options = []*commonpb.KeyValuePair{{Key: importutilv2.EZK, Value: backup}}
				cipher.resolve = func(artifact string) (int64, []byte, error) {
					require.Equal(t, backup, artifact)
					return 73, []byte("old"), nil
				}
			}
			if tc.targetID != 0 {
				job.Schema.Properties = []*commonpb.KeyValuePair{{
					Key: common.EncryptionEzIDKey, Value: strconv.FormatInt(tc.targetID, 10),
				}}
				cipher.EXPECT().GetUnsafeKey(tc.targetID, int64(7)).Return([]byte("new")).Twice()
			}
			im := NewMockImportMeta(t)
			im.EXPECT().GetJob(mock.Anything, int64(1)).Return(job)
			pre := &preImportTask{importMeta: im}
			pre.task.Store(&datapb.PreImportTask{JobID: 1, CollectionID: 7})
			preReq, err := AssemblePreImportRequest(pre, job)
			require.NoError(t, err)

			imp := &importTask{importMeta: im}
			imp.task.Store(&datapb.ImportTaskV2{JobID: 1, CollectionID: 7})
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocN(mock.Anything).Return(10, 20, nil)
			impReq, err := AssembleImportRequest(imp, job, nil, alloc)
			require.NoError(t, err)

			for _, entries := range [][]*commonpb.KeyValuePair{preReq.GetPluginContext(), impReq.GetPluginContext()} {
				var values []string
				for _, entry := range entries {
					require.Equal(t, hookutil.CipherConfigUnsafeEZK, entry.GetKey())
					values = append(values, entry.GetValue())
				}
				require.Equal(t, tc.want, values)
			}
		})
	}
}

func TestImportDispatchStopsOnEZKError(t *testing.T) {
	ctx := context.Background()
	backup := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":73}`))
	schema := &schemapb.CollectionSchema{Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "91"}}}
	for _, state := range []internalpb.ImportJobState{internalpb.ImportJobState_PreImporting, internalpb.ImportJobState_Importing} {
		t.Run(state.String(), func(t *testing.T) {
			var sourceErr error
			var resolves int
			cipher := &importRequestCipher{MockCipher: mock_hook.NewMockCipher(t), resolve: func(artifact string) (int64, []byte, error) {
				resolves++
				require.Equal(t, backup, artifact)
				return 73, []byte("old"), sourceErr
			}}
			t.Cleanup(hookutil.SetTestCipher(cipher))

			catalog := mocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListImportJobs(mock.Anything).Return([]*datapb.ImportJob{{
				JobID: 1, Schema: schema, State: state, DataTs: 100, Options: []*commonpb.KeyValuePair{{Key: importutilv2.EZK, Value: backup}},
			}}, nil)
			catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(errors.New("catalog unavailable")).Once()
			catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
			cluster := session.NewMockCluster(t)
			alloc := allocator.NewMockAllocator(t)
			workerOffline := merr.Error(merr.Status(merr.WrapErrServiceUnavailable("worker unavailable")))
			if state == internalpb.ImportJobState_PreImporting {
				catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{
					JobID: 1, TaskID: 2, State: datapb.ImportTaskStateV2_Pending,
				}}, nil)
				catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
				cluster.EXPECT().CreatePreImport(int64(11), mock.Anything, mock.Anything).Return(workerOffline).Once()
			} else {
				catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
				catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{
					JobID: 1, TaskID: 2, State: datapb.ImportTaskStateV2_Pending,
				}}, nil)
				alloc.EXPECT().AllocN(mock.Anything).Return(10, 20, nil)
				cluster.EXPECT().CreateImport(int64(11), mock.Anything, mock.Anything).Return(workerOffline).Once()
			}
			im, err := NewImportMeta(ctx, catalog, alloc, nil)
			require.NoError(t, err)
			task := im.GetTask(ctx, 2)
			transients := []error{
				context.Canceled,
				context.DeadlineExceeded,
				io.ErrUnexpectedEOF,
				merr.ErrServiceUnavailable,
				merr.WrapErrServiceInternalErr(merr.ErrServiceUnavailable, "KMS client failed"),
				status.Error(codes.Unavailable, "source temporarily unavailable"),
				temporaryImportTestError{errors.New("source KMS throttled Decrypt")},
			}
			for _, transient := range transients {
				sourceErr = fmt.Errorf("%w: %w", errors.New("decrypt failed"), transient)
				task.CreateTaskOnWorker(10, cluster)
				require.Equal(t, state, im.GetJob(ctx, 1).GetState())
				require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
				catalog.AssertNotCalled(t, "SaveImportJob", mock.Anything, mock.Anything)
			}

			sourceErr = nil
			cipher.EXPECT().GetUnsafeKey(int64(91), task.GetCollectionID()).Return(nil).Once()
			task.CreateTaskOnWorker(10, cluster)
			require.Equal(t, state, im.GetJob(ctx, 1).GetState())
			require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
			catalog.AssertNotCalled(t, "SaveImportJob", mock.Anything, mock.Anything)

			cipher.EXPECT().GetUnsafeKey(int64(91), task.GetCollectionID()).Return([]byte("new")).Once()
			task.CreateTaskOnWorker(11, cluster)
			require.Equal(t, state, im.GetJob(ctx, 1).GetState())
			require.EqualValues(t, 1, task.GetTaskVersion())

			sourceErr = errors.New("source KMS denied Decrypt")
			job := im.GetJob(ctx, 1)
			_, err = getImportPluginContext(job.GetOptions(), job.GetSchema().GetProperties(), task.GetCollectionID())
			require.True(t, errors.Is(err, merr.ErrImportFailed))
			require.ErrorIs(t, err, sourceErr)
			task.CreateTaskOnWorker(12, cluster)
			require.Equal(t, state, im.GetJob(ctx, 1).GetState())
			task.CreateTaskOnWorker(12, cluster)
			job = im.GetJob(ctx, 1)
			require.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
			require.Contains(t, job.GetReason(), sourceErr.Error())

			// The scheduler can revisit a pending task before the import checker fails it.
			sourceErr = nil
			task.CreateTaskOnWorker(13, cluster)
			require.Equal(t, len(transients)+5, resolves)
		})
	}
}

func TestWrapStoragePluginContext(t *testing.T) {
	const collectionID = 7
	properties := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "91"}}
	for _, req := range []interface {
		proto.Message
		GetPluginContext() []*commonpb.KeyValuePair
	}{
		&datapb.CompactionPlan{}, &workerpb.CreateJobRequest{}, &workerpb.AnalyzeRequest{}, &workerpb.CreateStatsRequest{},
	} {
		t.Run(string(req.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			cipher := mock_hook.NewMockCipher(t)
			t.Cleanup(hookutil.SetTestCipher(cipher))
			cipher.EXPECT().GetUnsafeKey(int64(91), int64(collectionID)).Return(nil).Once()
			require.ErrorIs(t, WrapPluginContext(collectionID, properties, req), merr.ErrServiceUnavailable)
			require.Empty(t, req.GetPluginContext())

			cipher.EXPECT().GetUnsafeKey(int64(91), int64(collectionID)).Return([]byte("new")).Once()
			require.NoError(t, WrapPluginContext(collectionID, properties, req))
			require.Equal(t, []*commonpb.KeyValuePair{{Key: hookutil.CipherConfigUnsafeEZK, Value: "91:bmV3"}}, req.GetPluginContext())
		})
	}
}

func TestCompactionTasksRetryTemporaryPluginContextError(t *testing.T) {
	const collectionID = 7
	const ezID = 91
	schema := &schemapb.CollectionSchema{
		Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: strconv.Itoa(ezID)}},
	}

	t.Run("mix_compaction", func(t *testing.T) {
		cipher := mock_hook.NewMockCipher(t)
		t.Cleanup(hookutil.SetTestCipher(cipher))
		cipher.EXPECT().GetUnsafeKey(int64(ezID), int64(collectionID)).Return(nil).Once()

		compMeta := NewMockCompactionMeta(t)
		compMeta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            200,
			CollectionID:  collectionID,
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
		}}).Once()
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Once()

		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     2,
			CollectionID:  collectionID,
			PartitionID:   10,
			Channel:       "ch-1",
			Type:          datapb.CompactionType_MixCompaction,
			State:         datapb.CompactionTaskState_pipelining,
			InputSegments: []int64{200},
			Schema:        schema,
		}, alloc, compMeta, newMockVersionManager())

		cluster := session.NewMockCluster(t)
		task.CreateTaskOnWorker(11, cluster)
		require.Equal(t, datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
		compMeta.AssertNotCalled(t, "SaveCompactionTask", mock.Anything, mock.Anything)
	})

	t.Run("l0_compaction", func(t *testing.T) {
		cipher := mock_hook.NewMockCipher(t)
		t.Cleanup(hookutil.SetTestCipher(cipher))
		cipher.EXPECT().GetUnsafeKey(int64(ezID), int64(collectionID)).Return(nil).Once()

		compMeta := NewMockCompactionMeta(t)
		compMeta.EXPECT().GetHealthySegment(mock.Anything, int64(300)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            300,
			CollectionID:  collectionID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
		}}).Once()
		compMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return([]*SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{
			ID:            301,
			CollectionID:  collectionID,
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
		}}}).Once()
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Once()

		task := newL0CompactionTask(&datapb.CompactionTask{
			PlanID:        2,
			TriggerID:     3,
			CollectionID:  collectionID,
			PartitionID:   10,
			Channel:       "ch-1",
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			State:         datapb.CompactionTaskState_pipelining,
			InputSegments: []int64{300},
			Schema:        schema,
		}, alloc, compMeta)

		cluster := session.NewMockCluster(t)
		task.CreateTaskOnWorker(11, cluster)
		require.Equal(t, datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
		compMeta.AssertNotCalled(t, "SaveCompactionTask", mock.Anything, mock.Anything)
	})
}

func TestImportDispatchStopsOnWorkerCipherRejection(t *testing.T) {
	// Exercise the error delivered by the CreateTask RPC status round trip.
	rejected := merr.Error(merr.Status(merr.WrapErrImportFailed("incompatible historical read keys")))
	for _, state := range []internalpb.ImportJobState{internalpb.ImportJobState_PreImporting, internalpb.ImportJobState_Importing} {
		t.Run(state.String(), func(t *testing.T) {
			job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: state, DataTs: 100}}
			im := NewMockImportMeta(t)
			im.EXPECT().GetJob(mock.Anything, int64(1)).Return(job)
			im.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
				Return(errors.New("catalog unavailable")).Once()
			im.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
				Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
					for _, apply := range actions {
						apply(job)
					}
				}).Return(nil).Once()
			cluster := session.NewMockCluster(t)
			var task ImportTask
			if state == internalpb.ImportJobState_PreImporting {
				pre := &preImportTask{importMeta: im}
				pre.task.Store(&datapb.PreImportTask{JobID: 1, TaskID: 2})
				cluster.EXPECT().CreatePreImport(int64(11), mock.Anything, mock.Anything).Return(rejected).Twice()
				task = pre
			} else {
				alloc := allocator.NewMockAllocator(t)
				alloc.EXPECT().AllocN(mock.Anything).Return(10, 20, nil).Twice()
				imp := &importTask{importMeta: im, alloc: alloc}
				imp.task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 2})
				cluster.EXPECT().CreateImport(int64(11), mock.Anything, mock.Anything).Return(rejected).Twice()
				task = imp
			}
			task.CreateTaskOnWorker(11, cluster)
			require.Equal(t, state, job.GetState())
			require.EqualValues(t, 1, task.GetTaskVersion())
			task.CreateTaskOnWorker(11, cluster)
			require.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
			require.Contains(t, job.GetReason(), "incompatible historical read keys")
			require.EqualValues(t, 1, task.GetTaskVersion())
			task.CreateTaskOnWorker(11, cluster)
		})
	}
}
