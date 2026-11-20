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
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type importRequestCipher struct {
	*mock_hook.MockCipher
	resolve func(artifact string) (int64, []byte, error)
}

func (c *importRequestCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	return c.resolve(artifact)
}

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
				JobID: 1, State: state, DataTs: 100, Options: []*commonpb.KeyValuePair{{Key: importutilv2.EZK, Value: backup}},
			}}, nil)
			catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(errors.New("catalog unavailable")).Once()
			catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
			cluster := session.NewMockCluster(t)
			alloc := allocator.NewMockAllocator(t)
			workerOffline := errors.New("worker unavailable")
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
			for _, transient := range []error{context.DeadlineExceeded, merr.ErrServiceUnavailable} {
				sourceErr = fmt.Errorf("%w: %w", errors.New("decrypt failed"), transient)
				task.CreateTaskOnWorker(10, cluster)
				require.Equal(t, state, im.GetJob(ctx, 1).GetState())
				catalog.AssertNotCalled(t, "SaveImportJob", mock.Anything, mock.Anything)
			}

			sourceErr = nil
			task.CreateTaskOnWorker(11, cluster)
			require.Equal(t, state, im.GetJob(ctx, 1).GetState())

			sourceErr = errors.New("source KMS denied Decrypt")
			task.CreateTaskOnWorker(12, cluster)
			require.Equal(t, state, im.GetJob(ctx, 1).GetState())
			task.CreateTaskOnWorker(12, cluster)
			job := im.GetJob(ctx, 1)
			require.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
			require.Contains(t, job.GetReason(), sourceErr.Error())

			// The scheduler can revisit a pending task before the import checker fails it.
			sourceErr = nil
			task.CreateTaskOnWorker(13, cluster)
			require.Equal(t, 5, resolves)
		})
	}
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
				cluster.EXPECT().CreatePreImport(int64(11), mock.Anything, mock.Anything).Return(rejected).Once()
				task = pre
			} else {
				alloc := allocator.NewMockAllocator(t)
				alloc.EXPECT().AllocN(mock.Anything).Return(10, 20, nil).Once()
				imp := &importTask{importMeta: im, alloc: alloc}
				imp.task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 2})
				cluster.EXPECT().CreateImport(int64(11), mock.Anything, mock.Anything).Return(rejected).Once()
				task = imp
			}
			task.CreateTaskOnWorker(11, cluster)
			require.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
			require.Contains(t, job.GetReason(), "incompatible historical read keys")
			task.CreateTaskOnWorker(11, cluster)
		})
	}
}
