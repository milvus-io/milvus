// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package coordinator

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/util/dependency"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
)

func TestMixcoord_EnableActiveStandby(t *testing.T) {
	randVal := rand.Int()
	paramtable.Init()
	testutil.ResetEnvironment()
	Params.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
	// Need to reset global etcd to follow new path
	kvfactory.CloseEtcdClient()
	paramtable.Get().Save(Params.MixCoordCfg.EnableActiveStandby.Key, "true")
	defer paramtable.Get().Reset(Params.MixCoordCfg.EnableActiveStandby.Key)
	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordTimeTick.Key)
	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordStatistics.Key)
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))
	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordDml.Key)

	ctx := context.Background()
	coreFactory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	core, err := NewMixCoordServer(ctx, coreFactory)
	core.SetEtcdClient(etcdCli)
	assert.NoError(t, err)
	core.SetTiKVClient(tikv.SetupLocalTxn())

	err = core.Init()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_StandBy, core.GetStateCode())
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
	err = core.Start()
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return core.GetStateCode() == commonpb.StateCode_Healthy
	}, time.Second*5, time.Millisecond*200)
	resp, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		CollectionName: "unexist",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	err = core.Stop()
	assert.NoError(t, err)
}

func TestMixCoord_FlushAll(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockey.PatchConvey("test flush all success", t, func() {
			mockDataCoord := &datacoord.Server{}
			coord := &mixCoordImpl{
				datacoordServer: mockDataCoord,
			}
			expectedResp := &datapb.FlushAllResponse{
				Status:  merr.Success(),
				FlushTs: 12345,
			}
			mockey.Mock((*datacoord.Server).FlushAll).Return(expectedResp, nil).Build()

			resp, err := coord.FlushAll(context.Background(), &datapb.FlushAllRequest{})
			assert.NoError(t, err)
			assert.Equal(t, expectedResp, resp)
		})
	})

	t.Run("failure", func(t *testing.T) {
		mockey.PatchConvey("test flush all failure", t, func() {
			mockDataCoord := &datacoord.Server{}
			coord := &mixCoordImpl{
				datacoordServer: mockDataCoord,
			}
			expectedErr := errors.New("mock flush all error")
			mockey.Mock((*datacoord.Server).FlushAll).Return(nil, expectedErr).Build()

			resp, err := coord.FlushAll(context.Background(), &datapb.FlushAllRequest{})
			assert.Error(t, err)
			assert.Equal(t, expectedErr, err)
			assert.Nil(t, resp)
		})
	})
}

func TestMixCoord_checkExpiredPOSIXDIR(t *testing.T) {
	t.Run("POSIX mode disabled", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().Save(Params.CommonCfg.EnablePosixMode.Key, "false")

		// Create temporary directory for testing
		tempDir := t.TempDir()
		rootCachePath := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(rootCachePath, 0o755)
		assert.NoError(t, err)

		// Create some directories
		nodeIDs := []int64{1001, 1002, 2001}
		for _, nodeID := range nodeIDs {
			nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
			err := os.MkdirAll(nodeDir, 0o755)
			assert.NoError(t, err)
		}

		coord := &mixCoordImpl{
			ctx: context.Background(),
		}

		// Should not remove any directories when POSIX mode is disabled
		coord.checkExpiredPOSIXDIR()

		// Verify all directories still exist
		for _, nodeID := range nodeIDs {
			nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
			assert.DirExists(t, nodeDir)
		}
	})

	t.Run("POSIX mode enabled - no expired directories", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().Save(Params.CommonCfg.EnablePosixMode.Key, "true")

		// Create temporary directory for testing
		tempDir := t.TempDir()
		rootCachePath := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(rootCachePath, 0o755)
		assert.NoError(t, err)

		// Create some valid node directories
		activeNodeIDs := []int64{1001, 1002, 2001}
		for _, nodeID := range activeNodeIDs {
			nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
			err := os.MkdirAll(nodeDir, 0o755)
			assert.NoError(t, err)
		}

		// Mock queryCoord and dataCoord servers
		mockQueryCoord := &querycoordv2.Server{}
		mockDataCoord := &datacoord.Server{}

		coord := &mixCoordImpl{
			ctx:              context.Background(),
			queryCoordServer: mockQueryCoord,
			datacoordServer:  mockDataCoord,
		}

		// Mock ServerExist methods
		mockey.PatchConvey("test POSIX cleanup with no expired dirs", t, func() {
			// Mock ServerExist to return true for QueryCoord, false for DataCoord
			mockey.Mock((*querycoordv2.Server).ServerExist).Return(true).Build()
			mockey.Mock((*datacoord.Server).ServerExist).Return(false).Build()
			mockey.Mock(pathutil.GetPath).Return(rootCachePath).Build()

			// Should not remove any directories
			coord.checkExpiredPOSIXDIR()

			// Verify all directories still exist
			for _, nodeID := range activeNodeIDs {
				nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
				assert.DirExists(t, nodeDir)
			}
		})
	})

	t.Run("POSIX mode enabled - with expired directories", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().Save(Params.CommonCfg.EnablePosixMode.Key, "true")

		// Create temporary directory for testing
		tempDir := t.TempDir()
		rootCachePath := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(rootCachePath, 0o755)
		assert.NoError(t, err)

		// Create node directories (some active, some expired)
		activeNodeIDs := []int64{1001, 1002}
		expiredNodeIDs := []int64{1003, 2002}
		allNodeIDs := append(activeNodeIDs, expiredNodeIDs...)

		for _, nodeID := range allNodeIDs {
			nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
			err := os.MkdirAll(nodeDir, 0o755)
			assert.NoError(t, err)
		}

		// Mock queryCoord and dataCoord servers
		mockQueryCoord := &querycoordv2.Server{}
		mockDataCoord := &datacoord.Server{}

		coord := &mixCoordImpl{
			ctx:              context.Background(),
			queryCoordServer: mockQueryCoord,
			datacoordServer:  mockDataCoord,
		}

		// Mock ServerExist methods - return false for all nodes (simulating expired state)
		mockey.PatchConvey("test POSIX cleanup with expired dirs", t, func() {
			mockey.Mock((*querycoordv2.Server).ServerExist).Return(false).Build()
			mockey.Mock((*datacoord.Server).ServerExist).Return(false).Build()
			mockey.Mock(pathutil.GetPath).Return(rootCachePath).Build()

			// Should remove all directories since all nodes are expired
			coord.checkExpiredPOSIXDIR()

			// Verify all directories are removed
			for _, nodeID := range allNodeIDs {
				nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
				assert.NoDirExists(t, nodeDir)
			}
		})
	})

	t.Run("POSIX mode enabled - all nodes active", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().Save(Params.CommonCfg.EnablePosixMode.Key, "true")

		// Create temporary directory for testing
		tempDir := t.TempDir()
		rootCachePath := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(rootCachePath, 0o755)
		assert.NoError(t, err)

		// Create node directories
		nodeIDs := []int64{1001, 1002, 2001, 2002}

		for _, nodeID := range nodeIDs {
			nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
			err := os.MkdirAll(nodeDir, 0o755)
			assert.NoError(t, err)
		}

		// Mock queryCoord and dataCoord servers
		mockQueryCoord := &querycoordv2.Server{}
		mockDataCoord := &datacoord.Server{}

		coord := &mixCoordImpl{
			ctx:              context.Background(),
			queryCoordServer: mockQueryCoord,
			datacoordServer:  mockDataCoord,
		}

		// Mock ServerExist methods
		mockey.PatchConvey("test POSIX cleanup with all nodes active", t, func() {
			// Mock ServerExist to return true for both QueryCoord and DataCoord (all nodes active)
			mockey.Mock((*querycoordv2.Server).ServerExist).Return(true).Build()
			mockey.Mock((*datacoord.Server).ServerExist).Return(true).Build()
			mockey.Mock(pathutil.GetPath).Return(rootCachePath).Build()

			// Should not remove any directories
			coord.checkExpiredPOSIXDIR()

			// Verify all directories still exist
			for _, nodeID := range nodeIDs {
				nodeDir := filepath.Join(rootCachePath, fmt.Sprintf("%d", nodeID))
				assert.DirExists(t, nodeDir)
			}
		})
	})

	t.Run("POSIX mode enabled - invalid directory names", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().Save(Params.CommonCfg.EnablePosixMode.Key, "true")

		// Create temporary directory for testing
		tempDir := t.TempDir()
		rootCachePath := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(rootCachePath, 0o755)
		assert.NoError(t, err)

		// Create directories with invalid names
		invalidDirs := []string{"invalid_dir", "node_abc", "123abc"}
		for _, dirName := range invalidDirs {
			invalidDir := filepath.Join(rootCachePath, dirName)
			err := os.MkdirAll(invalidDir, 0o755)
			assert.NoError(t, err)
		}

		// Create one valid directory
		validDir := filepath.Join(rootCachePath, "1001")
		err = os.MkdirAll(validDir, 0o755)
		assert.NoError(t, err)

		// Mock queryCoord and dataCoord servers
		mockQueryCoord := &querycoordv2.Server{}
		mockDataCoord := &datacoord.Server{}

		coord := &mixCoordImpl{
			ctx:              context.Background(),
			queryCoordServer: mockQueryCoord,
			datacoordServer:  mockDataCoord,
		}

		// Mock ServerExist methods
		mockey.PatchConvey("test POSIX cleanup with invalid dir names", t, func() {
			// Mock ServerExist for the valid node
			mockey.Mock((*querycoordv2.Server).ServerExist).Return(true).Build()
			mockey.Mock((*datacoord.Server).ServerExist).Return(false).Build()
			mockey.Mock(pathutil.GetPath).Return(rootCachePath).Build()

			// Should handle invalid directory names gracefully
			coord.checkExpiredPOSIXDIR()

			// Verify valid directory still exists
			assert.DirExists(t, validDir)

			// Verify invalid directories are not removed (they should be ignored)
			for _, dirName := range invalidDirs {
				invalidDir := filepath.Join(rootCachePath, dirName)
				assert.DirExists(t, invalidDir)
			}
		})
	})

	t.Run("POSIX mode enabled - read directory error", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().Save(Params.CommonCfg.EnablePosixMode.Key, "true")

		// Mock queryCoord and dataCoord servers
		mockQueryCoord := &querycoordv2.Server{}
		mockDataCoord := &datacoord.Server{}

		coord := &mixCoordImpl{
			ctx:              context.Background(),
			queryCoordServer: mockQueryCoord,
			datacoordServer:  mockDataCoord,
		}

		// Mock ServerExist methods
		mockey.PatchConvey("test POSIX cleanup with read dir error", t, func() {
			mockey.Mock(pathutil.GetPath).Return("/non/existent/path").Build()

			// Should handle read directory error gracefully
			coord.checkExpiredPOSIXDIR()
		})
	})
}

func TestMixCoord_SnapshotMethods(t *testing.T) {
	ctx := context.Background()

	t.Run("RestoreSnapshot", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "test_collection",
		}

		mockey.PatchConvey("test RestoreSnapshot", t, func() {
			expectedResp := &datapb.RestoreSnapshotResponse{
				Status: merr.Success(),
				JobId:  12345,
			}
			mockey.Mock((*datacoord.Server).RestoreSnapshot).Return(expectedResp, nil).Build()

			resp, err := coord.RestoreSnapshot(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, int64(12345), resp.GetJobId())
		})
	})

	t.Run("GetRestoreSnapshotState", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.GetRestoreSnapshotStateRequest{
			JobId: 12345,
		}

		mockey.PatchConvey("test GetRestoreSnapshotState", t, func() {
			expectedResp := &datapb.GetRestoreSnapshotStateResponse{
				Status: merr.Success(),
				Info: &datapb.RestoreSnapshotInfo{
					JobId:        12345,
					SnapshotName: "test_snapshot",
					CollectionId: 1001,
					State:        datapb.RestoreSnapshotState_RestoreSnapshotCompleted,
				},
			}
			mockey.Mock((*datacoord.Server).GetRestoreSnapshotState).Return(expectedResp, nil).Build()

			resp, err := coord.GetRestoreSnapshotState(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.NotNil(t, resp.GetInfo())
			assert.Equal(t, datapb.RestoreSnapshotState_RestoreSnapshotCompleted, resp.GetInfo().GetState())
		})
	})

	t.Run("ListRestoreSnapshotJobs", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.ListRestoreSnapshotJobsRequest{
			CollectionId: 1001,
		}

		mockey.PatchConvey("test ListRestoreSnapshotJobs", t, func() {
			expectedResp := &datapb.ListRestoreSnapshotJobsResponse{
				Status: merr.Success(),
				Jobs: []*datapb.RestoreSnapshotInfo{
					{
						JobId:        12345,
						SnapshotName: "test_snapshot",
						CollectionId: 1001,
						State:        datapb.RestoreSnapshotState_RestoreSnapshotCompleted,
					},
				},
			}
			mockey.Mock((*datacoord.Server).ListRestoreSnapshotJobs).Return(expectedResp, nil).Build()

			resp, err := coord.ListRestoreSnapshotJobs(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, 1, len(resp.GetJobs()))
			assert.Equal(t, int64(12345), resp.GetJobs()[0].GetJobId())
		})
	})
}

func TestMixCoord_ExternalCollectionRefreshMethods(t *testing.T) {
	ctx := context.Background()

	t.Run("RefreshExternalCollection_Success", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.RefreshExternalCollectionRequest{
			CollectionId:   1001,
			CollectionName: "test_collection",
		}

		expectedResp := &datapb.RefreshExternalCollectionResponse{
			Status: merr.Success(),
			JobId:  54321,
		}
		mockRefresh := mockey.Mock((*datacoord.Server).RefreshExternalCollection).Return(expectedResp, nil).Build()
		defer mockRefresh.UnPatch()

		resp, err := coord.RefreshExternalCollection(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, int64(54321), resp.GetJobId())
		assert.Equal(t, merr.Success(), resp.GetStatus())
	})

	t.Run("RefreshExternalCollection_Error", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.RefreshExternalCollectionRequest{
			CollectionId:   1001,
			CollectionName: "test_collection",
		}

		expectedErr := errors.New("mock refresh error")
		mockRefresh := mockey.Mock((*datacoord.Server).RefreshExternalCollection).Return(nil, expectedErr).Build()
		defer mockRefresh.UnPatch()

		resp, err := coord.RefreshExternalCollection(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, resp)
	})

	t.Run("GetRefreshExternalCollectionProgress_Success", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.GetRefreshExternalCollectionProgressRequest{
			JobId: 54321,
		}

		expectedResp := &datapb.GetRefreshExternalCollectionProgressResponse{
			Status: merr.Success(),
			JobInfo: &datapb.ExternalCollectionRefreshJob{
				JobId:          54321,
				CollectionName: "test_collection",
				State:          indexpb.JobState_JobStateInProgress,
				Progress:       50,
			},
		}
		mockProgress := mockey.Mock((*datacoord.Server).GetRefreshExternalCollectionProgress).Return(expectedResp, nil).Build()
		defer mockProgress.UnPatch()

		resp, err := coord.GetRefreshExternalCollectionProgress(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, int64(54321), resp.GetJobInfo().GetJobId())
		assert.Equal(t, int64(50), resp.GetJobInfo().GetProgress())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, resp.GetJobInfo().GetState())
	})

	t.Run("GetRefreshExternalCollectionProgress_Error", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.GetRefreshExternalCollectionProgressRequest{
			JobId: 54321,
		}

		expectedErr := errors.New("mock get progress error")
		mockProgress := mockey.Mock((*datacoord.Server).GetRefreshExternalCollectionProgress).Return(nil, expectedErr).Build()
		defer mockProgress.UnPatch()

		resp, err := coord.GetRefreshExternalCollectionProgress(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, resp)
	})

	t.Run("ListRefreshExternalCollectionJobs_Success", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.ListRefreshExternalCollectionJobsRequest{
			CollectionId: 1001,
		}

		expectedResp := &datapb.ListRefreshExternalCollectionJobsResponse{
			Status: merr.Success(),
			Jobs: []*datapb.ExternalCollectionRefreshJob{
				{
					JobId:          54321,
					CollectionName: "test_collection",
					State:          indexpb.JobState_JobStateFinished,
					Progress:       100,
				},
				{
					JobId:          54322,
					CollectionName: "test_collection",
					State:          indexpb.JobState_JobStateFailed,
					Progress:       50,
					FailReason:     "Test failure",
				},
			},
		}
		mockList := mockey.Mock((*datacoord.Server).ListRefreshExternalCollectionJobs).Return(expectedResp, nil).Build()
		defer mockList.UnPatch()

		resp, err := coord.ListRefreshExternalCollectionJobs(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, 2, len(resp.GetJobs()))
		assert.Equal(t, int64(54321), resp.GetJobs()[0].GetJobId())
		assert.Equal(t, indexpb.JobState_JobStateFinished, resp.GetJobs()[0].GetState())
		assert.Equal(t, int64(54322), resp.GetJobs()[1].GetJobId())
		assert.Equal(t, indexpb.JobState_JobStateFailed, resp.GetJobs()[1].GetState())
	})

	t.Run("ListRefreshExternalCollectionJobs_Error", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.ListRefreshExternalCollectionJobsRequest{
			CollectionId: 1001,
		}

		expectedErr := errors.New("mock list jobs error")
		mockList := mockey.Mock((*datacoord.Server).ListRefreshExternalCollectionJobs).Return(nil, expectedErr).Build()
		defer mockList.UnPatch()

		resp, err := coord.ListRefreshExternalCollectionJobs(ctx, req)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, resp)
	})

	t.Run("ListRefreshExternalCollectionJobs_EmptyList", func(t *testing.T) {
		mockDataCoord := &datacoord.Server{}
		coord := &mixCoordImpl{
			datacoordServer: mockDataCoord,
		}

		req := &datapb.ListRefreshExternalCollectionJobsRequest{
			CollectionId: 9999,
		}

		expectedResp := &datapb.ListRefreshExternalCollectionJobsResponse{
			Status: merr.Success(),
			Jobs:   []*datapb.ExternalCollectionRefreshJob{},
		}
		mockList := mockey.Mock((*datacoord.Server).ListRefreshExternalCollectionJobs).Return(expectedResp, nil).Build()
		defer mockList.UnPatch()

		resp, err := coord.ListRefreshExternalCollectionJobs(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, 0, len(resp.GetJobs()))
	})
}
