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

// make sure the main functions work well when EnableActiveStandby=false
func TestMixcoord_DisableActiveStandby(t *testing.T) {
	randVal := rand.Int()
	paramtable.Init()
	testutil.ResetEnvironment()
	Params.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
	// Need to reset global etcd to follow new path
	kvfactory.CloseEtcdClient()

	paramtable.Get().Save(Params.MixCoordCfg.EnableActiveStandby.Key, "false")
	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))

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
	assert.Equal(t, commonpb.StateCode_Initializing, core.GetStateCode())
	err = core.Start()
	assert.NoError(t, err)
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, core.GetStateCode())
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
