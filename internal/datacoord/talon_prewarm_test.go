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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestPrewarmURI(t *testing.T) {
	descriptor := packed.MarshalManifestPath("files//literal/../insert_log/1/2/3/", 17)
	for _, provider := range []string{"aws", "aliyun", "tencent", "huawei", "gcp", "azure"} {
		t.Run(provider, func(t *testing.T) {
			scheme := "s3"
			if provider == "azure" {
				scheme = "az"
			}
			resolve, err := manifestPrewarmResolver(provider, "bucket///")
			require.NoError(t, err)
			uri, err := resolve(descriptor)
			require.NoError(t, err)
			require.Equal(t, scheme+"://bucket/files//literal/../insert_log/1/2/3//_metadata/manifest-17.avro", uri)
			// Growing V3 metadata starts at ManifestEarliest before any physical file.
			uri, err = resolve(packed.MarshalManifestPath("files/insert_log/1/2/3", 0))
			require.NoError(t, err)
			require.Empty(t, uri)
			_, err = resolve("not-json")
			require.Error(t, err)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
	_, err := manifestPrewarmResolver("unsupported", "bucket")
	require.Error(t, err)
	_, err = manifestPrewarmResolver("aws", "")
	require.Error(t, err)
}

func TestTalonPrewarmConfiguration(t *testing.T) {
	p := &paramtable.ComponentParam{}
	p.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	calls := 0
	factory := func(config *indexpb.StorageConfig, concurrency int) (TalonPrewarmClient, error) {
		calls++
		require.Equal(t, 1, concurrency)
		require.Equal(t, "coordinator:7000", config.GetTalonCoordinator())
		return nil, merr.WrapErrServiceInternalMsg("recorded construction")
	}
	supplier := func(context.Context) ([]string, error) { return nil, nil }
	ob, err := newTalonPrewarm(context.Background(), p, supplier, factory)
	require.NoError(t, err)
	require.Nil(t, ob)
	require.Zero(t, calls)
	require.NoError(t, p.Save(p.DataCoordCfg.TalonPrewarmEnabled.Key, "true"))
	require.NoError(t, p.Save(p.CommonCfg.StorageType.Key, "local"))
	ob, err = newTalonPrewarm(context.Background(), p, nil, factory)
	require.NoError(t, err)
	require.Nil(t, ob)
	require.Zero(t, calls)
	require.NoError(t, p.Save(p.CommonCfg.StorageType.Key, "remote"))
	require.NoError(t, p.Save(p.CommonCfg.StorageTalonCoordinator.Key, "coordinator:7000"))
	require.NoError(t, p.Save(p.MinioCfg.CloudProvider.Key, "aws"))
	require.NoError(t, p.Save(p.MinioCfg.BucketName.Key, "bucket"))
	for _, tc := range []struct{ key, value string }{
		{p.DataCoordCfg.TalonPrewarmInterval.Key, "0"},
		{p.DataCoordCfg.TalonPrewarmInterval.Key, "86401"},
		{p.DataCoordCfg.TalonPrewarmInterval.Key, "garbage"},
		{p.DataCoordCfg.TalonPrewarmBatchSize.Key, "-1"},
		{p.DataCoordCfg.TalonPrewarmBatchSize.Key, "4097"},
	} {
		require.NoError(t, p.Save(tc.key, tc.value))
		_, err = newTalonPrewarm(context.Background(), p, supplier, factory)
		require.Error(t, err)
		require.Zero(t, calls)
		require.NoError(t, p.Reset(tc.key))
	}
	_, err = newTalonPrewarm(context.Background(), p, nil, factory)
	require.Error(t, err)
	require.Zero(t, calls)
	require.NoError(t, p.Save(p.CommonCfg.StorageTalonCoordinator.Key, ""))
	_, err = newTalonPrewarm(context.Background(), p, supplier, factory)
	require.Error(t, err)
	require.Zero(t, calls)
	require.NoError(t, p.Save(p.CommonCfg.StorageTalonCoordinator.Key, "coordinator:7000"))
	_, err = newTalonPrewarm(context.Background(), p, supplier, factory)
	require.ErrorContains(t, err, "recorded construction")
	require.Equal(t, 1, calls)
}

func TestTalonPrewarmUsesDataCoordConfiguration(t *testing.T) {
	p := &paramtable.ComponentParam{}
	p.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	for key, value := range map[string]string{
		"common.storageType":               "remote",
		"common.storage.talon.coordinator": "coordinator:7000",
		"minio.cloudProvider":              "aws",
		"minio.bucketName":                 "bucket",
		"dataCoord.talonPrewarm.enabled":   "true",
		"dataCoord.talonPrewarm.interval":  "300",
		"dataCoord.talonPrewarm.batchSize": "256",
	} {
		require.NoError(t, p.Save(key, value))
	}
	factoryErr := merr.WrapErrServiceInternalMsg("Talon client unavailable")
	_, err := newTalonPrewarm(context.Background(), p,
		func(context.Context) ([]string, error) { return nil, nil },
		func(*indexpb.StorageConfig, int) (TalonPrewarmClient, error) {
			return nil, factoryErr
		})
	// Enabling the DC key must activate construction and preserve its failure.
	require.ErrorIs(t, err, factoryErr)
}

func TestTalonPrewarmDataCoordStartAfterHealthy(t *testing.T) {
	// Keep DC's state transition and prewarm loop real; suppress unrelated
	// schedulers, import recovery, service discovery and the process-level
	// server-info file.
	patchScheduler := mockey.Mock((*Server).startTaskScheduler).To(func(*Server) {}).Build()
	defer patchScheduler.UnPatch()
	patchImportReload := mockey.Mock((*importInspector).Reload).To(func(*importInspector) {}).Build()
	defer patchImportReload.UnPatch()
	patchLoops := mockey.Mock((*Server).startServerLoop).To(func(*Server) {}).Build()
	defer patchLoops.UnPatch()
	patchInfo := mockey.Mock(sessionutil.SaveServerInfo).To(func(string, int64) {}).Build()
	defer patchInfo.UnPatch()

	s := &Server{ctx: context.Background(), meta: newEmptyTestMeta(), importInspector: &importInspector{}}
	s.UpdateStateCode(commonpb.StateCode_Initializing)
	s.meta.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 10, State: commonpb.SegmentState_Flushed, ManifestPath: "unloaded/object",
	}))
	session := sessionutil.NewMockSession(t)
	session.EXPECT().GetServerID().Return(int64(1))
	s.session = session
	c := &recordingTalonPrewarm{stats: map[string]int{}, pending: make(chan struct{}), closeStarted: make(chan struct{})}
	s.talonPrewarmInspector = NewTalonPrewarmInspector(s.ctx, s.GetPublishedManifestPaths, c,
		func(s string) (string, error) { return s, nil }, time.Hour, 1)
	t.Cleanup(func() {
		s.talonPrewarmInspector.Stop()
		<-c.closeStarted
	})

	require.NoError(t, s.Start())
	select {
	case <-c.pending:
	case <-time.After(time.Second):
		t.Fatal("DC did not prewarm its metadata after becoming Healthy")
	}
	require.Equal(t, []TalonPrewarmRequest{{URI: "unloaded/object", Size: 15, Version: "etag:unloaded/object"}}, c.batches[0])
}

func TestTalonPrewarmDataCoordStopBeforeStart(t *testing.T) {
	s := &Server{ctx: context.Background()}
	s.UpdateStateCode(commonpb.StateCode_Initializing)
	c := &recordingTalonPrewarm{closeStarted: make(chan struct{})}
	s.talonPrewarmInspector = NewTalonPrewarmInspector(s.ctx, nil, c, nil, time.Hour, 1)
	t.Cleanup(func() {
		s.talonPrewarmInspector.Stop()
		<-c.closeStarted
	})
	// Init may create a client before another component fails to start.
	require.NoError(t, s.Stop())
	select {
	case <-c.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("DC Stop leaked the prewarm client before Start")
	}
}
