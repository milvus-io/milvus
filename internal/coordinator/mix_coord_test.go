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

package coordinator

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
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
