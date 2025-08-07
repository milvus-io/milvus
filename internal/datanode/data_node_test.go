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

package datanode

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	util2 "github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(t *testing.M) {
	rand.Seed(time.Now().Unix())
	path := "/tmp/milvus_ut/rdb_data"
	os.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)

	paramtable.Init()
	// change to specific channel for test
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))

	code := t.Run()
	os.Exit(code)
}

func NewIDLEDataNodeMock(ctx context.Context, pkType schemapb.DataType) *DataNode {
	node := NewDataNode(ctx)
	node.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})

	syncMgr := syncmgr.NewSyncManager(nil)
	node.syncMgr = syncMgr

	return node
}

func TestDataNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := NewIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
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
	node.SetEtcdClient(etcdCli)
	err = node.Init()
	assert.NoError(t, err)
	err = node.Start()
	assert.NoError(t, err)
	assert.Empty(t, node.GetAddress())
	node.SetAddress("address")
	assert.Equal(t, "address", node.GetAddress())

	defer node.Stop()
	paramtable.SetNodeID(1)

	defer cancel()

	t.Run("Test getSystemInfoMetrics", func(t *testing.T) {
		emptyNode := &DataNode{}
		emptyNode.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := emptyNode.getSystemInfoMetrics(context.TODO(), req)
		assert.NoError(t, err)
		assert.NotEmpty(t, resp)
	})

	t.Run("Test getSystemInfoMetrics with quotaMetric error", func(t *testing.T) {
		emptyNode := &DataNode{}
		emptyNode.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		util2.DeregisterRateCollector(metricsinfo.InsertConsumeThroughput)
		resp, err := emptyNode.getSystemInfoMetrics(context.TODO(), req)
		assert.Error(t, err)
		assert.Empty(t, resp)
		util2.RegisterRateCollector(metricsinfo.InsertConsumeThroughput)
	})
}
