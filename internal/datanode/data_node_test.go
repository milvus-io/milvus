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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const returnError = "ReturnError"

type ctxKey struct{}

func TestMain(t *testing.M) {
	rand.Seed(time.Now().Unix())
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal("failed to start embed etcd server", zap.Error(err))
	}
	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Close()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)
	// setup env for etcd endpoint
	os.Setenv("etcd.endpoints", strings.Join(addrs, ","))

	path := "/tmp/milvus_ut/rdb_data"
	os.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)

	paramtable.Init()
	// change to specific channel for test
	paramtable.Get().Save(Params.EtcdCfg.Endpoints.Key, strings.Join(addrs, ","))
	paramtable.Get().Save(Params.CommonCfg.DataCoordTimeTick.Key, Params.CommonCfg.DataCoordTimeTick.GetValue()+strconv.Itoa(rand.Int()))

	rateCol, err = newRateCollector()
	if err != nil {
		panic("init test failed, err = " + err.Error())
	}

	code := t.Run()
	os.Exit(code)
}

func TestDataNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
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

	broker := &broker.MockBroker{}
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()

	node.broker = broker

	defer node.Stop()

	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath("/tmp/milvus_test/datanode"))
	paramtable.SetNodeID(1)

	defer cancel()
	t.Run("Test SetRootCoordClient", func(t *testing.T) {
		emptyDN := &DataNode{}
		tests := []struct {
			inrc        types.RootCoordClient
			isvalid     bool
			description string
		}{
			{nil, false, "nil input"},
			{&RootCoordFactory{}, true, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := emptyDN.SetRootCoordClient(test.inrc)
				if test.isvalid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("Test SetDataCoordClient", func(t *testing.T) {
		emptyDN := &DataNode{}
		tests := []struct {
			inrc        types.DataCoordClient
			isvalid     bool
			description string
		}{
			{nil, false, "nil input"},
			{&DataCoordFactory{}, true, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := emptyDN.SetDataCoordClient(test.inrc)
				if test.isvalid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("Test getSystemInfoMetrics", func(t *testing.T) {
		emptyNode := &DataNode{}
		emptyNode.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})
		emptyNode.flowgraphManager = newFlowgraphManager()

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := emptyNode.getSystemInfoMetrics(context.TODO(), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		log.Info("Test DataNode.getSystemInfoMetrics",
			zap.String("name", resp.ComponentName),
			zap.String("response", resp.Response))
	})

	t.Run("Test getSystemInfoMetrics with quotaMetric error", func(t *testing.T) {
		emptyNode := &DataNode{}
		emptyNode.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})
		emptyNode.flowgraphManager = newFlowgraphManager()

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		rateCol.Deregister(metricsinfo.InsertConsumeThroughput)
		resp, err := emptyNode.getSystemInfoMetrics(context.TODO(), req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		rateCol.Register(metricsinfo.InsertConsumeThroughput)
	})

	t.Run("Test BackGroundGC", func(t *testing.T) {
		vchanNameCh := make(chan string)
		node.clearSignal = vchanNameCh
		node.stopWaiter.Add(1)
		go node.BackGroundGC(vchanNameCh)

		testDataSyncs := []struct {
			dmChannelName string
		}{
			{"fake-by-dev-rootcoord-dml-backgroundgc-1"},
			{"fake-by-dev-rootcoord-dml-backgroundgc-2"},
		}

		for _, test := range testDataSyncs {
			err = node.flowgraphManager.AddandStartWithEtcdTickler(node, &datapb.VchannelInfo{
				CollectionID: 1, ChannelName: test.dmChannelName,
			}, &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
					},
					{
						FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
					},
					{
						FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
					},
					{
						FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: "128"},
						},
					},
				},
			}, genTestTickler())
			assert.NoError(t, err)
			vchanNameCh <- test.dmChannelName
		}

		assert.Eventually(t, func() bool {
			for _, test := range testDataSyncs {
				if node.flowgraphManager.HasFlowgraph(test.dmChannelName) {
					return false
				}
			}
			return true
		}, 2*time.Second, 10*time.Millisecond)
	})
}
