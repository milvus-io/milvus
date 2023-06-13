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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
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
		log.Fatal(err.Error())
	}
	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Close()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)
	// setup env for etcd endpoint
	os.Setenv("etcd.endpoints", strings.Join(addrs, ","))

	path := "/tmp/milvus_ut/rdb_data"
	os.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)

	Params.Init()
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
	importutil.ReportImportAttempts = 1

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
	defer node.Stop()

	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath("/tmp/milvus_test/datanode"))
	paramtable.SetNodeID(1)

	defer cancel()
	t.Run("Test SetRootCoord", func(t *testing.T) {
		emptyDN := &DataNode{}
		tests := []struct {
			inrc        types.RootCoord
			isvalid     bool
			description string
		}{
			{nil, false, "nil input"},
			{&RootCoordFactory{}, true, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := emptyDN.SetRootCoord(test.inrc)
				if test.isvalid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("Test SetDataCoord", func(t *testing.T) {
		emptyDN := &DataNode{}
		tests := []struct {
			inrc        types.DataCoord
			isvalid     bool
			description string
		}{
			{nil, false, "nil input"},
			{&DataCoordFactory{}, true, "valid input"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := emptyDN.SetDataCoord(test.inrc)
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
		emptyNode.SetSession(&sessionutil.Session{ServerID: 1})
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
		emptyNode.SetSession(&sessionutil.Session{ServerID: 1})
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
		node.wg.Add(1)
		go node.BackGroundGC(vchanNameCh)

		testDataSyncs := []struct {
			dmChannelName string
		}{
			{"fake-by-dev-rootcoord-dml-backgroundgc-1"},
			{"fake-by-dev-rootcoord-dml-backgroundgc-2"},
		}

		for _, test := range testDataSyncs {
			err = node.flowgraphManager.addAndStart(node, &datapb.VchannelInfo{CollectionID: 1, ChannelName: test.dmChannelName}, nil, genTestTickler())
			assert.NoError(t, err)
			vchanNameCh <- test.dmChannelName
		}

		assert.Eventually(t, func() bool {
			for _, test := range testDataSyncs {
				if node.flowgraphManager.exist(test.dmChannelName) {
					return false
				}
			}
			return true
		}, 2*time.Second, 10*time.Millisecond)
	})

}

func TestWatchChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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
	defer node.Stop()
	err = node.Register()
	assert.NoError(t, err)

	defer cancel()

	t.Run("test watch channel", func(t *testing.T) {
		kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath.GetValue())
		oldInvalidCh := "datanode-etcd-test-by-dev-rootcoord-dml-channel-invalid"
		path := fmt.Sprintf("%s/%d/%s", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID(), oldInvalidCh)
		err = kv.Save(path, string([]byte{23}))
		assert.NoError(t, err)

		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		path = fmt.Sprintf("%s/%d/%s", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID(), ch)

		vchan := &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         ch,
			UnflushedSegmentIds: []int64{},
		}
		info := &datapb.ChannelWatchInfo{
			State: datapb.ChannelWatchState_ToWatch,
			Vchan: vchan,
		}
		val, err := proto.Marshal(info)
		assert.NoError(t, err)
		err = kv.Save(path, string(val))
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			exist := node.flowgraphManager.exist(ch)
			if !exist {
				return false
			}
			bs, err := kv.LoadBytes(fmt.Sprintf("%s/%d/%s", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID(), ch))
			if err != nil {
				return false
			}
			watchInfo := &datapb.ChannelWatchInfo{}
			err = proto.Unmarshal(bs, watchInfo)
			if err != nil {
				return false
			}
			return watchInfo.GetState() == datapb.ChannelWatchState_WatchSuccess
		}, 3*time.Second, 100*time.Millisecond)

		err = kv.RemoveWithPrefix(fmt.Sprintf("%s/%d", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID()))
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			exist := node.flowgraphManager.exist(ch)
			return !exist
		}, 3*time.Second, 100*time.Millisecond)
	})

	t.Run("Test release channel", func(t *testing.T) {
		kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath.GetValue())
		oldInvalidCh := "datanode-etcd-test-by-dev-rootcoord-dml-channel-invalid"
		path := fmt.Sprintf("%s/%d/%s", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID(), oldInvalidCh)
		err = kv.Save(path, string([]byte{23}))
		assert.NoError(t, err)

		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		path = fmt.Sprintf("%s/%d/%s", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID(), ch)
		c := make(chan struct{})
		go func() {
			ec := kv.WatchWithPrefix(fmt.Sprintf("%s/%d", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID()))
			c <- struct{}{}
			cnt := 0
			for {
				evt := <-ec
				for _, event := range evt.Events {
					if strings.Contains(string(event.Kv.Key), ch) {
						cnt++
					}
				}
				if cnt >= 2 {
					break
				}
			}
			c <- struct{}{}
		}()
		// wait for check goroutine start Watch
		<-c

		vchan := &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         ch,
			UnflushedSegmentIds: []int64{},
		}
		info := &datapb.ChannelWatchInfo{
			State: datapb.ChannelWatchState_ToRelease,
			Vchan: vchan,
		}
		val, err := proto.Marshal(info)
		assert.NoError(t, err)
		err = kv.Save(path, string(val))
		assert.NoError(t, err)

		// wait for check goroutine received 2 events
		<-c
		exist := node.flowgraphManager.exist(ch)
		assert.False(t, exist)

		err = kv.RemoveWithPrefix(fmt.Sprintf("%s/%d", Params.CommonCfg.DataCoordWatchSubPath.GetValue(), paramtable.GetNodeID()))
		assert.NoError(t, err)
		//TODO there is not way to sync Release done, use sleep for now
		time.Sleep(100 * time.Millisecond)

		exist = node.flowgraphManager.exist(ch)
		assert.False(t, exist)

	})

	t.Run("handle watch info failed", func(t *testing.T) {
		e := &event{
			eventType: putEventType,
		}

		node.handleWatchInfo(e, "test1", []byte{23})

		exist := node.flowgraphManager.exist("test1")
		assert.False(t, exist)

		info := datapb.ChannelWatchInfo{
			Vchan: nil,
			State: datapb.ChannelWatchState_Uncomplete,
		}
		bs, err := proto.Marshal(&info)
		assert.NoError(t, err)
		node.handleWatchInfo(e, "test2", bs)

		exist = node.flowgraphManager.exist("test2")
		assert.False(t, exist)

		chPut := make(chan struct{}, 1)
		chDel := make(chan struct{}, 1)

		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		m := newChannelEventManager(
			func(info *datapb.ChannelWatchInfo, version int64) error {
				r := node.handlePutEvent(info, version)
				chPut <- struct{}{}
				return r
			},
			func(vChan string) {
				node.handleDeleteEvent(vChan)
				chDel <- struct{}{}
			}, time.Millisecond*100,
		)
		node.eventManagerMap.Insert(ch, m)
		m.Run()
		defer m.Close()

		info = datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{ChannelName: ch},
			State: datapb.ChannelWatchState_Uncomplete,
		}
		bs, err = proto.Marshal(&info)
		assert.NoError(t, err)

		msFactory := node.factory
		defer func() { node.factory = msFactory }()

		// todo review the UT logic
		// As we remove timetick channel logic, flow_graph_insert_buffer_node no longer depend on MessageStreamFactory
		// so data_sync_service can be created. this assert becomes true
		node.factory = &FailMessageStreamFactory{}
		node.handleWatchInfo(e, ch, bs)
		<-chPut
		exist = node.flowgraphManager.exist(ch)
		assert.True(t, exist)
	})

	t.Run("handle watchinfo out of date", func(t *testing.T) {
		chPut := make(chan struct{}, 1)
		chDel := make(chan struct{}, 1)
		// inject eventManager
		ch := fmt.Sprintf("datanode-etcd-test-by-dev-rootcoord-dml-channel_%d", rand.Int31())
		m := newChannelEventManager(
			func(info *datapb.ChannelWatchInfo, version int64) error {
				r := node.handlePutEvent(info, version)
				chPut <- struct{}{}
				return r
			},
			func(vChan string) {
				node.handleDeleteEvent(vChan)
				chDel <- struct{}{}
			}, time.Millisecond*100,
		)
		node.eventManagerMap.Insert(ch, m)
		m.Run()
		defer m.Close()
		e := &event{
			eventType: putEventType,
			version:   10000,
		}

		info := datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{ChannelName: ch},
			State: datapb.ChannelWatchState_Uncomplete,
		}
		bs, err := proto.Marshal(&info)
		assert.NoError(t, err)

		node.handleWatchInfo(e, ch, bs)
		<-chPut
		exist := node.flowgraphManager.exist("test3")
		assert.False(t, exist)
	})

	t.Run("handle watchinfo compatibility", func(t *testing.T) {
		info := datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID:        1,
				ChannelName:         "delta-channel1",
				UnflushedSegments:   []*datapb.SegmentInfo{{ID: 1}},
				FlushedSegments:     []*datapb.SegmentInfo{{ID: 2}},
				DroppedSegments:     []*datapb.SegmentInfo{{ID: 3}},
				UnflushedSegmentIds: []int64{1},
			},
			State: datapb.ChannelWatchState_Uncomplete,
		}
		bs, err := proto.Marshal(&info)
		assert.NoError(t, err)

		newWatchInfo, err := parsePutEventData(bs)
		assert.NoError(t, err)

		assert.Equal(t, []*datapb.SegmentInfo{}, newWatchInfo.GetVchan().GetUnflushedSegments())
		assert.Equal(t, []*datapb.SegmentInfo{}, newWatchInfo.GetVchan().GetFlushedSegments())
		assert.Equal(t, []*datapb.SegmentInfo{}, newWatchInfo.GetVchan().GetDroppedSegments())
		assert.NotEmpty(t, newWatchInfo.GetVchan().GetUnflushedSegmentIds())
		assert.NotEmpty(t, newWatchInfo.GetVchan().GetFlushedSegmentIds())
		assert.NotEmpty(t, newWatchInfo.GetVchan().GetDroppedSegmentIds())
	})
}
