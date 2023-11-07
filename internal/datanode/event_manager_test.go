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
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

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

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()
	broker.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	broker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	node.broker = broker

	node.timeTickSender = newTimeTickSender(node.broker, 0)

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
		// TODO there is not way to sync Release done, use sleep for now
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

func TestChannelEventManager(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		ran := false
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			ran = true
			ch <- struct{}{}
			return nil
		}, func(name string) {}, time.Millisecond*10)

		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info:      &datapb.ChannelWatchInfo{},
		})
		<-ch
		assert.True(t, ran)
	})

	t.Run("close behavior", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			return errors.New("mocked error")
		}, func(name string) {}, time.Millisecond*10)

		go func() {
			evt := event{
				eventType: putEventType,
				vChanName: "",
				version:   0,
				info:      &datapb.ChannelWatchInfo{},
			}
			em.handleEvent(evt)
			ch <- struct{}{}
		}()

		select {
		case <-ch:
		case <-time.After(time.Second):
			t.FailNow()
		}
		close(em.eventChan)

		assert.NotPanics(t, func() {
			em.Close()
			em.Close()
		})
	})

	t.Run("cancel by delete event", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		ran := false
		em := newChannelEventManager(
			func(info *datapb.ChannelWatchInfo, version int64) error {
				return errors.New("mocked error")
			},
			func(name string) {
				ran = true
				ch <- struct{}{}
			},
			time.Millisecond*10,
		)
		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info:      &datapb.ChannelWatchInfo{},
		})
		em.handleEvent(event{
			eventType: deleteEventType,
			vChanName: "",
			version:   0,
			info:      &datapb.ChannelWatchInfo{},
		})
		<-ch
		assert.True(t, ran)
	})

	t.Run("overwrite put event", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		ran := false
		em := newChannelEventManager(
			func(info *datapb.ChannelWatchInfo, version int64) error {
				if version > 0 {
					ran = true
					ch <- struct{}{}
					return nil
				}
				return errors.New("mocked error")
			},
			func(name string) {},
			time.Millisecond*10)
		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				State: datapb.ChannelWatchState_ToWatch,
			},
		})
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   1,
			info: &datapb.ChannelWatchInfo{
				State: datapb.ChannelWatchState_ToWatch,
			},
		})
		<-ch
		assert.True(t, ran)
	})
}

func parseWatchInfo(key string, data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	if err := proto.Unmarshal(data, &watchInfo); err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, key: %s, err: %v", key, err)
	}

	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo, key: %s", key)
	}
	reviseVChannelInfo(watchInfo.GetVchan())

	return &watchInfo, nil
}

func TestEventTickler(t *testing.T) {
	channelName := "test-channel"
	etcdPrefix := "test_path"

	kv, err := newTestEtcdKV()
	assert.NoError(t, err)
	kv.RemoveWithPrefix(etcdPrefix)
	defer kv.RemoveWithPrefix(etcdPrefix)

	tickler := newEtcdTickler(0, path.Join(etcdPrefix, channelName), &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			ChannelName: channelName,
		},
	}, kv, 100*time.Millisecond)
	defer tickler.stop()
	endCh := make(chan struct{}, 1)
	go func() {
		watchCh := kv.WatchWithPrefix(etcdPrefix)
		for {
			event, ok := <-watchCh
			assert.True(t, ok)
			for _, evt := range event.Events {
				key := string(evt.Kv.Key)
				watchInfo, err := parseWatchInfo(key, evt.Kv.Value)
				assert.NoError(t, err)
				if watchInfo.GetVchan().GetChannelName() == channelName {
					assert.Equal(t, int32(1), watchInfo.Progress)
					endCh <- struct{}{}
					return
				}
			}
		}
	}()

	tickler.inc()
	tickler.watch()
	assert.Eventually(t, func() bool {
		select {
		case <-endCh:
			return true
		default:
			return false
		}
	}, 4*time.Second, 100*time.Millisecond)
}
