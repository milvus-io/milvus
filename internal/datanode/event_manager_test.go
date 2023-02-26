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
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

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

	tickler := newTickler(0, path.Join(etcdPrefix, channelName), &datapb.ChannelWatchInfo{
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
