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
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestChannelEventManager(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		ran := false
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			ran = true
			ch <- struct{}{}
			return nil
		}, func(name string) bool { return true }, time.Millisecond*10)

		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
			},
		})
		<-ch
		assert.True(t, ran)
	})

	t.Run("event already timeout", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		ran := false
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			ran = true
			ch <- struct{}{}
			return nil
		}, func(name string) bool { return true }, time.Millisecond*10)

		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				TimeoutTs: time.Now().Add(-time.Minute).UnixNano(),
			},
		})
		select {
		case <-ch:
			t.FailNow()
		case <-time.NewTimer(time.Millisecond * 100).C:
		}
		assert.False(t, ran)
	})

	t.Run("retry success", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		ran := false
		counter := atomic.Int32{}
		counter.Store(0)
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			current := counter.Add(1)
			if current == 2 {
				ran = true
				ch <- struct{}{}
				return nil
			}

			return errors.New("mocked error")
		}, func(name string) bool { return true }, time.Millisecond*10)

		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
			},
		})
		<-ch
		assert.True(t, ran)
	})

	t.Run("retry until timeout", func(t *testing.T) {
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			return errors.New("mocked error")
		}, func(name string) bool { return true }, time.Millisecond*100)

		ch := make(chan struct{}, 1)

		go func() {
			ddl := time.Now().Add(time.Millisecond * 50)
			evt := event{
				eventType: putEventType,
				vChanName: "",
				version:   0,
				info: &datapb.ChannelWatchInfo{
					TimeoutTs: ddl.UnixNano(),
				},
			}
			em.retryHandlePutEvent(evt)
			ch <- struct{}{}
		}()

		select {
		case <-ch:
		case <-time.NewTimer(time.Second).C:
			t.FailNow()
		}
	})

	t.Run("close behavior", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		em := newChannelEventManager(func(info *datapb.ChannelWatchInfo, version int64) error {
			return errors.New("mocked error")
		}, func(name string) bool { return true }, time.Millisecond*10)

		go func() {
			ddl := time.Now().Add(time.Minute)
			evt := event{
				eventType: putEventType,
				vChanName: "",
				version:   0,
				info: &datapb.ChannelWatchInfo{
					TimeoutTs: ddl.UnixNano(),
				},
			}
			em.retryHandlePutEvent(evt)
			ch <- struct{}{}
		}()

		close(em.eventChan)
		select {
		case <-ch:
		case <-time.NewTimer(time.Second).C:
			t.FailNow()
		}

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
			func(name string) bool {
				ran = true
				ch <- struct{}{}
				return true
			},
			time.Millisecond*10,
		)
		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
			},
		})
		em.handleEvent(event{
			eventType: deleteEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
			},
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
			func(name string) bool {
				return false
			},
			time.Millisecond*10)
		em.Run()
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   0,
			info: &datapb.ChannelWatchInfo{
				State:     datapb.ChannelWatchState_ToWatch,
				TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
			},
		})
		em.handleEvent(event{
			eventType: putEventType,
			vChanName: "",
			version:   1,
			info: &datapb.ChannelWatchInfo{
				State:     datapb.ChannelWatchState_ToWatch,
				TimeoutTs: time.Now().Add(time.Minute).UnixNano(),
			},
		})
		<-ch
		assert.True(t, ran)
	})

	t.Run("canceled by EndStates", func(t *testing.T) {
		endStates := []datapb.ChannelWatchState{
			datapb.ChannelWatchState_Complete,
			datapb.ChannelWatchState_WatchSuccess,
			datapb.ChannelWatchState_WatchFailure,
			datapb.ChannelWatchState_ReleaseSuccess,
			datapb.ChannelWatchState_ReleaseFailure,
		}

		for _, es := range endStates {
			em := newChannelEventManager(
				func(info *datapb.ChannelWatchInfo, version int64) error {
					return errors.New("mocked error")
				},
				func(name string) bool {
					return false
				},
				time.Millisecond*100,
			)

			ch := make(chan struct{}, 1)
			ddl := time.Now().Add(time.Minute)

			go func() {
				evt := event{
					eventType: putEventType,
					vChanName: "",
					version:   0,
					info: &datapb.ChannelWatchInfo{
						TimeoutTs: ddl.UnixNano(),
					},
				}
				em.retryHandlePutEvent(evt)
				ch <- struct{}{}
			}()

			em.eventChan <- event{
				eventType: putEventType,
				vChanName: "",
				version:   0,
				info: &datapb.ChannelWatchInfo{
					State:     es,
					TimeoutTs: ddl.UnixNano(),
				},
			}
			select {
			case <-ch:
			case <-time.NewTimer(time.Minute).C:
				t.FailNow()
			}
		}
	})
}
