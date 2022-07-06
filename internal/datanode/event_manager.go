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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"go.uber.org/zap"
)

const retryWatchInterval = 20 * time.Second

type event struct {
	eventType int
	vChanName string
	version   int64
	info      *datapb.ChannelWatchInfo
}

type channelEventManager struct {
	sync.Once
	eventChan         chan event
	closeChan         chan struct{}
	handlePutEvent    func(watchInfo *datapb.ChannelWatchInfo, version int64) error // node.handlePutEvent
	handleDeleteEvent func(vChanName string)                                        // node.handleDeleteEvent
	retryInterval     time.Duration
}

const (
	putEventType    = 1
	deleteEventType = 2
)

func newChannelEventManager(handlePut func(*datapb.ChannelWatchInfo, int64) error,
	handleDel func(string), retryInterval time.Duration) *channelEventManager {
	return &channelEventManager{
		eventChan:         make(chan event, 10),
		closeChan:         make(chan struct{}),
		handlePutEvent:    handlePut,
		handleDeleteEvent: handleDel,
		retryInterval:     retryInterval,
	}
}

func (e *channelEventManager) Run() {
	go func() {
		for {
			select {
			case event := <-e.eventChan:
				switch event.eventType {
				case putEventType:
					e.retryHandlePutEvent(event)
				case deleteEventType:
					e.handleDeleteEvent(event.vChanName)
				}
			case <-e.closeChan:
				return
			}
		}
	}()
}

func (e *channelEventManager) retryHandlePutEvent(event event) {
	countdown := time.Until(time.Unix(0, event.info.TimeoutTs))
	if countdown < 0 {
		log.Warn("event already timed out", zap.String("vChanName", event.vChanName))
		return
	}
	// Trigger retry for-loop when fail to handle put event for the first time
	if err := e.handlePutEvent(event.info, event.version); err != nil {
		timer := time.NewTimer(countdown)
		defer timer.Stop()
		ticker := time.NewTicker(e.retryInterval)
		defer ticker.Stop()
		for {
			log.Warn("handle put event fail, starting retry",
				zap.String("vChanName", event.vChanName),
				zap.String("retry interval", e.retryInterval.String()),
				zap.Error(err))

			// reset the ticker
			ticker.Reset(e.retryInterval)

			select {
			case <-ticker.C:
				// ticker notify, do another retry
			case <-timer.C:
				// timeout
				log.Warn("event process timed out", zap.String("vChanName", event.vChanName))
				return
			case evt, ok := <-e.eventChan:
				if !ok {
					log.Warn("event channel closed", zap.String("vChanName", event.vChanName))
					return
				}
				// When got another put event, overwrite current event
				if evt.eventType == putEventType {
					// handles only Uncomplete, ToWatch and ToRelease
					if isEndWatchState(evt.info.State) {
						return
					}
					event = evt
				}
				// When getting a delete event at next retry, exit retry loop
				// When getting a put event, just continue the retry
				if evt.eventType == deleteEventType {
					log.Warn("delete event triggerred, terminating retry.",
						zap.String("vChanName", event.vChanName))
					e.handleDeleteEvent(evt.vChanName)
					return
				}
			}

			err = e.handlePutEvent(event.info, event.version)
			if err != nil {
				log.Warn("failed to handle put event", zap.String("vChanName", event.vChanName), zap.Error(err))
				// no need to retry here,
			} else {
				log.Info("handle put event successfully", zap.String("vChanName", event.vChanName))
				return
			}
		}
	}
}

func (e *channelEventManager) handleEvent(event event) {
	e.eventChan <- event
}

func (e *channelEventManager) Close() {
	e.Do(func() {
		close(e.closeChan)
	})
}

func isEndWatchState(state datapb.ChannelWatchState) bool {
	return state != datapb.ChannelWatchState_ToWatch && // start watch
		state != datapb.ChannelWatchState_ToRelease && // start release
		state != datapb.ChannelWatchState_Uncomplete // legacy state, equal to ToWatch
}
