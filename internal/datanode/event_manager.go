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
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"go.uber.org/zap"
)

const retryWatchInterval = 20 * time.Second

type event struct {
	eventType int
	vChanName string
	info      *datapb.ChannelWatchInfo
}

type channelEventManager struct {
	eventChan         chan event
	closeChan         chan struct{}
	handlePutEvent    func(watchInfo *datapb.ChannelWatchInfo) error // node.handlePutEvent
	handleDeleteEvent func(vChanName string)                         // node.handleDeleteEvent
}

const (
	putEventType    = 1
	deleteEventType = 2
)

func (e *channelEventManager) Run() {
	go func() {
		for {
			select {
			case event := <-e.eventChan:
				switch event.eventType {
				case putEventType:
					// Trigger retry for-loop when fail to handle put event for the first time
					if err := e.handlePutEvent(event.info); err != nil {
						for {
							log.Warn("handle put event fail, starting retry",
								zap.String("vChanName", event.vChanName),
								zap.String("retry interval", retryWatchInterval.String()),
								zap.Error(err))

							<-time.NewTimer(retryWatchInterval).C

							select {
							case e, ok := <-e.eventChan:
								// When getting a delete event at next retry, exit retry loop
								// When getting a put event, just continue the retry
								if ok && e.eventType == deleteEventType {
									log.Warn("delete event triggerred, terminating retry.",
										zap.String("vChanName", event.vChanName))
									return
								}
							default:
							}

							err = e.handlePutEvent(event.info)
							if err == nil {
								log.Info("retry to handle put event successfully",
									zap.String("vChanName", event.vChanName))
								return
							}
						}
					}
				case deleteEventType:
					e.handleDeleteEvent(event.vChanName)
				}
			case <-e.closeChan:
				return
			}
		}
	}()
}

func (e *channelEventManager) handleEvent(event event) {
	e.eventChan <- event
}

func (e *channelEventManager) Close() {
	close(e.closeChan)
}
