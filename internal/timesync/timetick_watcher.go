// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package timesync

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	ms "github.com/milvus-io/milvus/internal/msgstream"
)

type TimeTickWatcher interface {
	Watch(msg *ms.TimeTickMsg)
	StartBackgroundLoop(ctx context.Context)
}

type MsgTimeTickWatcher struct {
	streams  []ms.MsgStream
	msgQueue chan *ms.TimeTickMsg
}

func NewMsgTimeTickWatcher(streams ...ms.MsgStream) *MsgTimeTickWatcher {
	watcher := &MsgTimeTickWatcher{
		streams:  streams,
		msgQueue: make(chan *ms.TimeTickMsg),
	}
	return watcher
}

func (watcher *MsgTimeTickWatcher) Watch(msg *ms.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *MsgTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("msg time tick watcher closed")
			return
		case msg := <-watcher.msgQueue:
			msgPack := &ms.MsgPack{}
			msgPack.Msgs = append(msgPack.Msgs, msg)
			for _, stream := range watcher.streams {
				if err := stream.Broadcast(msgPack); err != nil {
					log.Warn("stream broadcast failed", zap.Error(err))
				}
			}
		}
	}
}

func (watcher *MsgTimeTickWatcher) Close() {
}
