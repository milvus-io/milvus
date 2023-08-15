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

package server

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type produceState struct {
	endMsgID int64
	written  bool
	ready    chan struct{}
}

// rocksmqSyncer is to make message write concurrently but updates the page info sequentially
type rocksmqSyncer struct {
	msgQueueMu    sync.RWMutex
	msgQueue      []*produceState
	maxReadableID int64 // the end id of the last readable msg, should use atomic load/store
}

func (s *rocksmqSyncer) push(endMsgID int64) *produceState {
	s.msgQueueMu.Lock()
	defer s.msgQueueMu.Unlock()
	state := &produceState{
		endMsgID: endMsgID,
		written:  false,
		ready:    make(chan struct{}),
	}
	s.msgQueue = append(s.msgQueue, state)
	return state
}

func (s *rocksmqSyncer) finishWrite(endMsgID int64) error {
	s.msgQueueMu.RLock()
	defer s.msgQueueMu.RUnlock()
	for i, state := range s.msgQueue {
		if state.endMsgID != endMsgID {
			continue
		}

		state.written = true
		if i == 0 {
			close(state.ready)
		}
		return nil
	}

	return fmt.Errorf("state %d not found", endMsgID)
}

func (s *rocksmqSyncer) pop() {
	s.msgQueueMu.Lock()
	defer s.msgQueueMu.Unlock()
	atomic.StoreInt64(&s.maxReadableID, s.msgQueue[0].endMsgID-1)
	s.msgQueue = s.msgQueue[1:]
	if len(s.msgQueue) != 0 && s.msgQueue[0].written {
		close(s.msgQueue[0].ready)
	}
}

func (s *rocksmqSyncer) remove(endMsgID int64) {
	s.msgQueueMu.Lock()
	defer s.msgQueueMu.Unlock()

	var target = -1
	for i, state := range s.msgQueue {
		if state.endMsgID == endMsgID {
			target = i
			break
		}
	}

	if target == -1 {
		return
	}

	s.msgQueue = append(s.msgQueue[:target], s.msgQueue[target+1:]...)

	// if the first msg is removed, check current first msg if written
	if target == 0 && len(s.msgQueue) != 0 && s.msgQueue[0].written {
		close(s.msgQueue[0].ready)
	}
}

func (s *rocksmqSyncer) getMaxReadableID() int64 {
	return atomic.LoadInt64(&s.maxReadableID)
}

func newRocksMqSyncer(topicName string, latestMsgID int64) *rocksmqSyncer {
	syncer := &rocksmqSyncer{
		msgQueue: make([]*produceState, 0),
	}
	syncer.maxReadableID = latestMsgID
	return syncer
}
