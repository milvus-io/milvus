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

package task

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type nodeSegmentIndex struct {
	nodeID    int64
	segmentID int64
	scope     querypb.DataScope
}

type nodeChannelIndex struct {
	nodeID  int64
	channel string
}

// All touches both physical copies. Historical and Streaming may otherwise run
// independently, including while a growing segment is being sealed.
func segmentScopes(scope querypb.DataScope) []querypb.DataScope {
	if scope == querypb.DataScope_All {
		return []querypb.DataScope{querypb.DataScope_Streaming, querypb.DataScope_Historical}
	}
	return []querypb.DataScope{scope}
}

func taskResourceKeys(task Task) ([]nodeSegmentIndex, []nodeChannelIndex) {
	var segments []nodeSegmentIndex
	var channels []nodeChannelIndex
	switch task.(type) {
	case *SegmentTask:
		for _, action := range task.Actions() {
			a := action.(*SegmentAction)
			for _, scope := range segmentScopes(a.GetScope()) {
				segments = append(segments, nodeSegmentIndex{a.Node(), a.SegmentID, scope})
			}
		}
	case *ChannelTask:
		for _, action := range task.Actions() {
			a := action.(*ChannelAction)
			channels = append(channels, nodeChannelIndex{a.Node(), a.ChannelName()})
		}
	}
	return segments, channels
}

// Registration holds the collection lock. Segment IDs and channel names
// identify one collection, so check-all then reserve-all is atomic for
// competing tasks; the maps themselves allow unrelated collections to proceed.
// Reservations are released when the task's last ownership reference drops:
// at retirement, or later when an in-flight executor callback returns. That
// release can run without the collection lock because it only removes entries
// owned by this task, which at worst makes a concurrent check fail early.
func (scheduler *taskScheduler) checkTaskResources(task, replacement Task) error {
	segments, channels := taskResourceKeys(task)
	for _, key := range segments {
		if owner, ok := scheduler.segmentOwners.Get(key); ok && owner != replacement {
			return merr.WrapErrServiceInternalMsg("task %d for the same segment on this node exists", owner.ID())
		}
	}
	for _, key := range channels {
		if owner, ok := scheduler.channelOwners.Get(key); ok && owner != replacement {
			return merr.WrapErrServiceInternalMsg("task %d for the same channel on this node exists", owner.ID())
		}
	}
	return nil
}

func (scheduler *taskScheduler) reserveTaskResources(task Task) error {
	segments, channels := taskResourceKeys(task)
	for _, key := range segments {
		if owner, loaded := scheduler.segmentOwners.GetOrInsert(key, task); loaded && owner != task {
			scheduler.releaseTaskResources(task)
			return merr.WrapErrServiceInternalMsg("task %d for the same segment on this node exists", owner.ID())
		}
	}
	for _, key := range channels {
		if owner, loaded := scheduler.channelOwners.GetOrInsert(key, task); loaded && owner != task {
			scheduler.releaseTaskResources(task)
			return merr.WrapErrServiceInternalMsg("task %d for the same channel on this node exists", owner.ID())
		}
	}
	return nil
}

func (scheduler *taskScheduler) releaseTaskResources(task Task) {
	segments, channels := taskResourceKeys(task)
	for _, key := range segments {
		if owner, ok := scheduler.segmentOwners.Get(key); ok && owner == task {
			scheduler.segmentOwners.Remove(key)
		}
	}
	for _, key := range channels {
		if owner, ok := scheduler.channelOwners.Get(key); ok && owner == task {
			scheduler.channelOwners.Remove(key)
		}
	}
}
