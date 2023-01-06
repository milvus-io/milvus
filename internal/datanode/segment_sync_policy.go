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
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
)

// segmentSyncPolicy sync policy applies to segment
type segmentSyncPolicy func(segment *Segment, ts Timestamp) bool

// syncPeriodically get segmentSyncPolicy with segment sync periodically.
func syncPeriodically() segmentSyncPolicy {
	return func(segment *Segment, ts Timestamp) bool {
		endTime := tsoutil.PhysicalTime(ts)
		lastSyncTime := tsoutil.PhysicalTime(segment.lastSyncTs)
		shouldSync := endTime.Sub(lastSyncTime) >= Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second) && !segment.isBufferEmpty()
		if shouldSync {
			log.Info("sync segment periodically ", zap.Time("now", endTime), zap.Time("last sync", lastSyncTime))
		}
		return shouldSync
	}
}
