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

package resourcelimit

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type counterState struct {
	count     int64
	lastWrite time.Time
}

var counterStore = struct {
	mu       sync.RWMutex
	counters map[int64]counterState
}{
	counters: make(map[int64]counterState),
}

func Increase(ctx context.Context, collectionID int64) {
	if collectionID <= 0 {
		log.Ctx(ctx).Warn("skip increase resource limit counter due to invalid collectionID", zap.Int64("collectionID", collectionID))
		return
	}

	now := time.Now()
	window := paramtable.Get().QueryCoordCfg.ResourceLimitFastStopWindow.GetAsDuration(time.Second)

	counterStore.mu.Lock()
	defer counterStore.mu.Unlock()

	state, ok := counterStore.counters[collectionID]
	if ok && window > 0 && now.Sub(state.lastWrite) > window {
		state.count = 0
	}
	state.count++
	state.lastWrite = now
	counterStore.counters[collectionID] = state

	log.Ctx(ctx).Info("increased resource limit counter",
		zap.Int64("collectionID", collectionID),
		zap.Int64("counter", state.count),
	)
}

func ShouldFastStop(collectionID int64) bool {
	if collectionID <= 0 {
		return false
	}
	if !paramtable.Get().QueryCoordCfg.ResourceLimitFastStopEnable.GetAsBool() {
		return false
	}

	threshold := paramtable.Get().QueryCoordCfg.ResourceLimitFastStopThreshold.GetAsInt64()
	if threshold <= 0 {
		return false
	}

	return GetCounter(collectionID) >= threshold
}

func GetCounter(collectionID int64) int64 {
	counterStore.mu.RLock()
	state, ok := counterStore.counters[collectionID]
	counterStore.mu.RUnlock()
	if !ok {
		return 0
	}

	window := paramtable.Get().QueryCoordCfg.ResourceLimitFastStopWindow.GetAsDuration(time.Second)
	if window > 0 && time.Since(state.lastWrite) > window {
		return 0
	}

	return state.count
}

func Clear(collectionID int64) {
	if collectionID <= 0 {
		return
	}

	counterStore.mu.Lock()
	delete(counterStore.counters, collectionID)
	counterStore.mu.Unlock()
}
