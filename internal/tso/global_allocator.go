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

// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"log"
	"sync/atomic"
	"time"

	"errors"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// Allocator is a Timestamp Oracle allocator.
type Allocator interface {
	// Initialize is used to initialize a TSO allocator.
	// It will synchronize TSO with etcd and initialize the
	// memory for later allocation work.
	Initialize() error
	// UpdateTSO is used to update the TSO in memory and the time window in etcd.
	UpdateTSO() error
	// SetTSO sets the physical part with given tso. It's mainly used for BR restore
	// and can not forcibly set the TSO smaller than now.
	SetTSO(tso uint64) error
	// GenerateTSO is used to generate a given number of TSOs.
	// Make sure you have initialized the TSO allocator before calling.
	GenerateTSO(count uint32) (uint64, error)
	// Reset is used to reset the TSO allocator.
	Reset()
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalTSOAllocator struct {
	tso           *timestampOracle
	LimitMaxLogic bool
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(key string, txnKV kv.TxnKV) *GlobalTSOAllocator {
	return &GlobalTSOAllocator{
		tso: &timestampOracle{
			txnKV:         txnKV,
			saveInterval:  3 * time.Second,
			maxResetTSGap: func() time.Duration { return 3 * time.Second },
			key:           key,
		},
		LimitMaxLogic: true,
	}
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize() error {
	return gta.tso.InitTimestamp()
}

// SetLimitMaxLogic is to enable or disable the maximum limit on the logical part of the hybrid timestamp.
// When enabled, if the logical part of the hybrid timestamp exceeds the maximum limit,
// GlobalTSOAllocator will sleep for a period and try to allocate the timestamp again.
func (gta *GlobalTSOAllocator) SetLimitMaxLogic(flag bool) {
	gta.LimitMaxLogic = flag
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (gta *GlobalTSOAllocator) UpdateTSO() error {
	return gta.tso.UpdateTimestamp()
}

// SetTSO sets the physical part with given tso.
func (gta *GlobalTSOAllocator) SetTSO(tso uint64) error {
	return gta.tso.ResetUserTimestamp(tso)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (uint64, error) {
	var physical, logical int64
	if count == 0 {
		return 0, errors.New("tso count should be positive")
	}

	maxRetryCount := 10

	for i := 0; i < maxRetryCount; i++ {
		current := (*atomicObject)(atomic.LoadPointer(&gta.tso.TSO))
		if current == nil || current.physical.Equal(typeutil.ZeroTime) {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			log.Println("sync hasn't completed yet, wait for a while")
			time.Sleep(200 * time.Millisecond)
			continue
		}

		physical = current.physical.UnixNano() / int64(time.Millisecond)
		logical = atomic.AddInt64(&current.logical, int64(count))
		if logical >= maxLogical && gta.LimitMaxLogic {
			log.Println("logical part outside of max logical interval, please check ntp time",
				zap.Int("retry-count", i))
			time.Sleep(UpdateTimestampStep)
			continue
		}
		return tsoutil.ComposeTS(physical, logical), nil
	}
	return 0, errors.New("can not get timestamp")
}

// Alloc allocates a batch of timestamps. What is returned is the starting timestamp.
func (gta *GlobalTSOAllocator) Alloc(count uint32) (typeutil.Timestamp, error) {
	//return gta.tso.SyncTimestamp()
	start, err := gta.GenerateTSO(count)
	if err != nil {
		return typeutil.ZeroTimestamp, err
	}
	//ret := make([]typeutil.Timestamp, count)
	//for i:=uint32(0); i < count; i++{
	//	ret[i] = start + uint64(i)
	//}
	return start, err
}

// AllocOne only allocates one timestamp.
func (gta *GlobalTSOAllocator) AllocOne() (typeutil.Timestamp, error) {
	return gta.GenerateTSO(1)
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	gta.tso.ResetTimestamp()
}
