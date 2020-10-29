// Copyright 2020 TiKV Project Authors.
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
	"go.etcd.io/etcd/clientv3"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"github.com/pingcap/log"
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
	GenerateTSO(count uint32) (internalpb.TimestampMsg, error)
	// Reset is used to reset the TSO allocator.
	Reset()
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalTSOAllocator struct {
	timestampOracle *timestampOracle
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(client   *clientv3.Client, rootPath string, saveInterval time.Duration, maxResetTSGap func() time.Duration) Allocator {
	return &GlobalTSOAllocator{
		timestampOracle: &timestampOracle{
			client:        client,
			rootPath:      rootPath,
			saveInterval:  saveInterval,
			maxResetTSGap: maxResetTSGap,
		},
	}
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize() error {
	return gta.timestampOracle.SyncTimestamp()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (gta *GlobalTSOAllocator) UpdateTSO() error {
	return gta.timestampOracle.UpdateTimestamp()
}

// SetTSO sets the physical part with given tso.
func (gta *GlobalTSOAllocator) SetTSO(tso uint64) error {
	return gta.timestampOracle.ResetUserTimestamp(tso)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (internalpb.TimestampMsg, error) {
	var resp internalpb.TimestampMsg

	if count == 0 {
		return resp, errors.New("tso count should be positive")
	}

	maxRetryCount := 10

	for i := 0; i < maxRetryCount; i++ {
		current := (*atomicObject)(atomic.LoadPointer(&gta.timestampOracle.TSO))
		if current == nil || current.physical == typeutil.ZeroTime {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			log.Info("sync hasn't completed yet, wait for a while")
			time.Sleep(200 * time.Millisecond)
			continue
		}

		resp.Physical = current.physical.UnixNano() / int64(time.Millisecond)
		resp.Logical = atomic.AddInt64(&current.logical, int64(count))
		if resp.Logical >= maxLogical {
			log.Error("logical part outside of max logical interval, please check ntp time",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i))
			time.Sleep(UpdateTimestampStep)
			continue
		}
		return resp, nil
	}
	return resp, errors.New("can not get timestamp")
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	gta.timestampOracle.ResetTimestamp()
}
