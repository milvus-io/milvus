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
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/pkg/errors"
)

const (
	// UpdateTimestampStep is used to update timestamp.
	UpdateTimestampStep = 50 * time.Millisecond
	// updateTimestampGuard is the min timestamp interval.
	updateTimestampGuard = time.Millisecond
	// maxLogical is the max upper limit for logical time.
	// When a TSO's logical time reaches this limit,
	// the physical time will be forced to increase.
	maxLogical = int64(1 << 18)
)

// atomicObject is used to store the current TSO in memory.
type atomicObject struct {
	physical time.Time
	logical  int64
}

// timestampOracle is used to maintain the logic of tso.
type timestampOracle struct {
	key   string
	txnKV kv.TxnKV

	// TODO: remove saveInterval
	saveInterval  time.Duration
	maxResetTSGap func() time.Duration
	// For tso, set after the PD becomes a leader.
	TSO           unsafe.Pointer
	lastSavedTime atomic.Value
}

func (t *timestampOracle) loadTimestamp() (time.Time, error) {
	strData, err := t.txnKV.Load(t.key)
	if err != nil {
		// intend to return nil
		return typeutil.ZeroTime, nil
	}

	var binData = []byte(strData)
	if len(binData) == 0 {
		return typeutil.ZeroTime, nil
	}
	return typeutil.ParseTimestamp(binData)
}

// save timestamp, if lastTs is 0, we think the timestamp doesn't exist, so create it,
// otherwise, update it.
func (t *timestampOracle) saveTimestamp(ts time.Time) error {
	data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
	err := t.txnKV.Save(t.key, string(data))
	if err != nil {
		return errors.WithStack(err)
	}
	t.lastSavedTime.Store(ts)
	return nil
}

func (t *timestampOracle) InitTimestamp() error {
	last, err := t.loadTimestamp()
	if err != nil {
		return err
	}
	next := time.Now()

	//If the current system time minus the saved etcd timestamp is less than `updateTimestampGuard`,
	//the timestamp allocation will start from the saved etcd timestamp temporarily.
	if typeutil.SubTimeByWallClock(next, last) < updateTimestampGuard {
		next = last.Add(updateTimestampGuard)
	}

	save := next.Add(t.saveInterval)
	if err := t.saveTimestamp(save); err != nil {
		return err
	}

	log.Print("sync and save timestamp", zap.Time("last", last), zap.Time("save", save), zap.Time("next", next))

	current := &atomicObject{
		physical: next,
	}
	// atomic unsafe pointer
	/* #nosec G103 */
	atomic.StorePointer(&t.TSO, unsafe.Pointer(current))

	return nil
}

// ResetUserTimestamp update the physical part with specified tso.
func (t *timestampOracle) ResetUserTimestamp(tso uint64) error {
	physical, _ := tsoutil.ParseTS(tso)
	next := physical.Add(time.Millisecond)
	prev := (*atomicObject)(atomic.LoadPointer(&t.TSO))

	// do not update
	if typeutil.SubTimeByWallClock(next, prev.physical) <= 3*updateTimestampGuard {
		return errors.New("the specified ts too small than now")
	}

	if typeutil.SubTimeByWallClock(next, prev.physical) >= t.maxResetTSGap() {
		return errors.New("the specified ts too large than now")
	}

	save := next.Add(t.saveInterval)
	if err := t.saveTimestamp(save); err != nil {
		return err
	}
	update := &atomicObject{
		physical: next,
	}
	// atomic unsafe pointer
	/* #nosec G103 */
	atomic.CompareAndSwapPointer(&t.TSO, unsafe.Pointer(prev), unsafe.Pointer(update))
	return nil
}

// UpdateTimestamp is used to update the timestamp.
// This function will do two things:
// 1. When the logical time is going to be used up, increase the current physical time.
// 2. When the time window is not big enough, which means the saved etcd time minus the next physical time
//    will be less than or equal to `updateTimestampGuard`, then the time window needs to be updated and
//    we also need to save the next physical time plus `TsoSaveInterval` into etcd.
//
// Here is some constraints that this function must satisfy:
// 1. The saved time is monotonically increasing.
// 2. The physical time is monotonically increasing.
// 3. The physical time is always less than the saved timestamp.
func (t *timestampOracle) UpdateTimestamp() error {
	prev := (*atomicObject)(atomic.LoadPointer(&t.TSO))
	now := time.Now()

	jetLag := typeutil.SubTimeByWallClock(now, prev.physical)
	if jetLag > 3*UpdateTimestampStep {
		log.Print("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prev.physical), zap.Time("now", now))
	}

	var next time.Time
	prevLogical := atomic.LoadInt64(&prev.logical)
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > updateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Print("the logical time may be not enough", zap.Int64("prev-logical", prevLogical))
		next = prev.physical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if typeutil.SubTimeByWallClock(t.lastSavedTime.Load().(time.Time), next) <= updateTimestampGuard {
		save := next.Add(t.saveInterval)
		if err := t.saveTimestamp(save); err != nil {
			return err
		}
	}

	current := &atomicObject{
		physical: next,
		logical:  0,
	}
	// atomic unsafe pointer
	/* #nosec G103 */
	atomic.StorePointer(&t.TSO, unsafe.Pointer(current))

	return nil
}

// ResetTimestamp is used to reset the timestamp.
func (t *timestampOracle) ResetTimestamp() {
	zero := &atomicObject{
		physical: time.Now(),
	}
	// atomic unsafe pointer
	/* #nosec G103 */
	atomic.StorePointer(&t.TSO, unsafe.Pointer(zero))
}
