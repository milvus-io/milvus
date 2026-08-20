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

package segments

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type segmentLoadAdmissionWaiter struct {
	weight   uint64
	priority commonpb.LoadPriority
	ready    chan struct{}
	granted  bool
}

// segmentLoadAdmission limits the estimated peak memory of sealed segments
// loading concurrently. It is independent from caching layer reservations.
type segmentLoadAdmission struct {
	mu          sync.Mutex
	getCapacity func() uint64
	used        uint64
	waiters     []*segmentLoadAdmissionWaiter
}

func newSegmentLoadAdmission(capacity uint64) *segmentLoadAdmission {
	return newSegmentLoadAdmissionWithCapacity(func() uint64 { return capacity })
}

func newSegmentLoadAdmissionWithCapacity(getCapacity func() uint64) *segmentLoadAdmission {
	return &segmentLoadAdmission{getCapacity: getCapacity}
}

func newSegmentLoadAdmissionFromConfig() *segmentLoadAdmission {
	params := &paramtable.Get().QueryNodeCfg
	if !params.TieredEvictionEnabled.GetAsBool() {
		return nil
	}

	memoryCount := hardware.GetMemoryCount()
	return newSegmentLoadAdmissionWithCapacity(func() uint64 {
		return uint64(float64(memoryCount) * params.TieredMaxLoadingMemoryRatio.GetAsFloat())
	})
}

func (a *segmentLoadAdmission) acquire(
	ctx context.Context,
	weight uint64,
	priority commonpb.LoadPriority,
) (func(), error) {
	if weight == 0 {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	waiter := &segmentLoadAdmissionWaiter{
		weight:   weight,
		priority: priority,
		ready:    make(chan struct{}),
	}

	a.mu.Lock()
	a.enqueueLocked(waiter)
	a.grantWaitersLocked()
	a.mu.Unlock()

	select {
	case <-waiter.ready:
		return a.releaseFunc(waiter.weight), nil
	case <-ctx.Done():
		a.mu.Lock()
		if waiter.granted {
			a.used -= waiter.weight
		} else {
			a.removeWaiterLocked(waiter)
		}
		a.grantWaitersLocked()
		a.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (a *segmentLoadAdmission) run(
	ctx context.Context,
	weight uint64,
	priority commonpb.LoadPriority,
	load func() error,
) error {
	release, err := a.acquire(ctx, weight, priority)
	if err != nil {
		return err
	}
	defer release()
	return load()
}

func (a *segmentLoadAdmission) enqueueLocked(waiter *segmentLoadAdmissionWaiter) {
	insertAt := len(a.waiters)
	for i, queued := range a.waiters {
		if waiter.priority < queued.priority {
			insertAt = i
			break
		}
	}

	a.waiters = append(a.waiters, nil)
	copy(a.waiters[insertAt+1:], a.waiters[insertAt:])
	a.waiters[insertAt] = waiter
}

func (a *segmentLoadAdmission) grantWaitersLocked() {
	capacity := a.getCapacity()
	for len(a.waiters) > 0 {
		waiter := a.waiters[0]
		if !a.canGrantLocked(waiter.weight, capacity) {
			return
		}

		a.waiters = a.waiters[1:]
		a.used += waiter.weight
		waiter.granted = true
		close(waiter.ready)
	}
}

func (a *segmentLoadAdmission) canGrantLocked(weight, capacity uint64) bool {
	if a.used == 0 && weight > capacity {
		return true
	}
	if a.used > capacity {
		return false
	}
	return weight <= capacity-a.used
}

func (a *segmentLoadAdmission) removeWaiterLocked(waiter *segmentLoadAdmissionWaiter) {
	for i, queued := range a.waiters {
		if queued == waiter {
			a.waiters = append(a.waiters[:i], a.waiters[i+1:]...)
			return
		}
	}
}

func (a *segmentLoadAdmission) releaseFunc(weight uint64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.used -= weight
			a.grantWaitersLocked()
			a.mu.Unlock()
		})
	}
}
