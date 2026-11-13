// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package taskcost

import (
	"math"
	"sync"
)

// MemoryAdmission is the shared execution budget for index/stats/analyze and
// import V3. Waiting work stays in each scheduler's acceptance ledger; it must
// not reserve execution memory and prevent its own admission.
//
// externalUsage covers the other executors only. It must not read either
// participating scheduler: they call TryAcquire under their own locks.
// CPU and scalar slots never gate this budget.
type MemoryAdmission struct {
	mu            sync.Mutex
	used          int64
	capacity      func() int64
	externalUsage func() int64
}

// NewMemoryAdmission creates an execution budget, separate from queued bookings.
func NewMemoryAdmission(capacity, externalUsage func() int64) *MemoryAdmission {
	return &MemoryAdmission{capacity: capacity, externalUsage: externalUsage}
}

// Capacity returns the current non-negative node memory budget in bytes.
func (b *MemoryAdmission) Capacity() int64 {
	if b.capacity == nil {
		return 0
	}
	return max(b.capacity(), 0)
}

// TryAcquire atomically reserves a supplied memory price and returns an
// idempotent release. It retains the existing worker progress rule: an
// otherwise idle node can execute one oversized estimate alone. Coordinator
// placement remains strictly memory-bounded. Unknown capacity retains legacy
// admission, but still tracks reservations if capacity becomes known later.
func (b *MemoryAdmission) TryAcquire(memory int64) (func(), bool) {
	memory = max(memory, 0)
	b.mu.Lock()
	defer b.mu.Unlock()
	capacity := b.Capacity()
	var external int64
	if b.externalUsage != nil {
		external = max(b.externalUsage(), 0)
	}
	if capacity > 0 && (b.used > 0 || external > 0) {
		// Subtract capped terms instead of adding usages, which could wrap
		// for an oversized price and incorrectly offer memory again.
		remaining := max(capacity-min(b.used, capacity)-min(external, capacity), 0)
		if memory > remaining {
			return nil, false
		}
	}
	if memory > math.MaxInt64-b.used {
		return nil, false
	}
	b.used += memory
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.used -= memory
			b.mu.Unlock()
		})
	}, true
}
