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

package json

import (
	"sync"
	"sync/atomic"
)

// sonic JIT-compiles a dedicated decoder/encoder for every type on first use.
// The compilation appends a synthetic runtime.moduledata whose pluginpath is
// empty. If that append overlaps with plugin.Open's lastmoduleinit section, the
// Go runtime may treat sonic's synthetic module as the newly loaded plugin and
// terminate the process with "runtime: plugin has empty pluginpath".
//
// To prevent the race, every sonic entry point takes the reader side of this
// gate, while plugin.Open is executed inside the exclusive (writer) side.
//
// The reader fast path is lock-free: a single atomic increment plus a recheck of
// writeHeld. It only falls back to the mutex-protected slow path when a plugin
// load is actually in progress (a one-shot startup event), so the JSON hot path
// never touches a contended mutex.
//
// The reader side is implemented as a counter instead of sync.RWMutex so that a
// reader can safely re-enter (e.g. a custom MarshalJSON that calls json.Marshal
// again) without deadlocking against a pending writer.
var (
	// gateMu serializes the writer side and the slow (blocking) reader path.
	gateMu sync.Mutex
	// gateCond notifies waiters when the writer side is released or readers drain.
	gateCond = sync.NewCond(&gateMu)
	// writeHeld reports whether plugin loading (the exclusive side) is active.
	writeHeld atomic.Bool
	// reading counts in-flight sonic calls.
	reading atomic.Int64
	// gateDisabled is set once every plugin load in the process has finished.
	// After that plugin.Open never runs again, so sonic JIT registration can no
	// longer race with it and the reader fast path costs nothing.
	gateDisabled atomic.Bool
)

func acquireRead() {
	// Steady state: the gate is disabled after all plugin loading finished and
	// costs nothing beyond a single atomic load.
	if gateDisabled.Load() {
		return
	}
	// Fast path: no plugin load active, a single atomic increment suffices.
	if !writeHeld.Load() {
		reading.Add(1)
		if !writeHeld.Load() {
			return
		}
		// A plugin load started right after our check; undo and take the slow path.
		releaseRead()
	}
	// Slow path: wait until the exclusive side is released.
	gateMu.Lock()
	for writeHeld.Load() {
		gateCond.Wait()
	}
	reading.Add(1)
	gateMu.Unlock()
}

func releaseRead() {
	if reading.Add(-1) == 0 && writeHeld.Load() {
		// The writer may be waiting for the last in-flight reader to finish.
		gateMu.Lock()
		gateCond.Broadcast()
		gateMu.Unlock()
	}
}

// BlockForPluginLoad acquires the exclusive side of the gate. It returns once
// no sonic call is in flight and blocks any new sonic call from starting until
// UnblockForPluginLoad is called. plugin.Open must be wrapped with it so that
// sonic JIT module registration never runs concurrently with plugin loading.
func BlockForPluginLoad() {
	// Re-arm the gate in case it was disabled; a plugin load always needs the
	// reader side to block again.
	gateDisabled.Store(false)
	gateMu.Lock()
	writeHeld.Store(true)
	for reading.Load() > 0 {
		gateCond.Wait()
	}
	gateMu.Unlock()
}

// UnblockForPluginLoad releases the exclusive side of the gate acquired by
// BlockForPluginLoad.
func UnblockForPluginLoad() {
	gateMu.Lock()
	writeHeld.Store(false)
	gateCond.Broadcast()
	gateMu.Unlock()
}

// DisableGate permanently disables the reader side of the gate. It must only be
// called once every plugin loading in the process has finished; after that
// point plugin.Open never runs again, so sonic JIT registration can no longer
// race with it, and the reader fast path costs nothing beyond a single atomic
// load. A later BlockForPluginLoad re-arms the gate.
func DisableGate() {
	gateDisabled.Store(true)
}
