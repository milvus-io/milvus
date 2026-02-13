//go:build !mockey_disable_ss
// +build !mockey_disable_ss

/*
 * Copyright 2022 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sysmon

import (
	"unsafe"
)

// SuspendSysmon Suspends the system monitor thread.
func SuspendSysmon() (resume func()) {
	offset := getSysmonLockOffset()
	if offset <= 0 {
		return func() {}
	}

	// Calculate the actual memory address of sysmon lock
	sysmonLockPtr := unsafe.Pointer(uintptr(unsafe.Pointer(&sched)) + offset)

	// Acquire sysmon lock to pause the system monitor thread
	lock(sysmonLockPtr)

	// Brief sleep to ensure sysmon is paused
	usleep(100)

	// Construct resume function
	resume = func() {
		unlock(sysmonLockPtr)
	}
	return
}

// getSysmonLockOffset Get the sysmon lock offset for the current Go version
func getSysmonLockOffset() uintptr {
	return sysmonLockOffset
}

//go:linkname sched runtime.sched
var sched struct{}

var usleep func(uint32)

var lock func(unsafe.Pointer)

var unlock func(unsafe.Pointer)
