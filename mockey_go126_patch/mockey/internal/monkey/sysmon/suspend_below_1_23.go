//go:build !mockey_disable_ss && !go1.23
// +build !mockey_disable_ss,!go1.23

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

func init() {
	usleep = usleep0
	lock = lock0
	unlock = unlock0
}

//go:linkname usleep0 runtime.usleep
func usleep0(usec uint32)

//go:linkname lock0 runtime.lock
func lock0(unsafe.Pointer)

//go:linkname unlock0 runtime.unlock
func unlock0(unsafe.Pointer)
