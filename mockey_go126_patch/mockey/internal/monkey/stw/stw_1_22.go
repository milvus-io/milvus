//go:build go1.22 && !go1.23
// +build go1.22,!go1.23

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

package stw

import (
	_ "unsafe"
)

func doStopTheWorld() (resume func()) {
	w := stopTheWorld(stwForTestResetDebugLog)
	return func() { startTheWorld(w) }
}

const stwForTestResetDebugLog = 16

// stwReason is an enumeration of reasons the world is stopping.
type stwReason uint8

// worldStop provides context from the stop-the-world required by the
// start-the-world.
type worldStop struct {
	reason stwReason
	start  int64
}

//go:linkname stopTheWorld runtime.stopTheWorld
func stopTheWorld(reason stwReason) worldStop

//go:linkname startTheWorld runtime.startTheWorld
func startTheWorld(w worldStop)
