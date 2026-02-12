//go:build go1.23 && !go1.26
// +build go1.23,!go1.26

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
	"reflect"

	"github.com/bytedance/mockey/internal/monkey/fn"
	"github.com/bytedance/mockey/internal/monkey/linkname"
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
	reason           stwReason
	startedStopping  int64
	finishedStopping int64
	stoppingCPUTime  int64
}

var (
	stopTheWorld  func(reason stwReason) worldStop
	startTheWorld func(w worldStop)
)

func init() {
	stopTheWorldPC := linkname.FuncPCForName("runtime.stopTheWorld")
	stopTheWorld = fn.MakeFunc(reflect.TypeOf(stopTheWorld), stopTheWorldPC).Interface().(func(stwReason) worldStop)
	startTheWorldPC := linkname.FuncPCForName("runtime.startTheWorld")
	startTheWorld = fn.MakeFunc(reflect.TypeOf(startTheWorld), startTheWorldPC).Interface().(func(worldStop))
}
