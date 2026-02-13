//go:build race
// +build race

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

package inst

import (
	"reflect"
	_ "unsafe"

	"github.com/bytedance/mockey/internal/tool"
)

//go:linkname racefuncenter runtime.racefuncenter
func racefuncenter(callpc uintptr)

// not implemented & never used
////go:linkname racefuncenterfp runtime.racefuncenterfp
// func racefuncenterfp(fp uintptr)

//go:linkname racefuncexit runtime.racefuncexit
func racefuncexit()

//go:linkname raceread runtime.raceread
func raceread(addr uintptr)

//go:linkname racewrite runtime.racewrite
func racewrite(addr uintptr)

//go:linkname racereadrange runtime.racereadrange
func racereadrange(addr, size uintptr)

//go:linkname racewriterange runtime.racewriterange
func racewriterange(addr, size uintptr)

//go:linkname racereadrangepc1 runtime.racereadrangepc1
func racereadrangepc1(addr, size, pc uintptr)

//go:linkname racewriterangepc1 runtime.racewriterangepc1
func racewriterangepc1(addr, size, pc uintptr)

//go:linkname racecallbackthunk runtime.racecallbackthunk
func racecallbackthunk(uintptr)

func init() {
	proxyCallRace = map[uintptr]string{
		reflect.ValueOf(racefuncenter).Pointer(): "racefuncenter",
		// reflect.ValueOf(racefuncenterfp).Pointer():   "racefuncenterfp", // not implemented & never used
		reflect.ValueOf(racefuncexit).Pointer():      "racefuncexit",
		reflect.ValueOf(raceread).Pointer():          "raceread",
		reflect.ValueOf(racewrite).Pointer():         "racewrite",
		reflect.ValueOf(racereadrange).Pointer():     "racereadrange",
		reflect.ValueOf(racewriterange).Pointer():    "racewriterange",
		reflect.ValueOf(racereadrangepc1).Pointer():  "racereadrangepc1",
		reflect.ValueOf(racewriterangepc1).Pointer(): "racewriterangepc1",
		reflect.ValueOf(racecallbackthunk).Pointer(): "racecallbackthunk",
	}

	for addr, name := range proxyCallRace {
		tool.DebugPrintf("race func: %x(%v)\n", addr, name)
	}
}
