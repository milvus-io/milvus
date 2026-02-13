//go:build !go1.26
// +build !go1.26

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
	_ "unsafe"
)

//go:linkname duffcopy runtime.duffcopy
func duffcopy()

//go:linkname duffzero runtime.duffzero
func duffzero()

var duffcopyStart, duffcopyEnd, duffzeroStart, duffzeroEnd uintptr

func init() {
	duffcopyStart, duffcopyEnd = calcFnAddrRange("duffcopy", duffcopy)
	duffzeroStart, duffzeroEnd = calcFnAddrRange("duffzero", duffzero)
}

// proxyCallRace exclude functions used for race check in unit test
//
// If we use '-race' in test command, golang may use 'racefuncenter' and 'racefuncexit' in generic
// function's proxy, which is toxic for us to find the original generic function implementation.
//
// So we need to exclude them. We simply exclude most of race functions defined in runtime.
var proxyCallRace = map[uintptr]string{}

// isGenericProxyCallExtra checks if generic function's proxy called an extra internal function
// We check duffcopy/duffzero for passing huge params/returns and race-functions for -race option
func isGenericProxyCallExtra(addr uintptr) (bool, string) {
	/*
		When function argument size is too big, golang will use duff-copy
		to get better performance. Thus, we will see more call-instruction
		in asm code.
		For example, assume struct type like this:
			```
			type Large15 struct _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ string}
			```
		when we use `Large15` as generic function's argument or return types,
		the wrapper `function[Large15]` will call `duffcopy` and `duffzero`
		before passing arguments and after receiving returns.

		Notice that `duff` functions are very special, they are always called
		in the middle of function body(not at beginning). So we should check
		the `call` instruction's target address with a range.
	*/
	if addr >= duffcopyStart && addr <= duffcopyEnd {
		return true, "diffcopy"
	}

	if addr >= duffzeroStart && addr <= duffzeroEnd {
		return true, "duffzero"
	}

	if name, ok := proxyCallRace[addr]; ok {
		return true, name
	}
	return false, ""
}
