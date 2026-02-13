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

package tool

import (
	"reflect"
)

func CheckReturnValues(t reflect.Type, results ...interface{}) {
	Assert(t.NumOut() == len(results), "return args not match: target: %v, expected count: %d, current count: %d", t, t.NumOut(), len(results))
	for i := 0; i < t.NumOut(); i++ {
		if results[i] == nil {
			continue
		}
		Assert(reflect.TypeOf(results[i]).ConvertibleTo(t.Out(i)), "return args not match: target: %v, index: %v, current type: %v", t, i, reflect.TypeOf(results[i]))
	}
}

func CheckFuncReturnValues(a, b reflect.Type) {
	Assert(a.NumOut() == b.NumOut(), "return args not match: target: %v, current: %v", a, b)
	for indexA, indexB := 0, 0; indexA < a.NumOut(); indexA, indexB = indexA+1, indexB+1 {
		Assert(a.Out(indexA) == b.Out(indexB), "return args not match: target: %v, current: %v", a, b)
	}
}

func CheckFuncArgs(a, b reflect.Type, shiftA, shiftB int) bool {
	if a.NumIn()-shiftA == b.NumIn()-shiftB {
		for indexA, indexB := shiftA, shiftB; indexA < a.NumIn(); indexA, indexB = indexA+1, indexB+1 {
			if a.In(indexA) != b.In(indexB) {
				return false
			}
		}
		return true
	}
	return false
}
