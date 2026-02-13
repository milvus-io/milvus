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
	"fmt"
	"reflect"
)

func Assert(b bool, fmts ...interface{}) {
	if !b {
		var fmtStr string
		if len(fmts) == 0 {
			fmtStr = "unexpected happened"
		} else if _, ok := fmts[0].(string); !ok {
			fmtStr = "%+v"
		} else {
			fmtStr = fmts[0].(string)
			fmts = fmts[1:]
		}
		panic(fmt.Sprintf(fmtStr, fmts...))
	}
}

func AssertFunc(target interface{}) {
	Assert(reflect.TypeOf(target).Kind() == reflect.Func, "'%v' is not a function", target)
}

func AssertPtr(ptr interface{}) {
	Assert(reflect.TypeOf(ptr).Kind() == reflect.Ptr, "'%v' is not a pointer", ptr)
}
