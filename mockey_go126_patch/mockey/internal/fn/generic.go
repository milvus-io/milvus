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

package fn

import (
	"reflect"
	"unsafe"
)

var (
	genericInfoType = reflect.TypeOf(GenericInfo(0))
)

type GenericInfo uintptr

// UsedParamType get the type of used parameter in generic function/struct
//
// For example: assume we have generic function "f[int, float64](x int, y T1) T2" and derived type f[int, float64]:
//
//	UsedParamType(0) == reflect.TypeOf(int(0))
//	UsedParamType(1) == reflect.TypeOf(float64(0))
//
// If index n is out of range, or the derived types have more complex structure(for example: define a generic struct
// in a generic function using generic types, unused parameterized type etc.), this function may return unexpected value
// or cause unrecoverable runtime error . So it is NOT RECOMMENDED to use this function unless you actually knows what
// you are doing.
func (g GenericInfo) UsedParamType(n uintptr) reflect.Type {
	var vt interface{}
	*(*uintptr)(unsafe.Pointer(&vt)) = *(*uintptr)(unsafe.Pointer(uintptr(g) + 8*n))
	return reflect.TypeOf(vt)
}

func (g GenericInfo) Equal(other GenericInfo) bool {
	return g == other
}
