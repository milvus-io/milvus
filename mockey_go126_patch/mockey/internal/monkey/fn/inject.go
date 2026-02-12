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

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/monkey/mem/prot"
	"github.com/bytedance/mockey/internal/tool"
)

// InjectInto injects the raw codes into the target to make a new function. The target is the target function pointer.
func InjectInto(target reflect.Value, fnCode []byte) {
	vt := target.Type()
	tool.Assert(vt.Kind() == reflect.Ptr, "target is not a pointer")
	tool.Assert(vt.Elem().Kind() == reflect.Func, "target is not a function pointer")

	// ensure the code is executable
	err := prot.MProtectRX(fnCode)
	tool.Assert(err == nil, "protect page failed")

	// make a new function to receive the code
	carrier := MakeFunc(vt.Elem(), common.PtrOf(fnCode))

	// set the target with the new made function
	target.Elem().Set(carrier)
}

func MakeFunc(typ reflect.Type, addr uintptr) reflect.Value {
	tool.Assert(addr > 0, "func pc is zero")
	carrier := reflect.MakeFunc(typ, nil)
	type makeFuncImpl struct {
		fn uintptr
	}
	type value struct {
		_   uintptr
		ptr *makeFuncImpl
	}
	(*value)(unsafe.Pointer(&carrier)).ptr.fn = addr
	return carrier
}
