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

package mem

import (
	"fmt"
	"reflect"
	"syscall"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/tool"
)

func Write(target uintptr, data []byte) error {
	targetPage := common.PageOf(target)
	fnPage := common.PageOf(reflect.ValueOf(write).Pointer())
	tool.DebugPrintf("Write: target page(0x%x), fn page(0x%x), len(%v)\n", targetPage, fnPage, len(data))
	res := write(target, common.PtrOf(data), len(data), targetPage, common.PageSize(), syscall.PROT_READ|syscall.PROT_EXEC)
	if res != 0 {
		return fmt.Errorf("write failed, code %v", res)
	}
	tool.DebugPrintf("Write end(darwin)\n")
	return nil
}

//go:cgo_import_dynamic mach_task_self mach_task_self "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic mach_vm_protect mach_vm_protect "/usr/lib/libSystem.B.dylib"
func write(target, data uintptr, len int, page uintptr, pageSize, oriProt int) int
