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
	"runtime"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/tool"
)

// Copy copies the original function code to a new page, injecting the code to the target function.
func Copy(targetPtr, oriFn interface{}) {
	targetVal := reflect.ValueOf(targetPtr)
	tool.Assert(targetVal.Type().Kind() == reflect.Ptr, "'%v' is not a pointer", targetPtr)
	targetType := targetVal.Type().Elem()
	tool.Assert(targetType.Kind() == reflect.Func, "'%v' is not a function pointer", targetPtr)
	oriVal := reflect.ValueOf(oriFn)
	tool.Assert(tool.CheckFuncArgs(targetType, oriVal.Type(), 0, 0), "target and ori not match")

	oriAddr := oriVal.Pointer()
	tool.DebugPrintf("Copy: copy start for %v\n", runtime.FuncForPC(oriAddr).Name())
	// allocate a new page to store copied fn
	targetCode := common.AllocatePage()
	oriCode := common.BytesOf(oriAddr, common.PageSize())
	tool.DebugPrintf("Copy: target addr: 0x%x, ori addr: 0x%x\n", common.PtrOf(targetCode), oriAddr)
	// copy ori fn code to target code
	copyCode(targetCode, oriCode)
	// inject target code into the target function
	InjectInto(targetVal, targetCode)
	tool.DebugPrintf("Copy: copy end for %v\n", runtime.FuncForPC(oriAddr).Name())
}
