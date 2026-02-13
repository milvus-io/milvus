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
	"runtime"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/monkey/stw"
	"github.com/bytedance/mockey/internal/monkey/sysmon"
	"github.com/bytedance/mockey/internal/tool"
)

// WriteWithSTW copies data bytes to the target address and replaces the original bytes, during which it will stop the
// world (only the current goroutine's P is running).
func WriteWithSTW(target uintptr, data []byte) {
	resumeFn := suspendRuntime()
	defer resumeFn()

	begin := target
	end := target + uintptr(len(data))
	for begin < end {
		if common.PageOf(begin) < common.PageOf(end) {
			nextPage := common.PageOf(begin) + uintptr(common.PageSize())
			buf := data[:nextPage-begin]
			data = data[nextPage-begin:]
			err := Write(begin, buf)
			tool.Assert(err == nil, err)
			begin += uintptr(len(buf))
			continue
		}
		err := Write(begin, data)
		tool.Assert(err == nil, err)
		break
	}
}

func suspendRuntime() (resume func()) {
	runtime.LockOSThread()
	stwResume := stw.StopTheWorld()
	// Suspend the system monitor thread to avoid SIGBUS errors during memory writes
	// See https://github.com/bytedance/mockey/issues/68 for more details.
	sysmonResume := sysmon.SuspendSysmon()

	resume = func() {
		sysmonResume()
		stwResume()
		runtime.UnlockOSThread()
	}
	return
}
