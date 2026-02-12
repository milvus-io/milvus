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

package common

import (
	"syscall"

	"github.com/bytedance/mockey/internal/tool"
)

var pageSize = uintptr(syscall.Getpagesize())

func PageOf(ptr uintptr) uintptr {
	return ptr &^ (pageSize - 1)
}

func PageSize() int {
	return int(pageSize)
}

func AllocatePage() []byte {
	page, err := allocate(int(pageSize))
	tool.Assert(err == nil, "allocate page failed: %v", err)
	return page
}

func ReleasePage(mem []byte) {
	err := free(mem)
	tool.Assert(err == nil, "free page failed: %v", err)
}
