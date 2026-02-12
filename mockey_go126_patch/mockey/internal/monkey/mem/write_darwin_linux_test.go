//go:build linux || darwin
// +build linux darwin

package mem

// Copyright 2023 2022 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import (
	"syscall"
	"testing"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/tool"
)

func Test_write(t *testing.T) {
	target := make([]byte, 1024)
	for i := 0; i < len(target); i++ {
		target[i] = 0x00
	}
	data := make([]byte, 512)
	for i := 0; i < len(data); i++ {
		data[i] = 0xaa + byte(i)
	}

	res := write(common.PtrOf(target), common.PtrOf(data), 3, common.PageOf(common.PtrOf(target)), common.PageSize(), syscall.PROT_READ|syscall.PROT_WRITE)
	tool.Assert(res == 0, "assert fail")
	tool.Assert(target[0] == 0xaa, "assert fail, target0  %x", target[0])
	tool.Assert(target[1] == 0xab, "assert fail, target1  %x", target[1])
	tool.Assert(target[2] == 0xac, "assert fail, target2  %x", target[2])
	tool.Assert(target[3] == 0x00, "assert fail, target3  %x", target[3])
}
