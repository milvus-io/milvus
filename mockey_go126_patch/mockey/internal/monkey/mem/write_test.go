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
	"testing"

	"github.com/bytedance/mockey/internal/monkey/common"
	"github.com/bytedance/mockey/internal/tool"
)

func TestWrite(t *testing.T) {
	data := make([]byte, common.PageSize()*3)
	target := uintptr(common.PageSize() / 2)

	expected := [][2]uintptr{
		{target, uintptr(common.PageSize())},
		{uintptr(common.PageSize()), uintptr(common.PageSize()) * 2},
		{uintptr(common.PageSize() * 2), uintptr(common.PageSize()) * 3},
		{uintptr(common.PageSize() * 3), target + uintptr(len(data))},
	}

	begin := target
	end := target + uintptr(len(data))
	index := 0
	for begin < end {
		if common.PageOf(begin) < common.PageOf(end) {
			nextPage := common.PageOf(begin) + uintptr(common.PageSize())
			buf := data[:nextPage-begin]
			data = data[nextPage-begin:]

			// err := Write(begin, buf)
			// tool.Assert(err == nil, err)
			tool.Assert(begin == expected[index][0], index, begin, expected[index][0])
			tool.Assert(begin+uintptr(len(buf)) == expected[index][1], index, begin+uintptr(len(buf)), expected[index][1])

			begin += uintptr(len(buf))
			index += 1
			continue
		}

		// err := Write(begin, data)
		// tool.Assert(err == nil, err)
		tool.Assert(begin == expected[index][0], index, begin, expected[index][0])
		tool.Assert(begin+uintptr(len(data)) == expected[index][1], index, begin+uintptr(len(data)), expected[index][1])

		break
	}
}
