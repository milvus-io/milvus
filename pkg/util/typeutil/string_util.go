// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"strings"
	"unsafe"
)

// AddOne add one to last byte in string, on empty string return empty
// it helps with key iteration upper bound
func AddOne(data string) string {
	if len(data) == 0 {
		return data
	}
	datab := []byte(data)
	if datab[len(datab)-1] != 255 {
		datab[len(datab)-1]++
	} else {
		datab = append(datab, byte(0))
	}
	return string(datab)
}

// After get substring after sub string.
func After(str string, sub string) string {
	pos := strings.LastIndex(str, sub)
	if pos == -1 {
		return ""
	}
	adjustedPos := pos + len(sub)
	if adjustedPos >= len(str) {
		return ""
	}
	return str[adjustedPos:]
}

// AfterN Split slices After(str) into all substrings separated by sep
func AfterN(str string, sub string, sep string) []string {
	return strings.Split(After(str, sub), sep)
}

/* #nosec G103 */
func UnsafeStr2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	b := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&b))
}

/* #nosec G103 */
func UnsafeBytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
