// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import "unsafe"

/* #nosec G103 */
func UnsafeReadInt8(buf []byte, idx int) int8 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int8)(ptr))
}

/* #nosec G103 */
func UnsafeReadInt16(buf []byte, idx int) int16 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int16)(ptr))
}

/* #nosec G103 */
func UnsafeReadInt32(buf []byte, idx int) int32 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int32)(ptr))
}

/* #nosec G103 */
func UnsafeReadInt64(buf []byte, idx int) int64 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int64)(ptr))
}

/* #nosec G103 */
func UnsafeReadFloat32(buf []byte, idx int) float32 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*float32)(ptr))
}

/* #nosec G103 */
func UnsafeReadFloat64(buf []byte, idx int) float64 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*float64)(ptr))
}
