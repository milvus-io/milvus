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

package dablooms

/*
#cgo CFLAGS: -I${SRCDIR}/cwrapper

#cgo LDFLAGS: -L${SRCDIR}/cwrapper/output -ldablooms -lstdc++ -lm
#include <stdlib.h>
#include <dablooms.h>
*/
import "C"

import (
	"unsafe"
)

// ScalingBloom is a scaling bloom filter that supports remove elements after added
type ScalingBloom struct {
	cfilter *C.scaling_bloom_t
}

// NewScalingBloom returns a ScalingBloom object
func NewScalingBloom(capacity uint64, errorRate float64) *ScalingBloom {
	sb := &ScalingBloom{
		cfilter: C.new_scaling_bloom(C.uint(capacity), C.double(errorRate)),
	}
	return sb
}

// Destroy is used to free memory of this object
func (sb *ScalingBloom) Destroy() {
	C.free_scaling_bloom(sb.cfilter)
}

// Add is used to add an element to this bloom filter
func (sb *ScalingBloom) Add(key []byte, id int64) bool {
	cKey := (*C.char)(unsafe.Pointer(&key[0]))
	return C.scaling_bloom_add(sb.cfilter, cKey, C.size_t(len(key)), C.uint64_t(id)) == 1
}

// Remove is used to remove an element from this bloom filter
func (sb *ScalingBloom) Remove(key []byte, id int64) bool {
	cKey := (*C.char)(unsafe.Pointer(&key[0]))
	return C.scaling_bloom_remove(sb.cfilter, cKey, C.size_t(len(key)), C.uint64_t(id)) == 1
}

// Check returns whether a key may exist in this bloom filter
func (sb *ScalingBloom) Check(key []byte) bool {
	cKey := (*C.char)(unsafe.Pointer(&key[0]))
	return C.scaling_bloom_check(sb.cfilter, cKey, C.size_t(len(key))) == 1
}
