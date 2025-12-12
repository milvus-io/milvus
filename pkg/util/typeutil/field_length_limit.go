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

import "sync/atomic"

const (
	defaultVarCharEstimateLength      = 256
	defaultDynamicFieldEstimateLength = 512
	defaultSparseFloatEstimateLength  = 1200
)

var (
	varCharEstimateLength      atomic.Int64
	dynamicFieldEstimateLength atomic.Int64
	sparseEstimateLength       atomic.Int64
)

func init() {
	SetVarCharEstimateLength(defaultVarCharEstimateLength)
	SetDynamicFieldEstimateLength(defaultDynamicFieldEstimateLength)
	SetSparseFloatVectorEstimateLength(defaultSparseFloatEstimateLength)
}

// SetVarCharEstimateLength updates the global cap applied when estimating record sizes for VarChar fields.
func SetVarCharEstimateLength(length int) {
	if length <= 0 {
		length = defaultVarCharEstimateLength
	}
	varCharEstimateLength.Store(int64(length))
}

// GetVarCharEstimateLength returns the current cap used when estimating VarChar field sizes.
func GetVarCharEstimateLength() int {
	length := int(varCharEstimateLength.Load())
	if length <= 0 {
		return defaultVarCharEstimateLength
	}
	return length
}

// SetDynamicFieldEstimateLength updates the global cap used for dynamic fields (JSON/Array/Geometry).
func SetDynamicFieldEstimateLength(length int) {
	if length <= 0 {
		length = defaultDynamicFieldEstimateLength
	}
	dynamicFieldEstimateLength.Store(int64(length))
}

// GetDynamicFieldEstimateLength returns the current cap for dynamic fields.
func GetDynamicFieldEstimateLength() int {
	length := int(dynamicFieldEstimateLength.Load())
	if length <= 0 {
		return defaultDynamicFieldEstimateLength
	}
	return length
}

// SetSparseFloatVectorEstimateLength updates the fallback size used when estimating sparse float vector fields.
func SetSparseFloatVectorEstimateLength(length int) {
	if length <= 0 {
		length = defaultSparseFloatEstimateLength
	}
	sparseEstimateLength.Store(int64(length))
}

// GetSparseFloatVectorEstimateLength returns the current fallback size used for sparse float vector fields.
func GetSparseFloatVectorEstimateLength() int {
	length := int(sparseEstimateLength.Load())
	if length <= 0 {
		return defaultSparseFloatEstimateLength
	}
	return length
}
