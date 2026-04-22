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

package metric

import "strings"

// PositivelyRelated returns true if the metric type is positively related,
// meaning higher scores indicate better similarity.
func PositivelyRelated(metricType string) bool {
	mUpper := strings.ToUpper(metricType)
	return mUpper == strings.ToUpper(IP) ||
		mUpper == strings.ToUpper(COSINE) ||
		mUpper == strings.ToUpper(BM25) ||
		mUpper == strings.ToUpper(MHJACCARD) ||
		mUpper == strings.ToUpper(MaxSim) ||
		mUpper == strings.ToUpper(MaxSimIP) ||
		mUpper == strings.ToUpper(MaxSimCosine)
}

// IsBounded returns true for metric types whose scores have a well-defined
// mathematical range that must be enforced. Floating-point arithmetic in the
// C++ core (Knowhere/Segcore) can produce values outside this range for
// identical or near-identical vectors due to precision loss.
func IsBounded(metricType string) bool {
	mUpper := strings.ToUpper(metricType)
	return mUpper == strings.ToUpper(COSINE) ||
		mUpper == strings.ToUpper(MaxSimCosine)
}

// ClampCosineScores clamps each score in-place to [-1, 1] for metric types
// that are mathematically bounded in that range (COSINE, MAX_SIM_COSINE).
// For all other metric types this is a no-op.
//
// Floating-point arithmetic in Knowhere/Segcore can produce values such as
// 1.0000001192092896 for identical vectors. Without clamping, callers see
// distances that violate the mathematical definition of cosine similarity and
// may fail downstream validation (e.g. range filters, client-side assertions).
func ClampCosineScores(scores []float32, metricType string) {
	if !IsBounded(metricType) {
		return
	}
	for i, s := range scores {
		if s > 1.0 {
			scores[i] = 1.0
		} else if s < -1.0 {
			scores[i] = -1.0
		}
	}
}
