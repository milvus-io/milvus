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

package view

import "math"

const twoTierFillRate = 0.85

func TwoTierFullThreshold(expectedSize int64) int64 {
	return int64(float64(expectedSize) * twoTierFillRate)
}

func TwoTierFragmentThreshold(expectedSize int64) int64 {
	return TwoTierFullThreshold(expectedSize / 4)
}

type TwoTierGroup[T Sizable] struct {
	Inputs  []T
	MaxSize int64
}

// GroupTwoTier performs the same full and fragment passes for both selection
// modes. The caller classifies independent rewrites before this step.
func GroupTwoTier[T Sizable](inputs []T, compactTime *CompactTime, expectedSize, maxFragments int64, force bool) []TwoTierGroup[T] {
	packer := newCompactionPacker("full-tier", inputs, compactTime)
	maxLeft, minInputs := expectedSize-TwoTierFullThreshold(expectedSize), int64(2)
	if force {
		maxLeft, minInputs = math.MaxInt64, 1
	}

	var groups []TwoTierGroup[T]
	for {
		packed, _ := packer.PackSliding(expectedSize, maxLeft, minInputs, math.MaxInt64)
		if len(packed) == 0 {
			break
		}
		groups = append(groups, TwoTierGroup[T]{Inputs: packed, MaxSize: expectedSize})
	}
	if force {
		for _, input := range packer.Candidates {
			groups = append(groups, TwoTierGroup[T]{Inputs: []T{input}, MaxSize: expectedSize})
		}
		return groups
	}

	var fragments []T
	for _, input := range packer.Candidates {
		if input.GetResidualSegmentSize() < TwoTierFragmentThreshold(expectedSize) {
			fragments = append(fragments, input)
		}
	}
	if int64(len(fragments)) <= maxFragments {
		return groups
	}
	middleSize := expectedSize / 4
	fragmentPacker := newCompactionPacker("fragment-tier", fragments, compactTime)
	for {
		packed, _ := fragmentPacker.Pack(middleSize, math.MaxInt64, 2, math.MaxInt64)
		if len(packed) == 0 {
			break
		}
		groups = append(groups, TwoTierGroup[T]{Inputs: packed, MaxSize: middleSize})
	}
	return groups
}
