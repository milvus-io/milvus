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

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type stats struct {
	TruePositives  int64
	TrueNegatives  int64
	FalsePositives int64
	FalseNegatives int64
}

var Capacity uint64 = 1000000
var ErrorRate float64 = .05

func PrintResults(stats *stats) {
	falsePositiveRate := float64(stats.FalsePositives) / float64(stats.FalsePositives+stats.TrueNegatives)
	fmt.Printf("True positives:		%7d\n", stats.TruePositives)
	fmt.Printf("True negatives:		%7d\n", stats.TrueNegatives)
	fmt.Printf("False positives:	%7d\n", stats.FalsePositives)
	fmt.Printf("False negatives:	%7d\n", stats.FalseNegatives)
	fmt.Printf("False positive rate: %f\n", falsePositiveRate)

	if falsePositiveRate > ErrorRate {
		fmt.Printf("False positive rate too high\n")
	}
}

func TestDablooms_Correctness(t *testing.T) {
	sb := NewScalingBloom(Capacity, ErrorRate)
	assert.NotNil(t, sb)

	start := time.Now().UnixNano()
	for i := 0; i < int(Capacity*2); i++ {
		if i%2 == 0 {
			key := strconv.Itoa(i)
			sb.Add([]byte(key), int64(i))
		}
	}
	end := time.Now().UnixNano()

	seconds := float64((end - start) / 1e9)
	fmt.Printf("The time cost for add: %fs\n", seconds)

	results := &stats{
		TruePositives:  0,
		TrueNegatives:  0,
		FalsePositives: 0,
		FalseNegatives: 0,
	}

	start = time.Now().UnixNano()
	for i := 0; i < int(Capacity*2); i++ {
		if i%2 == 1 {
			key := strconv.Itoa(i)
			positive := sb.Check([]byte(key))
			if positive {
				results.FalsePositives++
			} else {
				results.TrueNegatives++
			}
		}
	}
	end = time.Now().UnixNano()
	seconds = float64((end - start) / 1e9)
	fmt.Printf("Time cost for check: %fs\n", seconds)

	sb.Destroy()

	PrintResults(results)

	// False negatives means that there should
	assert.False(t, results.FalseNegatives > 0)
}
