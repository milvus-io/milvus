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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSummarize(t *testing.T) {
	assert.Nil(t, summarize(nil))
	var costs []time.Duration
	for i := 1; i <= 20; i++ {
		costs = append(costs, time.Duration(i)*time.Millisecond)
	}
	assert.Equal(t, &latency{P50: 10, P95: 19, Max: 20, Samples: 20}, summarize(costs))
	assert.Equal(t, &latency{P50: 0.001, P95: 0.001, Max: 0.001, Samples: 1}, summarize([]time.Duration{1499 * time.Nanosecond}))
}
