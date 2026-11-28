// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queryclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHedgeOutcomes(t *testing.T) {
	h := &hedgeOutcomes{}
	assert.True(t, h.lastUsed().IsZero())
	for i := 0; i < hedgeWinRateMinSamples-1; i++ {
		h.add(testEpoch, false)
		_, ok := h.winRate(testEpoch)
		assert.False(t, ok, "too few hedges")
	}
	h.add(testEpoch, true)
	r, ok := h.winRate(testEpoch)
	require.True(t, ok)
	assert.InDelta(t, 0.1, r, 1e-9)
	assert.Equal(t, testEpoch, h.lastUsed())

	// outcomes older than the window do not count
	_, ok = h.winRate(testEpoch.Add(hedgeWindow + time.Second))
	assert.False(t, ok)
	later := testEpoch.Add(hedgeWindow + time.Second)
	for i := 0; i < 20; i++ {
		h.add(later, i%2 == 0)
	}
	r, _ = h.winRate(later)
	assert.InDelta(t, 0.5, r, 1e-9, "only the twenty recent ones")

	// the ring keeps the latest outcomes
	for i := 0; i < hedgeOutcomesCap; i++ {
		h.add(later, true)
	}
	r, _ = h.winRate(later)
	assert.InDelta(t, 1, r, 1e-9)
	h.add(later.Add(time.Second), false)
	r, _ = h.winRate(later.Add(time.Second))
	assert.InDelta(t, 1-1.0/hedgeOutcomesCap, r, 1e-9)
	assert.Equal(t, later.Add(time.Second), h.lastUsed())
}
