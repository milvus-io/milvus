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

func TestLatencySamplesQuantile(t *testing.T) {
	l := &latencySamples{}
	_, ok := l.quantile(testEpoch, 0.99)
	assert.False(t, ok, "no sample")
	assert.True(t, l.lastUsed().IsZero())

	// a few samples are enough: all of them are used, however old
	l.add(testEpoch, 7*time.Millisecond)
	q, ok := l.quantile(testEpoch.Add(time.Hour), 0.99)
	require.True(t, ok)
	assert.Equal(t, 7*time.Millisecond, q)
	assert.Equal(t, testEpoch, l.lastUsed())

	// within the window every sample counts: 990 of 10ms and 10 of 200ms
	l = &latencySamples{}
	for i := 0; i < 990; i++ {
		l.add(testEpoch, 10*time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		l.add(testEpoch, 200*time.Millisecond)
	}
	q, _ = l.quantile(testEpoch, 0.99)
	assert.Equal(t, 10*time.Millisecond, q)
	q, _ = l.quantile(testEpoch, 0.995)
	assert.Equal(t, 200*time.Millisecond, q)
	q, _ = l.quantile(testEpoch.Add(hedgeWindow), 0.99)
	assert.Equal(t, 10*time.Millisecond, q, "still in the window")
	// out of the window: the latest hedgeRecentSamples, of which the last ten are 200ms
	q, _ = l.quantile(testEpoch.Add(hedgeWindow+time.Second), 0.99)
	assert.Equal(t, 200*time.Millisecond, q)
	q, _ = l.quantile(testEpoch.Add(hedgeWindow+time.Second), 0.5)
	assert.Equal(t, 10*time.Millisecond, q)

	// new samples push the old ones out of the window
	later := testEpoch.Add(time.Minute)
	for i := 0; i < 200; i++ {
		l.add(later, time.Millisecond)
	}
	q, _ = l.quantile(later, 1)
	assert.Equal(t, time.Millisecond, q)

	// the ring keeps the latest samples once full
	for i := 0; i < latencySamplesCap+5; i++ {
		l.add(later, 2*time.Millisecond)
	}
	l.add(later.Add(time.Second), time.Second)
	assert.Len(t, l.ring, latencySamplesCap)
	q, _ = l.quantile(later.Add(time.Second), 1)
	assert.Equal(t, time.Second, q)
	q, _ = l.quantile(later.Add(time.Second), 0.5)
	assert.Equal(t, 2*time.Millisecond, q)
	assert.Equal(t, later.Add(time.Second), l.lastUsed())
}
