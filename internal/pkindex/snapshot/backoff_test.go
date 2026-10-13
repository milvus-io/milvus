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

package snapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoff(t *testing.T) {
	b := &Backoff{Base: time.Second, Max: time.Minute}
	now := time.Unix(1000, 0)
	assert.True(t, b.Ready(now))

	want := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, time.Minute, time.Minute, time.Minute,
	}
	for i, wait := range want {
		b.Fail(now)
		assert.Equal(t, i+1, b.Failures())
		assert.False(t, b.Ready(now.Add(wait-time.Nanosecond)), "failure %d", i+1)
		assert.True(t, b.Ready(now.Add(wait)), "failure %d", i+1)
	}

	// the shift stops growing and still stays capped
	for i := 0; i < 100; i++ {
		b.Fail(now)
	}
	assert.True(t, b.Ready(now.Add(time.Minute)))
	assert.False(t, b.Ready(now.Add(time.Minute-time.Nanosecond)))

	b.Succeed()
	assert.Equal(t, 0, b.Failures())
	assert.True(t, b.Ready(now))
}

func TestBackoffOverflowIsCapped(t *testing.T) {
	b := &Backoff{Base: time.Duration(1) << 60, Max: time.Hour}
	now := time.Unix(1000, 0)
	b.Fail(now)
	assert.True(t, b.Ready(now.Add(time.Hour)))
	b.Fail(now)
	assert.True(t, b.Ready(now.Add(time.Hour)))
	assert.False(t, b.Ready(now.Add(time.Hour-time.Nanosecond)))
}
