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

package vralloc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupedAllocator(t *testing.T) {
	a := NewGroupedAllocatorBuilder("a", &Resource{100, 100, 100}).
		AddChild("c1", &Resource{10, 10, 10}).
		AddChild("c2", &Resource{10, 10, 10}).
		AddChild("c3", &Resource{90, 90, 90}).
		Build()

	c1 := a.GetAllocator("c1")
	c2 := a.GetAllocator("c2")
	c3 := a.GetAllocator("c3")

	// Allocate
	allocated, _ := c1.Allocate("x11", &Resource{10, 10, 10})
	assert.Equal(t, true, allocated)
	allocated, short := c1.Allocate("x12", &Resource{90, 90, 90})
	assert.Equal(t, false, allocated)
	assert.Equal(t, &Resource{90, 90, 90}, short)
	allocated, _ = c2.Allocate("x21", &Resource{10, 10, 10})
	assert.Equal(t, true, allocated)
	allocated, short = c3.Allocate("x31", &Resource{90, 90, 90})
	assert.Equal(t, false, allocated)
	assert.Equal(t, &Resource{10, 10, 10}, short)
	inspect[string](a)

	// Release
	c1.Release("x11")
	allocated, _ = c3.Allocate("x31", &Resource{90, 90, 90})
	assert.Equal(t, true, allocated)

	// Inspect
	m := a.Inspect()
	assert.Equal(t, 3, len(m))
}
