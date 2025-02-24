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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

func inspect[T comparable](a Allocator[T]) {
	m := a.Inspect()
	log.Info("Allocation", zap.Any("allocations", m), zap.Any("used", a.Used()))
}

func TestFixedSizeAllocator(t *testing.T) {
	a := NewFixedSizeAllocator[string](&Resource{100, 100, 100})

	// Allocate
	allocated, _ := a.Allocate("a1", &Resource{10, 10, 10})
	assert.Equal(t, true, allocated)
	allocated, _ = a.Allocate("a2", &Resource{90, 90, 90})
	assert.Equal(t, true, allocated)
	allocated, short := a.Allocate("a3", &Resource{10, 0, 0})
	assert.Equal(t, false, allocated)
	assert.Equal(t, &Resource{10, 0, 0}, short)
	allocated, _ = a.Allocate("a0", &Resource{-10, 0, 0})
	assert.Equal(t, false, allocated)
	inspect[string](a)

	// Release
	a.Release("a2")
	allocated, _ = a.Allocate("a3", &Resource{10, 0, 0})
	assert.Equal(t, true, allocated)

	// Inspect
	m := a.Inspect()
	assert.Equal(t, 2, len(m))

	// Allocate on identical id is not allowed
	allocated, _ = a.Allocate("a1", &Resource{10, 0, 0})
	assert.Equal(t, false, allocated)

	// Reallocate
	allocated, _ = a.Reallocate("a1", &Resource{10, 0, 0})
	assert.Equal(t, true, allocated)
	allocated, _ = a.Reallocate("a1", &Resource{-10, 0, 0})
	assert.Equal(t, true, allocated)
	allocated, _ = a.Reallocate("a1", &Resource{-20, 0, 0})
	assert.Equal(t, false, allocated)
	allocated, _ = a.Reallocate("a1", &Resource{80, 0, 0})
	assert.Equal(t, true, allocated)
	allocated, _ = a.Reallocate("a1", &Resource{10, 0, 0})
	assert.Equal(t, false, allocated)
	allocated, _ = a.Reallocate("a4", &Resource{0, 10, 0})
	assert.Equal(t, true, allocated)
}

func TestFixedSizeAllocatorRace(t *testing.T) {
	a := NewFixedSizeAllocator[string](&Resource{100, 100, 100})
	wg := new(sync.WaitGroup)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			allocated, _ := a.Allocate(fmt.Sprintf("a%d", index), &Resource{1, 1, 1})
			assert.Equal(t, true, allocated)
		}(i)
	}
	wg.Wait()
	m := a.Inspect()
	assert.Equal(t, 100, len(m))
}

func TestWait(t *testing.T) {
	a := NewFixedSizeAllocator[string](&Resource{100, 100, 100})
	allocated, _ := a.Allocate("a1", &Resource{100, 100, 100})
	assert.True(t, allocated)
	for i := 0; i < 100; i++ {
		go func(index int) {
			allocated, _ := a.Reallocate("a1", &Resource{-1, -1, -1})
			assert.Equal(t, true, allocated)
		}(i)
	}

	allocated, _ = a.Allocate("a2", &Resource{100, 100, 100})
	i := 1
	for !allocated {
		a.Wait()
		allocated, _ = a.Allocate("a2", &Resource{100, 100, 100})
		i++
	}
	assert.True(t, allocated)
	assert.True(t, i < 100 && i > 1)
}

func TestPhysicalAwareFixedSizeAllocator(t *testing.T) {
	hwMemoryLimit := int64(float32(hardware.GetMemoryCount()) * 0.9)
	hwDiskLimit := int64(1<<63 - 1)
	a := NewPhysicalAwareFixedSizeAllocator[string](&Resource{100, 100, 100}, hwMemoryLimit, hwDiskLimit, "/tmp")

	allocated, _ := a.Allocate("a1", &Resource{10, 10, 10})
	assert.Equal(t, true, allocated)
	allocated, _ = a.Allocate("a2", &Resource{90, 90, 90})
	assert.Equal(t, true, allocated)
	allocated, short := a.Allocate("a3", &Resource{10, 0, 0})
	assert.Equal(t, false, allocated)
	assert.Equal(t, &Resource{10, 0, 0}, short)

	// Reallocate
	allocated, _ = a.Reallocate("a1", &Resource{0, -10, 0})
	assert.True(t, allocated)
	allocated, _ = a.Reallocate("a1", &Resource{10, 0, 0})
	assert.False(t, allocated)
}
