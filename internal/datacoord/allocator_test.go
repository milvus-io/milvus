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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocator_Basic(t *testing.T) {
	ms := newMockRootCoordService()
	allocator := newRootCoordAllocator(ms)
	ctx := context.Background()

	t.Run("Test allocTimestamp", func(t *testing.T) {
		_, err := allocator.allocTimestamp(ctx)
		assert.NoError(t, err)
	})

	t.Run("Test allocID", func(t *testing.T) {
		_, err := allocator.allocID(ctx)
		assert.NoError(t, err)
	})

	t.Run("Test Unhealthy Root", func(t *testing.T) {
		ms := newMockRootCoordService()
		allocator := newRootCoordAllocator(ms)
		err := ms.Stop()
		assert.Nil(t, err)

		_, err = allocator.allocTimestamp(ctx)
		assert.Error(t, err)
		_, err = allocator.allocID(ctx)
		assert.Error(t, err)
	})
}
