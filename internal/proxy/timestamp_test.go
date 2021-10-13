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

package proxy

import (
	"context"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/stretchr/testify/assert"
)

func TestNewTimestampAllocator(t *testing.T) {
	ctx := context.Background()
	tso := newMockTimestampAllocatorInterface()
	peerID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	tsAllocator, err := newTimestampAllocator(ctx, tso, peerID)
	assert.Nil(t, err)
	assert.NotNil(t, tsAllocator)
}

func TestTimestampAllocator_alloc(t *testing.T) {
	ctx := context.Background()
	tso := newMockTimestampAllocatorInterface()
	peerID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	tsAllocator, err := newTimestampAllocator(ctx, tso, peerID)
	assert.Nil(t, err)
	assert.NotNil(t, tsAllocator)

	count := rand.Uint32()%100 + 1
	ret, err := tsAllocator.alloc(count)
	assert.Nil(t, err)
	assert.Equal(t, int(count), len(ret))
}

func TestTimestampAllocator_AllocOne(t *testing.T) {
	ctx := context.Background()
	tso := newMockTimestampAllocatorInterface()
	peerID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	tsAllocator, err := newTimestampAllocator(ctx, tso, peerID)
	assert.Nil(t, err)
	assert.NotNil(t, tsAllocator)

	_, err = tsAllocator.AllocOne()
	assert.Nil(t, err)
}
