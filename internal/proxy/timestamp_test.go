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

package proxy

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

func TestNewTimestampAllocator(t *testing.T) {
	tso := newMockTimestampAllocatorInterface()
	peerID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	tsAllocator, err := newTimestampAllocator(tso, peerID)
	assert.NoError(t, err)
	assert.NotNil(t, tsAllocator)
}

func TestTimestampAllocator_alloc(t *testing.T) {
	ctx := context.Background()
	tso := newMockTimestampAllocatorInterface()
	peerID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	tsAllocator, err := newTimestampAllocator(tso, peerID)
	assert.NoError(t, err)
	assert.NotNil(t, tsAllocator)

	count := rand.Uint32()%100 + 1
	ret, err := tsAllocator.alloc(ctx, count)
	assert.NoError(t, err)
	assert.Equal(t, int(count), len(ret))
}

func TestTimestampAllocator_AllocOne(t *testing.T) {
	ctx := context.Background()
	tso := newMockTimestampAllocatorInterface()
	peerID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	tsAllocator, err := newTimestampAllocator(tso, peerID)
	assert.NoError(t, err)
	assert.NotNil(t, tsAllocator)

	_, err = tsAllocator.AllocOne(ctx)
	assert.NoError(t, err)
}
