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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMetaCacheGatedFields(t *testing.T) {
	mc := &MetaCache{gatedFields: make(map[UniqueID]map[int64]struct{})}
	assert.False(t, mc.IsFieldGated(100, 10))

	mc.SetGatedFields(100, []int64{10, 11})
	assert.True(t, mc.IsFieldGated(100, 10))
	assert.True(t, mc.IsFieldGated(100, 11))
	assert.False(t, mc.IsFieldGated(100, 12))
	assert.False(t, mc.IsFieldGated(101, 10))

	// A push replaces the whole set (the proxy holds the union, not deltas).
	mc.SetGatedFields(100, []int64{12})
	assert.False(t, mc.IsFieldGated(100, 10))
	assert.True(t, mc.IsFieldGated(100, 12))

	// An empty push releases every gate of the collection.
	mc.SetGatedFields(100, nil)
	assert.False(t, mc.IsFieldGated(100, 12))
}

func TestInvalidateCollectionMetaCache_DefencePush(t *testing.T) {
	ctx := context.Background()
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	prev := globalMetaCache
	defer func() { globalMetaCache = prev }()
	mc := &MetaCache{gatedFields: make(map[UniqueID]map[int64]struct{})}
	globalMetaCache = mc

	// A defence push applies the gated set and returns early -- it must NOT fall into
	// the msgType-driven eviction switch (the request carries no Base).
	status, err := node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
		CollectionID:       100,
		DefenceUpdate:      true,
		DefenceGatedFields: []int64{10},
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	assert.True(t, mc.IsFieldGated(100, 10))

	// An empty defence push releases the collection's gates.
	status, err = node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
		CollectionID:  100,
		DefenceUpdate: true,
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	assert.False(t, mc.IsFieldGated(100, 10))
}
