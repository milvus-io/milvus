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

package shardclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

func TestWorkloadsStayUnscopedForAnUnscopedRequest(t *testing.T) {
	ctx := context.Background()
	assert.Empty(t, scopedCollectionWorkload(ctx, CollectionWorkLoad{CollectionID: 1}).ResourceGroup,
		"a request that was never scoped must route across every replica")
	assert.Empty(t, scopedChannelWorkload(ctx, ChannelWorkload{CollectionID: 1}).ResourceGroup)
}

func TestWorkloadsAreStampedWithTheRequestScope(t *testing.T) {
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-b")
	collection := scopedCollectionWorkload(ctx, CollectionWorkLoad{CollectionID: 1})
	assert.Equal(t, "rg-b", collection.ResourceGroup)
	channel := scopedChannelWorkload(ctx, ChannelWorkload{CollectionID: 1, Channel: "ch0"})
	assert.Equal(t, "rg-b", channel.ResourceGroup)
	assert.Equal(t, "rg-b", collection.ForChannel("ch0", 0).ResourceGroup,
		"the scope must follow the collection workload onto every channel it fans out to")
}

func TestAnExplicitWorkloadScopeIsKept(t *testing.T) {
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-b")
	collection := scopedCollectionWorkload(ctx, CollectionWorkLoad{CollectionID: 1, ResourceGroup: "rg-explicit"})
	assert.Equal(t, "rg-explicit", collection.ResourceGroup)
	channel := scopedChannelWorkload(ctx, ChannelWorkload{CollectionID: 1, ResourceGroup: "rg-explicit"})
	assert.Equal(t, "rg-explicit", channel.ResourceGroup)
}
