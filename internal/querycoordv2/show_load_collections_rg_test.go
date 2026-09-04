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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// withFailedLoadCache makes sure the global failed-load cache ShowLoadCollections
// expires on every call exists for the test.
func withFailedLoadCache(t *testing.T) {
	t.Helper()
	if meta.GlobalFailedLoadCache == nil {
		meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	}
}

func TestShowLoadCollectionsWithoutAResourceGroupKeepsTheCollectionWideFigure(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putReplica(t, 100, 20, "rg-b")
	f.putDelegator(100, 10, "100-dmc0", 1, 2) // rg-a fully loaded, rg-b has nothing yet

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{100},
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, []int64{100}, resp.GetCollectionIDs())
	assert.Equal(t, f.meta.CalculateLoadPercentage(context.Background(), 100), int32(resp.GetInMemoryPercentages()[0]),
		"without a resource group the answer is the collection-wide percentage, as before")
}

func TestShowLoadCollectionsScopedToAResourceGroup(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putReplica(t, 100, 20, "rg-b")
	f.putDelegator(100, 10, "100-dmc0", 1, 2) // rg-a fully loaded, rg-b has nothing yet
	f.putResourceGroup(t, "rg-empty")

	show := func(rg string) *querypb.ShowCollectionsResponse {
		resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
			CollectionIDs: []int64{100},
			ResourceGroup: rg,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Equal(t, []int64{100}, resp.GetCollectionIDs())
		return resp
	}

	assert.EqualValues(t, 100, show("rg-a").GetInMemoryPercentages()[0],
		"the group whose replica serves every target is at 100 whatever its sibling does")
	assert.EqualValues(t, 0, show("rg-b").GetInMemoryPercentages()[0],
		"the group whose replica serves nothing yet is at 0, not at the collection-wide figure")
	assert.EqualValues(t, -1, show("rg-empty").GetInMemoryPercentages()[0],
		"a group that holds no replica of the collection answers -1, which is not 0")
}

func TestShowLoadCollectionsScopedToAnUnknownResourceGroupIsRefused(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putDelegator(100, 10, "100-dmc0", 1, 2)

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{100},
		ResourceGroup: "rg-missing",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrResourceGroupNotFound)
}

func TestShowLoadCollectionsScopedAnswersMinusOneForAnUnloadedCollection(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putResourceGroup(t, "rg-a")
	const neverLoaded = int64(777)

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{neverLoaded},
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotLoaded,
		"unscoped, an unloaded collection is refused exactly as before")

	resp, err = f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{neverLoaded},
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, []int64{neverLoaded}, resp.GetCollectionIDs())
	assert.EqualValues(t, -1, resp.GetInMemoryPercentages()[0],
		"scoped, an unloaded collection with no recorded failure holds no replica in the group: -1, not a refusal")
	assert.False(t, resp.GetQueryServiceAvailable()[0])
}
