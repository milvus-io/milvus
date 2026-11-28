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

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// listingCoordinator lists, like RootCoord, only the collections the caller
// may see, while describing any collection to the shared metadata cache.
type listingCoordinator struct {
	types.MixCoordClient
	collections map[string]int64 // name -> ID, all in the default database
	aliases     map[string]string
	listable    map[int64]bool

	listRequests []*milvuspb.ShowCollectionsRequest
	loadRequests [][]int64
}

func (c *listingCoordinator) DescribeCollection(_ context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	name := req.GetCollectionName()
	if target, ok := c.aliases[name]; ok {
		name = target
	}
	for collectionName, collectionID := range c.collections {
		if req.GetCollectionID() != 0 && collectionID == req.GetCollectionID() {
			name = collectionName
		}
	}
	id, ok := c.collections[name]
	if !ok {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(merr.WrapErrCollectionNotFoundWithDB(req.GetDbName(), req.GetCollectionName())),
		}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(), CollectionID: id, DbName: util.DefaultDBName, ShardsNum: 1,
		Schema: &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
	}, nil
}

func (c *listingCoordinator) ShowCollections(_ context.Context, req *milvuspb.ShowCollectionsRequest, _ ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	c.listRequests = append(c.listRequests, proto.Clone(req).(*milvuspb.ShowCollectionsRequest))
	selected := map[string]bool{}
	for _, name := range req.GetCollectionNames() {
		selected[name] = true
	}
	resp := &milvuspb.ShowCollectionsResponse{Status: merr.Success()}
	for name, id := range c.collections {
		if !c.listable[id] || (len(selected) > 0 && !selected[name]) {
			continue
		}
		resp.CollectionNames = append(resp.CollectionNames, name)
		resp.CollectionIds = append(resp.CollectionIds, id)
		resp.CreatedTimestamps = append(resp.CreatedTimestamps, 1)
		resp.CreatedUtcTimestamps = append(resp.CreatedUtcTimestamps, 1)
		resp.ShardsNum = append(resp.ShardsNum, 1)
	}
	return resp, nil
}

func (c *listingCoordinator) ShowLoadCollections(_ context.Context, req *querypb.ShowCollectionsRequest, _ ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	c.loadRequests = append(c.loadRequests, req.GetCollectionIDs())
	ids := req.GetCollectionIDs()
	if len(ids) == 0 {
		// Without IDs QueryCoord reports every loaded collection.
		for _, id := range c.collections {
			ids = append(ids, id)
		}
	}
	resp := &querypb.ShowCollectionsResponse{Status: merr.Success()}
	for _, id := range ids {
		resp.CollectionIDs = append(resp.CollectionIDs, id)
		resp.InMemoryPercentages = append(resp.InMemoryPercentages, 100)
		resp.QueryServiceAvailable = append(resp.QueryServiceAvailable, true)
	}
	return resp, nil
}

func showInMemory(t *testing.T, coord *listingCoordinator, cache Cache, names ...string) (*milvuspb.ShowCollectionsResponse, error) {
	ctx := context.Background()
	task := &ShowCollectionsTask{
		baseTask:  baseTask{MetaCache: cache},
		Condition: NewTaskCondition(ctx),
		ShowCollectionsRequest: &milvuspb.ShowCollectionsRequest{
			DbName: util.DefaultDBName, Type: milvuspb.ShowType_InMemory, CollectionNames: names,
		},
		ctx:      ctx,
		mixCoord: coord,
	}
	require.NoError(t, task.OnEnqueue())
	require.NoError(t, task.PreExecute(ctx))
	if err := task.Execute(ctx); err != nil {
		return nil, err
	}
	return task.result, nil
}

// A collection the caller may not list must not become visible because the
// shared metadata cache can resolve its name, whether or not another caller
// has already warmed the cache.
func TestShowCollectionsInMemoryHidesUnlistedCollections(t *testing.T) {
	paramtable.Init()
	metacache.MockPublishedSchemaForTest(t)
	newCoord := func(withHidden bool) *listingCoordinator {
		coord := &listingCoordinator{
			collections: map[string]int64{"visible": 1},
			aliases:     map[string]string{"visible_alias": "visible"},
			listable:    map[int64]bool{1: true},
		}
		if withHidden {
			coord.collections["hidden"] = 2
			coord.aliases["hidden_alias"] = "hidden"
		}
		return coord
	}
	newCache := func(coord *listingCoordinator) *metacache.MetaCache {
		cache, err := metacache.NewMetaCache(coord)
		require.NoError(t, err)
		t.Cleanup(cache.Close)
		return cache
	}

	for _, warm := range []bool{false, true} {
		for _, name := range []string{"hidden", "hidden_alias"} {
			coord := newCoord(true)
			cache := newCache(coord)
			if warm {
				_, err := cache.GetCollectionID(context.Background(), util.DefaultDBName, name)
				require.NoError(t, err)
			}
			_, hiddenErr := showInMemory(t, coord, cache, "visible", name)
			require.Error(t, hiddenErr)
			assert.Empty(t, coord.loadRequests, "load state of an unlisted collection must not be read")

			absent := newCoord(false)
			_, absentErr := showInMemory(t, absent, newCache(absent), "visible", name)
			require.Error(t, absentErr)
			assert.True(t, proto.Equal(merr.Status(hiddenErr), merr.Status(absentErr)),
				"warm=%v %s: an unlisted collection must look absent: %v / %v", warm, name, hiddenErr, absentErr)
		}
	}

	coord := newCoord(true)
	resp, err := showInMemory(t, coord, newCache(coord), "visible_alias")
	require.NoError(t, err)
	assert.Equal(t, []string{"visible_alias"}, resp.GetCollectionNames())
	assert.Equal(t, []int64{1}, resp.GetCollectionIds())
	assert.Equal(t, []int64{100}, resp.GetInMemoryPercentages())
	require.Len(t, coord.listRequests, 1)
	assert.Equal(t, []string{"visible"}, coord.listRequests[0].GetCollectionNames(), "RootCoord selects by collection name, not alias")

	// Without names the listing is RootCoord's, as before, even though the
	// hidden collection is loaded too.
	coord = newCoord(true)
	resp, err = showInMemory(t, coord, newCache(coord))
	require.NoError(t, err)
	assert.Equal(t, []string{"visible"}, resp.GetCollectionNames())
	assert.Empty(t, coord.listRequests[0].GetCollectionNames())

	// Every listed collection stays visible, so callers that may see a
	// collection are unaffected.
	coord = newCoord(true)
	coord.listable[2] = true
	resp, err = showInMemory(t, coord, newCache(coord), "visible", "hidden_alias")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"visible", "hidden_alias"}, resp.GetCollectionNames())
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
}
