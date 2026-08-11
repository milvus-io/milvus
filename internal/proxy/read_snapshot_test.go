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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type readSnapshotTestCache struct {
	Cache

	current        *collectionInfo
	infoCalls      int
	partitions     map[UniqueID]*partitionInfos
	partitionErrs  []error
	partitionCalls []UniqueID
}

func (c *readSnapshotTestCache) GetCollectionInfo(
	context.Context,
	string,
	string,
	int64,
) (*collectionInfo, error) {
	c.infoCalls++
	return c.current, nil
}

func (c *readSnapshotTestCache) GetPartitionInfosByID(
	_ context.Context,
	_ string,
	collectionID int64,
) (*partitionInfos, error) {
	c.partitionCalls = append(c.partitionCalls, collectionID)
	if len(c.partitionErrs) > 0 {
		err := c.partitionErrs[0]
		c.partitionErrs = c.partitionErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return c.partitions[collectionID], nil
}

func newAliasSnapshotCollectionInfo(
	dbID UniqueID,
	collectionID UniqueID,
	name string,
	consistencyLevel commonpb.ConsistencyLevel,
) *collectionInfo {
	return &collectionInfo{
		dbID:             dbID,
		dbName:           "db",
		collID:           collectionID,
		consistencyLevel: consistencyLevel,
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{Name: name},
		},
	}
}

func TestReadRequestSnapshotPinsAliasTargetAcrossConsumers(t *testing.T) {
	c1 := newAliasSnapshotCollectionInfo(10, 101, "collection_1", commonpb.ConsistencyLevel_Strong)
	c2 := newAliasSnapshotCollectionInfo(10, 202, "collection_2", commonpb.ConsistencyLevel_Bounded)
	cache := &readSnapshotTestCache{
		current: c1,
		partitions: map[UniqueID]*partitionInfos{
			101: parsePartitionsInfo([]*partitionInfo{
				{name: Params.CommonCfg.DefaultPartitionName.GetValue(), partitionID: 1000},
				{name: "p1", partitionID: 1001},
			}, false),
			202: parsePartitionsInfo([]*partitionInfo{{name: "p2", partitionID: 2001}}, false),
		},
	}
	oldCache := globalMetaCache
	globalMetaCache = cache
	t.Cleanup(func() {
		globalMetaCache = oldCache
	})

	ctx, snapshot, err := ensureReadRequestSnapshot(context.Background(), "db", "alias")
	require.NoError(t, err)
	require.Equal(t, UniqueID(101), snapshot.Collection().CollectionID())
	require.Equal(t, "collection_1", snapshot.Collection().CanonicalName())
	require.Same(t, c1.schema, snapshot.Collection().Schema())

	// Simulate AlterAlias after this request's linearization point.
	cache.current = c2
	schema, err := GetCachedCollectionSchema(ctx, "db", "alias")
	require.NoError(t, err)
	require.Same(t, c1.schema, schema)

	ctx, sameSnapshot, err := ensureReadRequestSnapshot(ctx, "db", "alias")
	require.NoError(t, err)
	require.Same(t, snapshot, sameSnapshot)
	require.Equal(t, 1, cache.infoCalls)

	request := &milvuspb.SearchRequest{
		DbName:         "db",
		CollectionName: "alias",
		PartitionNames: []string{"p1"},
		Nq:             3,
	}
	dbID, collectionToPartitions, rateType, n, err := GetRequestInfo(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(10), dbID)
	require.Equal(t, map[int64][]int64{101: {1001}}, collectionToPartitions)
	require.Equal(t, internalpb.RateType_DQLSearch, rateType)
	require.Equal(t, 3, n)
	require.Equal(t, []UniqueID{101}, cache.partitionCalls)
	require.Equal(t, 1, cache.infoCalls)
	defaultPartition, err := snapshot.PartitionInfo(ctx, "")
	require.NoError(t, err)
	require.Equal(t, UniqueID(1000), defaultPartition.partitionID)
	require.Equal(t, []UniqueID{101}, cache.partitionCalls)

	search := &searchTask{
		request: &milvuspb.SearchRequest{
			DbName:                "db",
			CollectionName:        "alias",
			UseDefaultConsistency: true,
		},
		readSnapshot: snapshot,
	}
	query := &queryTask{
		request: &milvuspb.QueryRequest{
			DbName:                "db",
			CollectionName:        "alias",
			UseDefaultConsistency: true,
		},
		readSnapshot: snapshot,
	}
	require.False(t, search.CanSkipAllocTimestamp())
	require.False(t, query.CanSkipAllocTimestamp())
	require.Equal(t, 1, cache.infoCalls)

	snapshot.PinTimestamp(commonpb.ConsistencyLevel_Strong, 11111, 12345)
	snapshot.PinTimestamp(commonpb.ConsistencyLevel_Bounded, 22222, 67890)
	consistencyLevel, requestTS, guaranteeTS, pinned := snapshot.GetPinnedTimestamp()
	require.True(t, pinned)
	require.Equal(t, commonpb.ConsistencyLevel_Strong, consistencyLevel)
	require.Equal(t, Timestamp(11111), requestTS)
	require.Equal(t, Timestamp(12345), guaranteeTS)
	require.True(t, search.CanSkipAllocTimestamp())
	require.True(t, query.CanSkipAllocTimestamp())

	_, _, err = ensureReadRequestSnapshot(ctx, "db", "another_alias")
	require.Error(t, err)
	require.Equal(t, 1, cache.infoCalls)
	_, _, _, _, err = GetRequestInfo(ctx, &milvuspb.SearchRequest{
		DbName:         "db",
		CollectionName: "another_alias",
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, 1, cache.infoCalls)

	mismatchedTask := &searchTask{
		SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}},
		request: &milvuspb.SearchRequest{
			DbName:         "db",
			CollectionName: "another_alias",
		},
		readSnapshot: snapshot,
	}
	require.ErrorIs(t, mismatchedTask.PreExecute(context.Background()), merr.ErrServiceInternal)
	require.Equal(t, 1, cache.infoCalls)

	_, nextSnapshot, err := ensureReadRequestSnapshot(context.Background(), "db", "alias")
	require.NoError(t, err)
	require.Equal(t, UniqueID(202), nextSnapshot.Collection().CollectionID())
	require.Equal(t, "collection_2", nextSnapshot.Collection().CanonicalName())
	require.Equal(t, 2, cache.infoCalls)
}

func TestReadRequestSnapshotRejectsIncompleteMetadata(t *testing.T) {
	tests := map[string]*collectionInfo{
		"missing schema":        {},
		"missing collection id": {schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Name: "collection"})},
	}
	for name, info := range tests {
		t.Run(name, func(t *testing.T) {
			cache := &readSnapshotTestCache{current: info}
			oldCache := globalMetaCache
			globalMetaCache = cache
			t.Cleanup(func() {
				globalMetaCache = oldCache
			})

			_, snapshot, err := ensureReadRequestSnapshot(context.Background(), "db", "alias")
			require.Nil(t, snapshot)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, 1, cache.infoCalls)
		})
	}
}

func TestReadRequestSnapshotRetriesPartitionFetchAfterTransientFailure(t *testing.T) {
	collection := newAliasSnapshotCollectionInfo(10, 101, "collection", commonpb.ConsistencyLevel_Strong)
	cache := &readSnapshotTestCache{
		current: collection,
		partitions: map[UniqueID]*partitionInfos{
			101: parsePartitionsInfo([]*partitionInfo{{name: "p1", partitionID: 1001}}, false),
		},
		partitionErrs: []error{merr.WrapErrServiceUnavailable("proxy", "transient partition fetch failure")},
	}
	oldCache := globalMetaCache
	globalMetaCache = cache
	t.Cleanup(func() {
		globalMetaCache = oldCache
	})

	ctx, snapshot, err := ensureReadRequestSnapshot(context.Background(), "db", "alias")
	require.NoError(t, err)

	_, err = snapshot.Partitions(ctx)
	require.ErrorContains(t, err, "transient partition fetch failure")

	partitions, err := snapshot.Partitions(ctx)
	require.NoError(t, err)
	require.Equal(t, UniqueID(1001), partitions.name2ID["p1"])
	require.Equal(t, []UniqueID{101, 101}, cache.partitionCalls)

	_, err = snapshot.Partitions(ctx)
	require.NoError(t, err)
	require.Equal(t, []UniqueID{101, 101}, cache.partitionCalls)
}
