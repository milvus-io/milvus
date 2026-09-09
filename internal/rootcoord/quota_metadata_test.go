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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func quotaMetaForTest(collections int) *MetaTable {
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"quota": {ID: 10, Name: "quota"},
			"empty": {ID: 20, Name: "empty"},
		},
		collID2Meta: make(map[int64]*model.Collection, collections),
	}
	for i := range collections {
		id := int64(i + 100)
		mt.collID2Meta[id] = &model.Collection{
			DBID: 10, CollectionID: id, State: etcdpb.CollectionState_CollectionCreated,
			Partitions: []*model.Partition{
				{PartitionID: id * 10, State: etcdpb.PartitionState_PartitionCreated},
				{PartitionID: id*10 + 1, State: etcdpb.PartitionState_PartitionDropping},
			},
		}
	}
	return mt
}

func TestQuotaMetadataProjection(t *testing.T) {
	mt := quotaMetaForTest(3)
	mt.collID2Meta[101].State = etcdpb.CollectionState_CollectionDropping
	mt.collID2Meta[102].DBID = util.NonDBID
	mt.collID2Meta[100].Properties = []*commonpb.KeyValuePair{
		{Key: "key", Value: "first"}, {Key: "key", Value: "last"},
	}
	ctx := context.Background()
	full := mt.ListAllAvailPartitions(ctx)
	require.Equal(t, []int64{1000, 1001}, full[10][100])
	require.Contains(t, full[util.DefaultDBID], int64(102))
	require.NotContains(t, full[10], int64(101))
	require.Empty(t, full[20])
	projected := mt.ListQuotaPartitions(ctx, false)
	for dbID, collections := range full {
		require.Len(t, projected[dbID], len(collections))
		for collectionID := range collections {
			require.Contains(t, projected[dbID], collectionID)
			require.Nil(t, projected[dbID][collectionID])
		}
	}
	props, err := mt.GetQuotaCollectionProperties(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"key": "last"}, props)
	props["key"] = "changed"
	require.Equal(t, "last", mt.collID2Meta[100].Properties[1].Value)
	empty, err := mt.GetQuotaCollectionProperties(ctx, 102)
	require.NoError(t, err)
	require.Nil(t, empty)
	for _, id := range []int64{101, 999} {
		_, err = mt.GetQuotaCollectionProperties(ctx, id)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	}
}

// The wrapper intentionally exposes only IMetaTable, exercising the fallback.
type legacyQuotaMeta struct{ IMetaTable }

func TestQuotaMetadataFallbackAndRefresh(t *testing.T) {
	mt := quotaMetaForTest(1)
	mt.collID2Meta[100].Properties = []*commonpb.KeyValuePair{{Key: "rate", Value: "10"}}
	for _, meta := range []IMetaTable{mt, legacyQuotaMeta{mt}} {
		q := NewQuotaCenter(nil, nil, nil, meta)
		t.Cleanup(q.cancel)
		mt.collID2Meta[100].Properties[0].Value = "10"
		require.Equal(t, map[string]string{"rate": "10"}, q.getCollectionLimitProperties(100))
		mt.collID2Meta[100].Properties[0].Value = "20"
		require.Equal(t, "10", q.getCollectionLimitProperties(100)["rate"])
		q.clearMetrics()
		require.Equal(t, "20", q.getCollectionLimitProperties(100)["rate"])
		require.Equal(t, mt.ListAllAvailPartitions(q.ctx), q.quotaPartitionSnapshot(true))
		require.Nil(t, q.getCollectionLimitProperties(999))
		require.NotContains(t, q.collectionProps, int64(999))
		mt.collID2Meta[999] = &model.Collection{
			CollectionID: 999, State: etcdpb.CollectionState_CollectionCreated,
			Properties: []*commonpb.KeyValuePair{{Key: "rate", Value: "30"}},
		}
		require.Equal(t, "30", q.getCollectionLimitProperties(999)["rate"])
		delete(mt.collID2Meta, 999)
	}
}
