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

// These collection meta tests exercise the full write -> persist -> reload
// cycle against a real etcd-backed catalog, so they live in internal/ next to
// the etcd catalog implementation rather than in the shared pkg module.
package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMetaTable_TruncateCollection(t *testing.T) {
	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10) + "/meta"
	catalogKV := etcdkv.NewEtcdKV(kv, path)
	catalog := rootcoord.NewCatalog(catalogKV)

	allocator := mocktso.NewAllocator(t)
	allocator.EXPECT().GenerateTSO(mock.Anything).Return(1000, nil)

	channel.ResetStaticPChannelStatsManager()
	meta, err := NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)

	err = meta.AddCollection(context.Background(), &model.Collection{
		CollectionID:         1,
		PhysicalChannelNames: []string{"pchannel1"},
		VirtualChannelNames:  []string{"vchannel1"},
		State:                pb.CollectionState_CollectionCreated,
		DBID:                 util.DefaultDBID,
		Properties:           common.NewKeyValuePairs(map[string]string{}),
		ShardInfos: map[string]*model.ShardInfo{
			"vchannel1": {
				VChannelName:         "vchannel1",
				PChannelName:         "pchannel1",
				LastTruncateTimeTick: 0,
			},
		},
	})
	require.NoError(t, err)

	// begin truncate collection
	err = meta.BeginTruncateCollection(context.Background(), 1)
	require.NoError(t, err)
	coll, err := meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m := common.CloneKeyValuePairs(coll.Properties).ToMap()
	require.Equal(t, "1", m[common.CollectionOnTruncatingKey])
	require.Equal(t, uint64(0), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)

	// reload the meta
	channel.ResetStaticPChannelStatsManager()
	meta, err = NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)
	coll, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m = common.CloneKeyValuePairs(coll.Properties).ToMap()
	require.Equal(t, "1", m[common.CollectionOnTruncatingKey])
	require.Equal(t, uint64(0), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)

	// remove the temp property
	b := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast(coll.VirtualChannelNames, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()

	meta.TruncateCollection(context.Background(), message.BroadcastResultTruncateCollectionMessageV2{
		Message: message.MustAsBroadcastTruncateCollectionMessageV2(b),
		Results: map[string]*message.AppendResult{
			"vchannel1": {
				TimeTick: 1000,
			},
		},
	})

	require.NoError(t, err)
	coll, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m = common.CloneKeyValuePairs(coll.Properties).ToMap()
	_, ok := m[common.CollectionOnTruncatingKey]
	require.False(t, ok)
	require.Equal(t, uint64(1000), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)

	// reload the meta again
	channel.ResetStaticPChannelStatsManager()
	meta, err = NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)
	coll, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m = common.CloneKeyValuePairs(coll.Properties).ToMap()
	_, ok = m[common.CollectionOnTruncatingKey]
	require.False(t, ok)
	require.Equal(t, 1, len(coll.ShardInfos))
	require.Equal(t, uint64(1000), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)
}

func TestMetaTableReloadNormalizesMaxFieldIDProperty(t *testing.T) {
	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10) + "/meta"
	catalogKV := etcdkv.NewEtcdKV(kv, path)
	catalog := rootcoord.NewCatalog(catalogKV)

	allocator := mocktso.NewAllocator(t)
	allocator.EXPECT().GenerateTSO(mock.Anything).Return(1000, nil)

	channel.ResetStaticPChannelStatsManager()
	meta, err := NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)

	err = meta.AddCollection(context.Background(), &model.Collection{
		CollectionID:         1,
		DBID:                 util.DefaultDBID,
		DBName:               util.DefaultDBName,
		Name:                 "test_reload_max_field_id",
		PhysicalChannelNames: []string{"pchannel1"},
		VirtualChannelNames:  []string{"vchannel1"},
		State:                pb.CollectionState_CollectionCreated,
		Fields: []*model.Field{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			{FieldID: 105, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
		Properties: common.NewKeyValuePairs(map[string]string{
			common.CollectionReplicaNumber: "1",
		}),
		ShardInfos: map[string]*model.ShardInfo{
			"vchannel1": {
				VChannelName:         "vchannel1",
				PChannelName:         "pchannel1",
				LastTruncateTimeTick: 0,
			},
		},
	})
	require.NoError(t, err)

	channel.ResetStaticPChannelStatsManager()
	meta, err = NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)

	coll, err := meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	props := common.CloneKeyValuePairs(coll.Properties).ToMap()
	require.Equal(t, "105", props[common.MaxFieldIDKey])
}
