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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRestoringCollectionCannotDrop(t *testing.T) {
	mockey.PatchConvey("reject public admission and replay before any durable drop", t, func() {
		channel.ResetStaticPChannelStatsManager()
		channel.RecoverPChannelStatsManager(nil)
		defer channel.ResetStaticPChannelStatsManager()
		collection := &model.Collection{
			CollectionID: 10, DBID: util.DefaultDBID, DBName: util.DefaultDBName, Name: "restored",
			State: etcdpb.CollectionState_CollectionRestoring, FileResourceIds: []int64{99},
		}
		mt := &MetaTable{
			catalog:     &kvrootcoord.Catalog{},
			dbName2Meta: map[string]*model.Database{util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName}},
			collID2Meta: map[int64]*model.Collection{10: collection}, fileResourceRefCnt: map[int64]int{99: 1},
			names: newNameDb(), aliases: newNameDb(),
		}
		mt.names.createDbIfNotExist(util.DefaultDBName)
		mt.aliases.createDbIfNotExist(util.DefaultDBName)
		mt.names.insert(util.DefaultDBName, collection.Name, collection.CollectionID)
		mt.rebuildAvailableCollectionCountLocked()
		var persisted []etcdpb.CollectionState
		alter := mockey.Mock((*kvrootcoord.Catalog).AlterCollection).To(func(_ *kvrootcoord.Catalog, _ context.Context, _, next *model.Collection, _ metastore.AlterType, _ typeutil.Timestamp, _ bool) error {
			persisted = append(persisted, next.State)
			return nil
		}).Build()
		mockey.Mock((*kvrootcoord.Catalog).DeleteGrantByCollectionName).Return(nil).Build()
		task := &dropCollectionTask{Core: &Core{meta: mt}, Req: &milvuspb.DropCollectionRequest{DbName: util.DefaultDBName, CollectionName: collection.Name}}
		require.ErrorIs(t, task.Prepare(t.Context()), merr.ErrServiceNotReady)
		require.Nil(t, task.header, "rejected admission must not produce a Drop recipe")
		require.ErrorIs(t, mt.DropCollection(t.Context(), 10, 200), merr.ErrServiceNotReady, "old pending Drop callbacks must obey the same state rule")
		require.Zero(t, alter.Times())
		require.Equal(t, etcdpb.CollectionState_CollectionRestoring, mt.collID2Meta[10].State)
		require.Equal(t, 1, mt.fileResourceRefCnt[99])
		require.Equal(t, 1, mt.availableCollectionCount)
		require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
		require.NoError(t, task.Prepare(t.Context()))
		require.EqualValues(t, 10, task.header.CollectionId)
		require.NoError(t, mt.DropCollection(t.Context(), 10, 300))
		require.NoError(t, mt.DropCollection(t.Context(), 10, 300))
		require.Equal(t, []etcdpb.CollectionState{etcdpb.CollectionState_CollectionCreated, etcdpb.CollectionState_CollectionDropping}, persisted)
		require.Equal(t, etcdpb.CollectionState_CollectionDropping, mt.collID2Meta[10].State)
		require.Zero(t, mt.fileResourceRefCnt[99])
		require.Zero(t, mt.availableCollectionCount)
	})
}

func TestRenameRespectsRestoringNameReservation(t *testing.T) {
	for _, destination := range []string{util.DefaultDBName, "other"} {
		for _, state := range []etcdpb.CollectionState{
			etcdpb.CollectionState_CollectionRestoring, etcdpb.CollectionState_CollectionCreated,
			etcdpb.CollectionState_CollectionDropping, etcdpb.CollectionState_CollectionDropped,
		} {
			t.Run(destination+"/"+state.String(), func(t *testing.T) {
				mt := &MetaTable{
					dbName2Meta: map[string]*model.Database{
						util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName}, "other": {ID: 2, Name: "other"},
					},
					collID2Meta: map[int64]*model.Collection{
						10: {CollectionID: 10, DBID: util.DefaultDBID, DBName: destination, Name: "target", State: state},
						20: {CollectionID: 20, DBID: util.DefaultDBID, DBName: util.DefaultDBName, Name: "source", State: etcdpb.CollectionState_CollectionCreated},
					},
					names: newNameDb(), aliases: newNameDb(),
				}
				if destination == "other" {
					mt.collID2Meta[10].DBID = 2
				}
				for _, db := range []string{util.DefaultDBName, "other"} {
					mt.names.createDbIfNotExist(db)
					mt.aliases.createDbIfNotExist(db)
				}
				mt.names.insert(destination, "target", 10)
				mt.names.insert(util.DefaultDBName, "source", 20)
				err := mt.CheckIfCollectionRenamable(t.Context(), util.DefaultDBName, "source", destination, "target")
				if state == etcdpb.CollectionState_CollectionRestoring || state == etcdpb.CollectionState_CollectionCreated {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
				} else {
					require.NoError(t, err, "dropped names retain their existing reuse behavior")
				}
				require.NoError(t, mt.CheckIfCollectionRenamable(t.Context(), util.DefaultDBName, "source", destination, "free"))
				require.EqualValues(t, 10, mt.GetCollectionID(t.Context(), destination, "target"))
			})
		}
	}
}
