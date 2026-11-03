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
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRestoredFileResourceReservationSurvivesRecovery(t *testing.T) {
	for _, createdBeforeRestart := range []bool{false, true} {
		t.Run(strconv.FormatBool(createdBeforeRestart), func(t *testing.T) {
			mockey.PatchConvey("pending Restore reserves resources until collection drop", t, func() {
				channel.ResetStaticPChannelStatsManager()
				channel.RecoverPChannelStatsManager(nil)
				defer channel.ResetStaticPChannelStatsManager()
				resource := &internalpb.FileResourceInfo{Id: 99, Name: "dict", Path: "dict.txt"}
				mt := &MetaTable{
					catalog:     &kvrootcoord.Catalog{},
					dbName2Meta: map[string]*model.Database{util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName}},
					collID2Meta: make(map[int64]*model.Collection), partitionName2ID: make(map[int64]map[string]int64),
					fileResourceID2Meta:   map[int64]*internalpb.FileResourceInfo{99: resource},
					fileResourceName2Meta: map[string]*internalpb.FileResourceInfo{"dict": resource},
					fileResourceRefCnt:    make(map[int64]int), names: newNameDb(), aliases: newNameDb(),
				}
				mt.names.createDbIfNotExist(util.DefaultDBName)
				collection := &model.Collection{
					CollectionID: 10, DBID: util.DefaultDBID, DBName: util.DefaultDBName, Name: "restored",
					State: etcdpb.CollectionState_CollectionRestoring, FileResourceIds: []int64{99},
				}
				if createdBeforeRestart {
					// Collection reload already accounted for this same resource.
					mt.collID2Meta[10] = collection
					mt.fileResourceRefCnt[99] = 1
					mt.names.insert(util.DefaultDBName, collection.Name, 10)
				}
				mt.rebuildAvailableCollectionCountLocked()
				mockey.Mock((*kvrootcoord.Catalog).CreateCollection).Return(nil).Build()
				mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(nil).Build()
				mockey.Mock((*kvrootcoord.Catalog).DeleteGrantByCollectionName).Return(nil).Build()
				removed := mockey.Mock((*kvrootcoord.Catalog).RemoveFileResource).Return(nil).Build()
				// This is the collection-ID/resource-ID union recovered by Broadcaster.
				mt.RecoverFileResourceRefCnt(map[int64][]int64{10: {99}})
				require.Equal(t, 1, mt.fileResourceRefCnt[99], "pending and persisted ownership count once")
				err, changed := mt.RemoveFileResource(t.Context(), "dict")
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.False(t, changed)
				require.Zero(t, removed.Times())
				require.NoError(t, mt.AddCollection(t.Context(), collection))
				require.Equal(t, 1, mt.fileResourceRefCnt[99], "callback replay must not acquire a second reference")
				require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
				require.NoError(t, mt.DropCollection(t.Context(), 10, 300))
				require.Zero(t, mt.fileResourceRefCnt[99])
				err, changed = mt.RemoveFileResource(t.Context(), "dict")
				require.NoError(t, err)
				require.True(t, changed)
				require.Equal(t, 1, removed.Times())
			})
		})
	}
}
