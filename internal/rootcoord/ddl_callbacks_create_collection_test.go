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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	streamingbroadcaster "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBroadcastCreateCollectionV1RollbackFileResourcesWhenTaskNotCreated(t *testing.T) {
	mockey.PatchConvey("rollback create collection file resources when broadcast task is not created", t, func() {
		const (
			dbName         = "default"
			collectionName = "test_collection"
		)
		heldFileResourceIDs := []int64{1001}
		schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{Name: "embedding", DataType: schemapb.DataType_FloatVector},
			},
		})
		require.NoError(t, err)

		meta := mockrootcoord.NewIMetaTable(t)
		db := model.NewDefaultDatabase(nil)
		meta.EXPECT().GetDatabaseByName(mock.Anything, dbName, mock.Anything).Return(db, nil)
		meta.EXPECT().GetAvailableCollectionCount(mock.Anything, db.ID).Return(0, 0, true)
		meta.EXPECT().GetGeneralCount(mock.Anything).Return(0)
		meta.EXPECT().DescribeAlias(mock.Anything, dbName, collectionName, mock.Anything).Return("", merr.ErrAliasNotFound)
		meta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound)
		reserved := meta.EXPECT().IncFileResourceRefCnt(heldFileResourceIDs).Return(nil).Once()
		meta.EXPECT().DecFileResourceRefCnt(heldFileResourceIDs).NotBefore(reserved).Once()
		core := newTestCore(withMeta(meta), withValidIDAllocator())

		broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).
			Return(nil, streamingbroadcaster.ErrBroadcastTaskNotCreated).Once()
		broadcastAPI.EXPECT().Close().Return().Once()

		mockey.Mock((*Core).startBroadcastWithCollectionLock).Return(broadcastAPI, nil).Build()
		mockey.Mock((*Core).validateSchemaAnalyzerFileResources).Return(heldFileResourceIDs, nil).Build()
		mockey.Mock((*snmanager.StreamingNodeManager).AllocVirtualChannels).Return([]string{"by-dev-rootcoord-dml_0_100v0"}, nil).Build()

		err = core.broadcastCreateCollectionV1(t.Context(), &milvuspb.CreateCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			ShardsNum:      1,
			Schema:         schemaBytes,
		})
		require.ErrorIs(t, err, streamingbroadcaster.ErrBroadcastTaskNotCreated)
	})
}
