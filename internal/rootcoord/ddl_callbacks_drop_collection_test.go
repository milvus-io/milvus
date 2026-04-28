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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBroadcastDropCollectionV1AckSyncUp(t *testing.T) {
	testCases := []struct {
		name            string
		fileResourceIDs []int64
		wantAckSyncUp   bool
	}{
		{
			name:          "collection without file resources uses default ack",
			wantAckSyncUp: false,
		},
		{
			name:            "collection with file resources syncs up ack",
			fileResourceIDs: []int64{1001},
			wantAckSyncUp:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			core := initStreamingSystemAndCore(t)
			t.Cleanup(func() {
				require.NoError(t, core.Stop())
			})
			t.Cleanup(broadcast.ResetBroadcaster)

			dbName := "db1"
			collectionName := funcutil.GenRandomStr()
			meta := mockrootcoord.NewIMetaTable(t)
			meta.EXPECT().IsAlias(mock.Anything, dbName, collectionName).Return(false)
			meta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return(&model.Collection{
				CollectionID:        1,
				DBName:              dbName,
				DBID:                1,
				State:               etcdpb.CollectionState_CollectionCreated,
				VirtualChannelNames: []string{"vchannel1"},
				FileResourceIds:     tc.fileResourceIDs,
			}, nil)
			meta.EXPECT().ListAliasesByID(mock.Anything, int64(1)).Return([]string{})
			core.meta = meta

			broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
			broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.MatchedBy(func(msg message.BroadcastMutableMessage) bool {
				return msg.BroadcastHeader().AckSyncUp == tc.wantAckSyncUp
			})).Return(&types.BroadcastAppendResult{}, nil).Once()
			broadcastAPI.EXPECT().Close().Return().Once()

			broadcaster := mock_broadcaster.NewMockBroadcaster(t)
			broadcaster.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).Return(broadcastAPI, nil).Once()
			broadcaster.EXPECT().Close().Return().Maybe()
			broadcast.ResetBroadcaster()
			broadcast.Register(broadcaster)

			err := core.broadcastDropCollectionV1(context.Background(), &milvuspb.DropCollectionRequest{
				DbName:         dbName,
				CollectionName: collectionName,
			})
			require.NoError(t, err)
		})
	}
}
