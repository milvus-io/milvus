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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	streamingbroadcaster "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
)

func TestBroadcastCreateCollectionV1RollbackFileResourcesWhenTaskNotCreated(t *testing.T) {
	mockey.PatchConvey("rollback create collection file resources when broadcast task is not created", t, func() {
		const (
			dbName         = "default"
			collectionName = "test_collection"
		)
		heldFileResourceIDs := []int64{1001}
		schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{Name: collectionName})
		require.NoError(t, err)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().DecFileResourceRefCnt(heldFileResourceIDs).Once()
		core := newTestCore(withMeta(meta))

		broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).
			Return(nil, streamingbroadcaster.ErrBroadcastTaskNotCreated).Once()
		broadcastAPI.EXPECT().Close().Return().Once()

		mockey.Mock((*Core).startBroadcastWithCollectionLock).Return(broadcastAPI, nil).Build()
		mockey.Mock((*createCollectionTask).Prepare).To(func(task *createCollectionTask, ctx context.Context) error {
			task.Req.ShardsNum = 1
			task.body.VirtualChannelNames = []string{"by-dev-rootcoord-dml_0_100v0"}
			task.heldFileResourceIds = heldFileResourceIDs
			return nil
		}).Build()

		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0").Once()
		streaming.SetWALForTest(wal)
		defer streaming.SetWALForTest(nil)

		err = core.broadcastCreateCollectionV1(context.Background(), &milvuspb.CreateCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			ShardsNum:      1,
			Schema:         schemaBytes,
		})
		require.ErrorIs(t, err, streamingbroadcaster.ErrBroadcastTaskNotCreated)
	})
}
