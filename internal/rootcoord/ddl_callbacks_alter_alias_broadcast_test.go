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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// These tests audit the PRODUCTION side of AlterAliasMessageHeader.OldCollectionId
// (the ack-callback consumer is covered by
// TestDDLCallbacksAlterAliasV2AckCallback_OldTargetRouting). They mock the
// broadcast plumbing and capture the built message to assert which sentinel the
// broadcaster stamps: CreateAlias => aliasNoOldTarget (-1), AlterAlias with a
// resolvable pre-alter target => its real id (>0), AlterAlias whose target
// cannot be resolved => 0 (the safe holder-scan path, never a failed alter).

// TestBroadcastCreateAlias_MarksNoOldTargetSentinel: a create provably has no
// pre-existing target, so the header must carry the aliasNoOldTarget sentinel to
// keep the ack callback off the O(N) holder scan.
func TestBroadcastCreateAlias_MarksNoOldTargetSentinel(t *testing.T) {
	mockey.PatchConvey("create alias stamps aliasNoOldTarget", t, func() {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().CheckIfAliasCreatable(mock.Anything, "db", "a", "coll").Return(nil)
		meta.EXPECT().GetDatabaseByName(mock.Anything, "db", mock.Anything).Return(&model.Database{ID: 3, Name: "db"}, nil)
		meta.EXPECT().GetCollectionByName(mock.Anything, "db", "coll", mock.Anything, false).
			Return(&model.Collection{CollectionID: 20, Name: "coll"}, nil)
		core := newTestCore(withMeta(meta))

		var captured message.BroadcastMutableMessage
		broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, msg message.BroadcastMutableMessage) { captured = msg }).
			Return(nil, nil).Once()
		broadcastAPI.EXPECT().Close().Return().Once()
		mockey.Mock(startBroadcastWithDatabaseLock).Return(broadcastAPI, nil).Build()

		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0").Once()
		streaming.SetWALForTest(wal)
		defer streaming.SetWALForTest(nil)

		err := core.broadcastCreateAlias(context.Background(), &milvuspb.CreateAliasRequest{
			DbName:         "db",
			Alias:          "a",
			CollectionName: "coll",
		})
		require.NoError(t, err)
		require.NotNil(t, captured)
		header := message.MustAsBroadcastAlterAliasMessageV2(captured).Header()
		require.Equal(t, aliasNoOldTarget, header.OldCollectionId, "CreateAlias must stamp the no-old-target sentinel")
		require.Equal(t, int64(20), header.CollectionId)
	})
}

// TestBroadcastAlterAlias_ForwardsResolvedOldTarget: an alter whose pre-alter
// alias target resolves must forward that id so the proxy evicts the old target
// by id (no scan).
func TestBroadcastAlterAlias_ForwardsResolvedOldTarget(t *testing.T) {
	mockey.PatchConvey("alter alias forwards the resolved old target id", t, func() {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().CheckIfAliasAlterable(mock.Anything, "db", "a", "coll").Return(nil)
		meta.EXPECT().GetDatabaseByName(mock.Anything, "db", mock.Anything).Return(&model.Database{ID: 3, Name: "db"}, nil)
		// the new target (by collection name)
		meta.EXPECT().GetCollectionByName(mock.Anything, "db", "coll", mock.Anything, false).
			Return(&model.Collection{CollectionID: 20, Name: "coll"}, nil)
		// the pre-alter target (by alias name, allowUnavailable=true)
		meta.EXPECT().GetCollectionByName(mock.Anything, "db", "a", mock.Anything, true).
			Return(&model.Collection{CollectionID: 10, Name: "oldcoll"}, nil)
		core := newTestCore(withMeta(meta))

		var captured message.BroadcastMutableMessage
		broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, msg message.BroadcastMutableMessage) { captured = msg }).
			Return(nil, nil).Once()
		broadcastAPI.EXPECT().Close().Return().Once()
		mockey.Mock(startBroadcastWithDatabaseLock).Return(broadcastAPI, nil).Build()

		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0").Once()
		streaming.SetWALForTest(wal)
		defer streaming.SetWALForTest(nil)

		err := core.broadcastAlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
			DbName:         "db",
			Alias:          "a",
			CollectionName: "coll",
		})
		require.NoError(t, err)
		require.NotNil(t, captured)
		header := message.MustAsBroadcastAlterAliasMessageV2(captured).Header()
		require.Equal(t, int64(10), header.OldCollectionId, "the resolved pre-alter target id must be forwarded")
		require.Equal(t, int64(20), header.CollectionId)
	})
}

// TestBroadcastAlterAlias_LeavesZeroWhenOldTargetUnresolvable: if the pre-alter
// target cannot be resolved (a meta inconsistency), the alter must NOT fail; it
// leaves OldCollectionId at 0 so the ack callback falls back to the proxy holder
// scan (0 is the safe-scan path, distinct from the -1 CreateAlias sentinel).
func TestBroadcastAlterAlias_LeavesZeroWhenOldTargetUnresolvable(t *testing.T) {
	mockey.PatchConvey("alter alias leaves zero when the old target is unresolvable", t, func() {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().CheckIfAliasAlterable(mock.Anything, "db", "a", "coll").Return(nil)
		meta.EXPECT().GetDatabaseByName(mock.Anything, "db", mock.Anything).Return(&model.Database{ID: 3, Name: "db"}, nil)
		meta.EXPECT().GetCollectionByName(mock.Anything, "db", "coll", mock.Anything, false).
			Return(&model.Collection{CollectionID: 20, Name: "coll"}, nil)
		// the pre-alter target cannot be resolved
		meta.EXPECT().GetCollectionByName(mock.Anything, "db", "a", mock.Anything, true).
			Return(nil, errors.New("meta inconsistency"))
		core := newTestCore(withMeta(meta))

		var captured message.BroadcastMutableMessage
		broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
		broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, msg message.BroadcastMutableMessage) { captured = msg }).
			Return(nil, nil).Once()
		broadcastAPI.EXPECT().Close().Return().Once()
		mockey.Mock(startBroadcastWithDatabaseLock).Return(broadcastAPI, nil).Build()

		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0").Once()
		streaming.SetWALForTest(wal)
		defer streaming.SetWALForTest(nil)

		err := core.broadcastAlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
			DbName:         "db",
			Alias:          "a",
			CollectionName: "coll",
		})
		require.NoError(t, err, "an unresolvable old target must NOT fail the alter")
		require.NotNil(t, captured)
		header := message.MustAsBroadcastAlterAliasMessageV2(captured).Header()
		require.Equal(t, int64(0), header.OldCollectionId, "an unresolvable old target must leave 0 (safe-scan path)")
	})
}
