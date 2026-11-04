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

package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/externalrefresh"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExternalRefreshBroadcastFences(t *testing.T) {
	mockey.PatchConvey("all target channels and errors", t, func() {
		api := &embeddedBroadcastAPI{}
		mockey.Mock((*ddl.Callbacks).StartBroadcastWithCollectionID).Return(api, nil).Build()
		mockey.Mock((*embeddedBroadcastAPI).Close).Return().Build()
		mockey.Mock((*externalrefresh.Manager).GetActiveJobByCollectionID).Return(nil).Build()
		mockey.Mock((*ServerHandler).GetCollection).Return(&meta.CollectionInfo{VChannelNames: []string{"v1", "v2"}}, nil).Build()
		mockey.Mock((*refreshBroadcastAllocator).AllocID).Return(int64(42), nil).Build()
		broadcastErr := error(nil)
		calls := 0
		mockey.Mock((*embeddedBroadcastAPI).Broadcast).To(func(_ *embeddedBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			calls++
			require.ElementsMatch(t, []string{"v1", "v2"}, msg.BroadcastHeader().VChannels)
			require.True(t, msg.BroadcastHeader().AckSyncUp)
			return &types.BroadcastAppendResult{}, broadcastErr
		}).Build()
		server := &Server{handler: &ServerHandler{}, allocator: &refreshBroadcastAllocator{}, externalCollectionRefreshManager: &externalrefresh.Manager{}}
		server.ddl = newDDLCallbacks(server)
		server.stateCode.Store(commonpb.StateCode_Healthy)
		request := &datapb.RefreshExternalCollectionRequest{CollectionId: 100}
		response, err := server.RefreshExternalCollection(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, merr.Error(response.GetStatus()))
		require.Equal(t, int64(42), response.GetJobId())
		broadcastErr = merr.ErrServiceUnavailable
		response, err = server.RefreshExternalCollection(t.Context(), request)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrServiceUnavailable)
		require.Equal(t, 2, calls)
	})
}

type refreshBroadcastAllocator struct{ allocator.Allocator }

func (*refreshBroadcastAllocator) AllocID(context.Context) (int64, error) {
	panic("AllocID must be patched")
}
