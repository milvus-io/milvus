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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A rename tells the proxies it is a rename, so they delete the metric series
// labeled with the old name; every other alteration stays AlterCollection. The
// aliases move with the collection, so theirs stay AlterAlias either way.
func TestCacheExpireForCollectionCarriesTheMessageType(t *testing.T) {
	ctx := context.Background()
	core := newTestCore(withHealthyCode())
	meta := mockrootcoord.NewIMetaTable(t)
	core.meta = meta
	meta.EXPECT().GetCollectionByName(mock.Anything, "db", "old", typeutil.MaxTimestamp, false).
		Return(&model.Collection{CollectionID: 7, Name: "old"}, nil)
	meta.EXPECT().ListAliases(mock.Anything, "db", "old", typeutil.MaxTimestamp).Return([]string{"alias"}, nil)

	msgTypes := func(t *testing.T, msgType commonpb.MsgType) []commonpb.MsgType {
		expirations, err := core.getCacheExpireForCollectionWithMsgType(ctx, "db", "old", msgType)
		require.NoError(t, err)
		types := make([]commonpb.MsgType, 0)
		for _, expiration := range expirations.GetCacheExpirations() {
			types = append(types, expiration.GetLegacyProxyCollectionMetaCache().GetMsgType())
		}
		return types
	}
	require.Equal(t, []commonpb.MsgType{commonpb.MsgType_RenameCollection, commonpb.MsgType_AlterAlias},
		msgTypes(t, commonpb.MsgType_RenameCollection))

	expirations, err := core.getCacheExpireForCollection(ctx, "db", "old")
	require.NoError(t, err)
	require.Equal(t, commonpb.MsgType_AlterCollection,
		expirations.GetCacheExpirations()[0].GetLegacyProxyCollectionMetaCache().GetMsgType())
}
