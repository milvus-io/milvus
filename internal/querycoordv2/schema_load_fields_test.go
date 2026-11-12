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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchemaLoadFieldsWaitsForQueryCoordRecovery(t *testing.T) {
	paramtable.Init()
	ctx := t.Context()
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	manager := meta.NewCollectionManager(catalog)
	server := &Server{ctx: ctx, meta: &meta.Meta{CollectionManager: manager}}
	previous := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{FieldID: 100}}}
	target := &schemapb.CollectionSchema{Version: 2, Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
	for _, state := range []commonpb.StateCode{commonpb.StateCode_Abnormal, commonpb.StateCode_StandBy, commonpb.StateCode_Initializing} {
		server.UpdateStateCode(state)
		require.ErrorIs(t, server.UpdateLoadFieldsForSchema(ctx, 10, previous, target), merr.ErrServiceNotReady)
	}
	// initMeta has assigned server.meta, but Recover has not populated it.
	catalog.EXPECT().GetCollections(mock.Anything).Run(func(context.Context) {
		require.Nil(t, manager.GetCollection(ctx, 10))
		require.ErrorIs(t, server.UpdateLoadFieldsForSchema(ctx, 10, previous, target), merr.ErrServiceNotReady)
	}).Return([]*querypb.CollectionLoadInfo{{CollectionID: 10, ReplicaNumber: 1, Status: querypb.LoadStatus_Loaded, LoadFields: []int64{100}}}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, []int64{10}).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	require.NoError(t, manager.Recover(ctx, nil))
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	var saved *querypb.CollectionLoadInfo
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Run(func(_ context.Context, info *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) {
		saved = info
	}).Return(nil).Once()
	require.NoError(t, server.UpdateLoadFieldsForSchema(ctx, 10, previous, target))
	require.Equal(t, []int64{100, 101}, saved.GetLoadFields())
	require.Equal(t, []int64{100, 101}, manager.GetLoadFields(ctx, 10))
	// A genuinely unloaded collection remains unloaded after recovery.
	require.NoError(t, server.UpdateLoadFieldsForSchema(ctx, 99, previous, target))
	require.Nil(t, manager.GetCollection(ctx, 99))
}
