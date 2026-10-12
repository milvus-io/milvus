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

package proxy

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func namespacePartitionModeSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:            "coll",
		EnableNamespace: true,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
		},
	}
}

func TestCreateNamespaceTaskPreExecuteModeValidation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		schema    *schemapb.CollectionSchema
		namespace string
		wantErr   bool
	}{
		{
			name: "namespace disabled",
			schema: &schemapb.CollectionSchema{
				Name:            "coll",
				EnableNamespace: false,
			},
			wantErr: true,
		},
		{
			name: "partition key mode",
			schema: &schemapb.CollectionSchema{
				Name:            "coll",
				EnableNamespace: true,
				Properties: []*commonpb.KeyValuePair{
					{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey},
				},
			},
			wantErr: true,
		},
		{
			name:   "partition mode",
			schema: namespacePartitionModeSchema(),
		},
		{
			name:      "invalid namespace name",
			schema:    namespacePartitionModeSchema(),
			namespace: "bad name",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCache := NewMockCache(t)
			mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, "coll").Return(mustNewSchemaInfo(tt.schema), nil)

			task := &createNamespaceTask{
				baseTask: baseTask{MetaCache: mockCache},
				CreateNamespaceRequest: &milvuspb.CreateNamespaceRequest{
					CollectionName: "coll",
					NamespaceName:  lo.Ternary(tt.namespace == "", "tenant_1", tt.namespace),
				},
			}
			err := task.PreExecute(ctx)
			if tt.wantErr {
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestDropNamespaceTaskPreExecuteRejectsLoadedNamespace(t *testing.T) {
	mockCache := NewMockCache(t)
	mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, "coll").Return(mustNewSchemaInfo(namespacePartitionModeSchema()), nil)
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, "coll").Return(int64(100), nil)
	mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, "coll", "tenant_1").Return(int64(200), nil)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:        merr.Success(),
		CollectionIDs: []int64{100},
	}, nil)
	mixCoord.EXPECT().ShowLoadPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status:       merr.Success(),
		PartitionIDs: []int64{200},
	}, nil)

	task := &dropNamespaceTask{
		baseTask: baseTask{MetaCache: mockCache},
		mixCoord: mixCoord,
		DropNamespaceRequest: &milvuspb.DropNamespaceRequest{
			CollectionName: "coll",
			NamespaceName:  "tenant_1",
		},
	}
	require.ErrorIs(t, task.PreExecute(context.Background()), merr.ErrParameterInvalid)
}

func TestGetNamespaceStatsTaskPreExecuteRejectsExact(t *testing.T) {
	task := &getNamespaceStatsTask{
		GetNamespaceStatsRequest: &milvuspb.GetNamespaceStatsRequest{
			Exact: true,
		},
	}
	require.ErrorIs(t, task.PreExecute(context.Background()), merr.ErrParameterInvalid)
}

func TestGetNamespaceStatsTaskExecuteRejectsDefaultPartition(t *testing.T) {
	defaultPartition := Params.CommonCfg.DefaultPartitionName.GetValue()
	mockCache := NewMockCache(t)
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, "coll").Return(int64(100), nil)
	mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, "coll", defaultPartition).Return(int64(200), nil)

	task := &getNamespaceStatsTask{
		baseTask: baseTask{MetaCache: mockCache},
		GetNamespaceStatsRequest: &milvuspb.GetNamespaceStatsRequest{
			CollectionName: "coll",
			NamespaceName:  defaultPartition,
		},
	}
	require.ErrorIs(t, task.Execute(context.Background()), merr.ErrNamespaceNotFound)
}

func TestNamespaceStatsFromKVs(t *testing.T) {
	stats := namespaceStatsFromKVs(context.Background(), []*commonpb.KeyValuePair{
		{Key: "row_count", Value: "42"},
		{Key: "other", Value: "ignored"},
	})
	require.Equal(t, int64(42), stats.GetApproxEntityCount())
	require.Equal(t, "approximate", stats.GetEntityCountType())
	require.Len(t, stats.GetStats(), 2)
}
