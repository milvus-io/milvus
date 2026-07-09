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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCreateNamespaceTaskPreExecuteModeValidation(t *testing.T) {
	mockey.PatchConvey("create namespace mode validation", t, func() {
		globalMetaCache = &MetaCache{}
		ctx := context.Background()

		tests := []struct {
			name    string
			schema  *schemapb.CollectionSchema
			wantErr bool
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
				name: "partition mode",
				schema: &schemapb.CollectionSchema{
					Name:            "coll",
					EnableNamespace: true,
					Properties: []*commonpb.KeyValuePair{
						{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				guard := mockey.Mock((*MetaCache).GetCollectionSchema).Return(newSchemaInfo(tt.schema), nil).Build()
				defer guard.UnPatch()

				task := &createNamespaceTask{
					CreateNamespaceRequest: &milvuspb.CreateNamespaceRequest{
						CollectionName: "coll",
						NamespaceName:  "tenant_1",
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
	})
}

func TestCreateNamespaceTaskPreExecuteRejectsInvalidName(t *testing.T) {
	mockey.PatchConvey("create namespace invalid name", t, func() {
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(newSchemaInfo(&schemapb.CollectionSchema{
			Name:            "coll",
			EnableNamespace: true,
			Properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
			},
		}), nil).Build()

		task := &createNamespaceTask{
			CreateNamespaceRequest: &milvuspb.CreateNamespaceRequest{
				CollectionName: "coll",
				NamespaceName:  "bad name",
			},
		}
		require.Error(t, task.PreExecute(context.Background()))
	})
}

func TestDropNamespaceTaskPreExecuteRejectsLoadedNamespace(t *testing.T) {
	mockey.PatchConvey("drop namespace loaded rejection", t, func() {
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(newSchemaInfo(&schemapb.CollectionSchema{
			Name:            "coll",
			EnableNamespace: true,
			Properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
			},
		}), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
		mockey.Mock((*MetaCache).GetPartitionID).Return(int64(200), nil).Build()
		mockey.Mock(isCollectionLoaded).Return(true, nil).Build()
		mockey.Mock(isPartitionLoaded).Return(true, nil).Build()

		task := &dropNamespaceTask{
			DropNamespaceRequest: &milvuspb.DropNamespaceRequest{
				CollectionName: "coll",
				NamespaceName:  "tenant_1",
			},
		}
		require.ErrorIs(t, task.PreExecute(context.Background()), merr.ErrParameterInvalid)
	})
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
	mockey.PatchConvey("get namespace stats rejects default partition", t, func() {
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
		mockey.Mock((*MetaCache).GetPartitionID).Return(int64(200), nil).Build()

		task := &getNamespaceStatsTask{
			GetNamespaceStatsRequest: &milvuspb.GetNamespaceStatsRequest{
				CollectionName: "coll",
				NamespaceName:  Params.CommonCfg.DefaultPartitionName.GetValue(),
			},
		}
		require.ErrorIs(t, task.Execute(context.Background()), merr.ErrNamespaceNotFound)
	})
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
