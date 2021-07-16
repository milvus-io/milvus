// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

/*
import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

type MockRootCoordClientInterface struct {
	types.RootCoord
}

func (m *MockRootCoordClientInterface) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if in.CollectionName == "collection1" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:   []typeutil.UniqueID{1, 2},
			PartitionNames: []string{"par1", "par2"},
		}, nil
	}
	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs:   []typeutil.UniqueID{},
		PartitionNames: []string{},
	}, nil
}

func (m *MockRootCoordClientInterface) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if in.CollectionName == "collection1" {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(1),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
		}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		CollectionID: typeutil.UniqueID(0),
		Schema:       nil,
	}, nil
}

func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "collection1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
	})
	id, err = globalMetaCache.GetCollectionID(ctx, "collection2")
	assert.NotNil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection2")
	assert.NotNil(t, err)
	assert.Nil(t, schema)
}

func TestMetaCache_GetPartitionID(t *testing.T) {
	ctx := context.Background()
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetPartitionID(ctx, "collection1", "par1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection1", "par2")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection1", "par3")
	assert.NotNil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection2", "par3")
	assert.NotNil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection2", "par4")
	assert.NotNil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))
}
*/
