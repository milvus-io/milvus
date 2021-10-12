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

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

type MockRootCoordClientInterface struct {
	types.RootCoord
	Error       bool
	AccessCount int
}

func (m *MockRootCoordClientInterface) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	if in.CollectionName == "collection1" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:         []typeutil.UniqueID{1, 2},
			CreatedTimestamps:    []uint64{100, 200},
			CreatedUtcTimestamps: []uint64{100, 200},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "collection2" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:         []typeutil.UniqueID{3, 4},
			CreatedTimestamps:    []uint64{201, 202},
			CreatedUtcTimestamps: []uint64{201, 202},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			PartitionIDs:         []typeutil.UniqueID{5, 6},
			CreatedTimestamps:    []uint64{201},
			CreatedUtcTimestamps: []uint64{201},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		PartitionIDs:         []typeutil.UniqueID{},
		CreatedTimestamps:    []uint64{},
		CreatedUtcTimestamps: []uint64{},
		PartitionNames:       []string{},
	}, nil
}

func (m *MockRootCoordClientInterface) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.AccessCount++
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
	if in.CollectionName == "collection2" {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(2),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionID: typeutil.UniqueID(3),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
		}, nil
	}

	err := fmt.Errorf("can't find collection: " + in.CollectionName)
	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "describe collection failed: " + err.Error(),
		},
		Schema: nil,
	}, nil
}

//Simulate the cache path and the
func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "collection1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	assert.Equal(t, client.AccessCount, 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Equal(t, client.AccessCount, 1)
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})
	id, err = globalMetaCache.GetCollectionID(ctx, "collection2")
	assert.Equal(t, client.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection2")
	assert.Equal(t, client.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})

	// test to get from cache, this should trigger root request
	id, err = globalMetaCache.GetCollectionID(ctx, "collection1")
	assert.Equal(t, client.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Equal(t, client.AccessCount, 2)
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})

}

func TestMetaCache_GetCollectionFailure(t *testing.T) {
	ctx := context.Background()
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)
	client.Error = true

	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.NotNil(t, err)
	assert.Nil(t, schema)

	client.Error = false

	schema, err = globalMetaCache.GetCollectionSchema(ctx, "collection1")
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})

	client.Error = true
	// should be cached with no error
	assert.Nil(t, err)
	assert.Equal(t, schema, &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{},
	})
}

func TestMetaCache_GetNonExistCollection(t *testing.T) {
	ctx := context.Background()
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "collection3")
	assert.NotNil(t, err)
	assert.Equal(t, id, int64(0))
	schema, err := globalMetaCache.GetCollectionSchema(ctx, "collection3")
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
	id, err = globalMetaCache.GetPartitionID(ctx, "collection2", "par1")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(3))
	id, err = globalMetaCache.GetPartitionID(ctx, "collection2", "par2")
	assert.Nil(t, err)
	assert.Equal(t, id, typeutil.UniqueID(4))
}

func TestMetaCache_GetPartitionError(t *testing.T) {
	ctx := context.Background()
	client := &MockRootCoordClientInterface{}
	err := InitMetaCache(client)
	assert.Nil(t, err)

	// Test the case where ShowPartitionsResponse is not aligned
	id, err := globalMetaCache.GetPartitionID(ctx, "errorCollection", "par1")
	assert.NotNil(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))

	partitions, err2 := globalMetaCache.GetPartitions(ctx, "errorCollection")
	assert.NotNil(t, err2)
	log.Debug(err.Error())
	assert.Equal(t, len(partitions), 0)

	// Test non existed tables
	id, err = globalMetaCache.GetPartitionID(ctx, "nonExisted", "par1")
	assert.NotNil(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))

	// Test non existed partition
	id, err = globalMetaCache.GetPartitionID(ctx, "collection1", "par3")
	assert.NotNil(t, err)
	log.Debug(err.Error())
	assert.Equal(t, id, typeutil.UniqueID(0))
}
