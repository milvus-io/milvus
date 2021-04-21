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
package dataservice

import (
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	testSchema := newTestSchema()
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&datapb.CollectionInfo{
		ID:         id,
		Schema:     testSchema,
		Partitions: []UniqueID{100},
	})
	assert.Nil(t, err)
	err = meta.AddCollection(&datapb.CollectionInfo{
		ID:     id,
		Schema: testSchema,
	})
	assert.NotNil(t, err)
	has := meta.HasCollection(id)
	assert.True(t, has)
	collection, err := meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, id, collection.ID)
	assert.EqualValues(t, testSchema, collection.Schema)
	assert.EqualValues(t, 1, len(collection.Partitions))
	assert.EqualValues(t, 100, collection.Partitions[0])
	err = meta.DropCollection(id)
	assert.Nil(t, err)
	has = meta.HasCollection(id)
	assert.False(t, has)
	_, err = meta.GetCollection(id)
	assert.NotNil(t, err)
}

func TestSegment(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segmentInfo, err := BuildSegment(id, 100, segID, "c1")
	assert.Nil(t, err)
	err = meta.AddSegment(segmentInfo)
	assert.Nil(t, err)
	info, err := meta.GetSegment(segmentInfo.ID)
	assert.Nil(t, err)
	assert.True(t, proto.Equal(info, segmentInfo))
	ids := meta.GetSegmentsOfCollection(id)
	assert.EqualValues(t, 1, len(ids))
	assert.EqualValues(t, segmentInfo.ID, ids[0])
	ids = meta.GetSegmentsOfPartition(id, 100)
	assert.EqualValues(t, 1, len(ids))
	assert.EqualValues(t, segmentInfo.ID, ids[0])
	err = meta.SealSegment(segmentInfo.ID, 100)
	assert.Nil(t, err)
	err = meta.FlushSegment(segmentInfo.ID, 200)
	assert.Nil(t, err)
	info, err = meta.GetSegment(segmentInfo.ID)
	assert.Nil(t, err)
	assert.NotZero(t, info.SealedTime)
	assert.NotZero(t, info.FlushedTime)
}

func TestPartition(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	testSchema := newTestSchema()
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)

	err = meta.AddPartition(id, 10)
	assert.NotNil(t, err)
	err = meta.AddCollection(&datapb.CollectionInfo{
		ID:         id,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	})
	assert.Nil(t, err)
	err = meta.AddPartition(id, 10)
	assert.Nil(t, err)
	err = meta.AddPartition(id, 10)
	assert.NotNil(t, err)
	collection, err := meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 10, collection.Partitions[0])
	err = meta.DropPartition(id, 10)
	assert.Nil(t, err)
	collection, err = meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, len(collection.Partitions))
	err = meta.DropPartition(id, 10)
	assert.NotNil(t, err)
}

func TestGetCount(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	nums, err := meta.GetNumRowsOfCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, nums)
	segment, err := BuildSegment(id, 100, segID, "c1")
	assert.Nil(t, err)
	segment.NumRows = 100
	err = meta.AddSegment(segment)
	assert.Nil(t, err)
	segID, err = mockAllocator.allocID()
	assert.Nil(t, err)
	segment, err = BuildSegment(id, 100, segID, "c1")
	assert.Nil(t, err)
	segment.NumRows = 300
	err = meta.AddSegment(segment)
	assert.Nil(t, err)
	nums, err = meta.GetNumRowsOfCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 400, nums)
}
