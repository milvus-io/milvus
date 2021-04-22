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

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestMetaService_start(t *testing.T) {
	node := newQueryNodeMock()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	node.metaService.start()
	node.Stop()
}

func TestMetaService_getCollectionObjId(t *testing.T) {
	var key = "/collection/collection0"
	var collectionObjID1 = GetCollectionObjID(key)

	assert.Equal(t, collectionObjID1, "/collection/collection0")

	key = "fakeKey"
	var collectionObjID2 = GetCollectionObjID(key)

	assert.Equal(t, collectionObjID2, "fakeKey")
}

func TestMetaService_getSegmentObjId(t *testing.T) {
	var key = "/segment/segment0"
	var segmentObjID1 = GetSegmentObjID(key)

	assert.Equal(t, segmentObjID1, "/segment/segment0")

	key = "fakeKey"
	var segmentObjID2 = GetSegmentObjID(key)

	assert.Equal(t, segmentObjID2, "fakeKey")
}

func TestMetaService_isCollectionObj(t *testing.T) {
	var key = Params.MetaRootPath + "/collection/collection0"
	var b1 = isCollectionObj(key)

	assert.Equal(t, b1, true)

	key = Params.MetaRootPath + "/segment/segment0"
	var b2 = isCollectionObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_isSegmentObj(t *testing.T) {
	var key = Params.MetaRootPath + "/segment/segment0"
	var b1 = isSegmentObj(key)

	assert.Equal(t, b1, true)

	key = Params.MetaRootPath + "/collection/collection0"
	var b2 = isSegmentObj(key)

	assert.Equal(t, b2, false)
}

func TestMetaService_printCollectionStruct(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)
	printCollectionStruct(collectionMeta)
}

func TestMetaService_printSegmentStruct(t *testing.T) {
	var s = datapb.SegmentInfo{
		ID:           UniqueID(0),
		CollectionID: UniqueID(0),
		PartitionID:  defaultPartitionID,
		OpenTime:     Timestamp(0),
		NumRows:      UniqueID(0),
	}

	printSegmentStruct(&s)
}

func TestMetaService_processCollectionCreate(t *testing.T) {
	node := newQueryNodeMock()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `schema: <
				name: "test"
				autoID: true
				fields: <
				fieldID:100
				name: "vec"
				data_type: FloatVector
				type_params: <
				  key: "dim"
				  value: "16"
				>
				index_params: <
				  key: "metric_type"
				  value: "L2"
				>
				>
				fields: <
				fieldID:101
				name: "age"
				data_type: Int32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				partitionIDs: 2021
				`

	node.metaService.processCollectionCreate(id, value)

	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))
	node.Stop()
}

func TestMetaService_processSegmentCreate(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	id := "0"
	value := `partitionID: 2021
				`

	(*node.metaService).processSegmentCreate(id, value)

	s, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))
	node.Stop()
}

func TestMetaService_processCreate(t *testing.T) {
	node := newQueryNodeMock()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	key1 := Params.MetaRootPath + "/collection/0"
	msg1 := `schema: <
				name: "test"
				autoID: true
				fields: <
				fieldID:100
				name: "vec"
				data_type: FloatVector
				type_params: <
				  key: "dim"
				  value: "16"
				>
				index_params: <
				  key: "metric_type"
				  value: "L2"
				>
				>
				fields: <
				fieldID:101
				name: "age"
				data_type: Int32
				type_params: <
				  key: "dim"
				  value: "1"
				>
				>
				>
				partitionIDs: 2021
				`

	(*node.metaService).processCreate(key1, msg1)
	collectionNum := node.replica.getCollectionNum()
	assert.Equal(t, collectionNum, 1)

	collection, err := node.replica.getCollectionByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, collection.ID(), UniqueID(0))

	key2 := Params.MetaRootPath + "/segment/0"
	msg2 := `partitionID: 2021
				`

	(*node.metaService).processCreate(key2, msg2)
	s, err := node.replica.getSegmentByID(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, s.segmentID, UniqueID(0))
	node.Stop()
}

func TestMetaService_loadCollections(t *testing.T) {
	node := newQueryNodeMock()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	err2 := (*node.metaService).loadCollections()
	assert.Nil(t, err2)
	node.Stop()
}

func TestMetaService_loadSegments(t *testing.T) {
	node := newQueryNodeMock()
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	err2 := (*node.metaService).loadSegments()
	assert.Nil(t, err2)
	node.Stop()
}
