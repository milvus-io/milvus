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

package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type CollectionManagerSuite struct {
	suite.Suite
	cm *collectionManager
}

func (s *CollectionManagerSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CollectionManagerSuite) SetupTest() {
	s.cm = NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	err := s.cm.PutOrRef(1, schema, mock_segcore.GenTestIndexMeta(1, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
		DbProperties: []*commonpb.KeyValuePair{
			{
				Key:   common.ReplicateIDKey,
				Value: "local-test",
			},
		},
	})
	s.Require().NoError(err)
}

func (s *CollectionManagerSuite) TestRefUnref() {
	var id int64 = 1
	s.cm = NewCollectionManager()

	// create a new Collection
	schema1 := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	indexMeta1 := mock_segcore.GenTestIndexMeta(id, schema1)
	err := s.cm.PutOrRef(id, schema1, indexMeta1, &querypb.LoadMetaInfo{
		LoadType:      querypb.LoadType_LoadCollection,
		SchemaVersion: 1,
	})
	s.Require().NoError(err)

	col := s.cm.Get(id)
	s.Require().NotNil(col)
	s.Require().Equal(schema1, col.Schema())
	segCol := col.GetCCollection()
	s.Require().Equal(indexMeta1, segCol.IndexMeta())

	exist := s.cm.Ref(id, 1)
	s.Require().True(exist)

	// update the Collection
	schema2 := mock_segcore.GenTestBM25CollectionSchema("bm25")
	indexMeta2 := mock_segcore.GenTestIndexMeta(id, schema2)
	indexMeta2.IndexMetas = indexMeta2.IndexMetas[:1]
	err = s.cm.PutOrRef(id, schema2, indexMeta2, &querypb.LoadMetaInfo{
		LoadType:      querypb.LoadType_LoadCollection,
		SchemaVersion: 2,
	})
	s.Require().NoError(err)

	col = s.cm.Get(id)
	s.Require().NotNil(col)
	s.Require().Equal(schema2, col.Schema())
	segCol = col.GetCCollection()
	s.Require().Equal(indexMeta2, segCol.IndexMeta())
}

func (s *CollectionManagerSuite) TestUpdateSchema() {
	s.Run("normal_case", func() {
		schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + int64(len(schema.Fields)),
			Name:     "added_field",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		err := s.cm.UpdateSchema(1, schema, 100)
		s.NoError(err)
	})

	s.Run("not_exist_collection", func() {
		schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + int64(len(schema.Fields)),
			Name:     "added_field",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		err := s.cm.UpdateSchema(2, schema, 100)
		s.Error(err)
	})

	s.Run("nil_schema", func() {
		s.NotPanics(func() {
			err := s.cm.UpdateSchema(1, nil, 100)
			s.Error(err)
		})
	})
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}

func TestCollection(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("collection", schemapb.DataType_Int64, false)
	indexMeta := mock_segcore.GenTestIndexMeta(1, schema)
	col, err := NewCollection(1, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		DbProperties: []*commonpb.KeyValuePair{},
	})
	assert.NoError(t, err)
	assert.NotNil(t, col)

	refCount := col.Ref(1)
	assert.Equal(t, uint32(1), refCount)
	refCount = col.Ref(10)
	assert.Equal(t, uint32(11), refCount)
	refCount = col.Ref(0)
	assert.Equal(t, uint32(11), refCount)
	refCount = col.Unref(1)
	assert.Equal(t, uint32(10), refCount)
	refCount = col.Unref(0)
	assert.Equal(t, uint32(10), refCount)
	refCount = col.Unref(100)
	assert.Equal(t, uint32(0), refCount)
}
