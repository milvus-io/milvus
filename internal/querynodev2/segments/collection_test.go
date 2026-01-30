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

func (s *CollectionManagerSuite) TestRef() {
	s.Run("ref_existing_collection", func() {
		ok := s.cm.Ref(1, 1)
		s.True(ok)
	})

	s.Run("ref_non_existing_collection", func() {
		ok := s.cm.Ref(9999, 1)
		s.False(ok)
	})
}

func (s *CollectionManagerSuite) TestUnref() {
	s.Run("unref_non_existing_collection", func() {
		// Unref on non-existing collection should return true
		ok := s.cm.Unref(9999, 1)
		s.True(ok)
	})

	s.Run("unref_without_release", func() {
		// Add more refs first
		s.cm.Ref(1, 2)
		// Unref once, should not release (refCount > 0)
		ok := s.cm.Unref(1, 1)
		s.False(ok)
		// Collection should still exist
		coll := s.cm.Get(1)
		s.NotNil(coll)
	})

	s.Run("unref_with_release", func() {
		// Create a new collection manager for this test
		cm := NewCollectionManager()
		schema := mock_segcore.GenTestCollectionSchema("collection_2", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(2, schema, mock_segcore.GenTestIndexMeta(2, schema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.Require().NoError(err)

		// Unref to release the collection (refCount goes to 0)
		ok := cm.Unref(2, 1)
		s.True(ok)

		// Collection should be removed
		coll := cm.Get(2)
		s.Nil(coll)
	})
}

func (s *CollectionManagerSuite) TestList() {
	ids := s.cm.List()
	s.Contains(ids, int64(1))
}

func (s *CollectionManagerSuite) TestListWithName() {
	names := s.cm.ListWithName()
	s.Contains(names, int64(1))
	s.Equal("collection_1", names[1])
}

func (s *CollectionManagerSuite) TestPutOrRef() {
	s.Run("put_new_collection", func() {
		cm := NewCollectionManager()
		schema := mock_segcore.GenTestCollectionSchema("collection_new", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(100, schema, mock_segcore.GenTestIndexMeta(100, schema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.NoError(err)
		coll := cm.Get(100)
		s.NotNil(coll)
	})

	s.Run("ref_existing_collection", func() {
		// Ref existing collection (id=1)
		schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
		err := s.cm.PutOrRef(1, schema, mock_segcore.GenTestIndexMeta(1, schema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.NoError(err)
	})
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}
