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

package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection_Group(t *testing.T) {
	Factory := &MetaFactory{}

	collName := "collection0"
	collID := UniqueID(1)
	collMeta := Factory.CollectionMetaFactory(collID, collName)

	t.Run("new_collection_nil_schema", func(t *testing.T) {
		coll, err := newCollection(collID, nil)
		assert.Error(t, err)
		assert.Nil(t, coll)
	})

	t.Run("new_collection_right_schema", func(t *testing.T) {
		coll, err := newCollection(collID, collMeta.Schema)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		assert.Equal(t, collName, coll.GetName())
		assert.Equal(t, collID, coll.GetID())
		assert.Equal(t, collMeta.Schema, coll.GetSchema())
		assert.Equal(t, *collMeta.Schema, *coll.GetSchema())
	})

	t.Run("getters", func(t *testing.T) {
		coll := new(Collection)
		assert.Empty(t, coll.GetName())
		assert.Empty(t, coll.GetID())
		assert.Empty(t, coll.GetSchema())

		coll, err := newCollection(collID, collMeta.Schema)
		assert.NoError(t, err)
		assert.Equal(t, collName, coll.GetName())
		assert.Equal(t, collID, coll.GetID())
		assert.Equal(t, collMeta.Schema, coll.GetSchema())
		assert.Equal(t, *collMeta.Schema, *coll.GetSchema())
	})

}
