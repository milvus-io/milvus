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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	collectionID0   = UniqueID(0)
	collectionID1   = UniqueID(1)
	collectionName0 = "collection_0"
	collectionName1 = "collection_1"
)

func TestMetaService_All(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica := newReplica()
	mFactory := &MasterServiceFactory{}
	mFactory.setCollectionID(collectionID0)
	mFactory.setCollectionName(collectionName0)
	ms := newMetaService(ctx, replica, mFactory)

	t.Run("Test getCollectionNames", func(t *testing.T) {
		names, err := ms.getCollectionNames(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(names))
		assert.Equal(t, collectionName0, names[0])
	})

	t.Run("Test createCollection", func(t *testing.T) {
		hasColletion := ms.replica.hasCollection(collectionID0)
		assert.False(t, hasColletion)

		err := ms.createCollection(ctx, collectionName0)
		assert.NoError(t, err)
		hasColletion = ms.replica.hasCollection(collectionID0)
		assert.True(t, hasColletion)
	})

	t.Run("Test loadCollections", func(t *testing.T) {
		hasColletion := ms.replica.hasCollection(collectionID1)
		assert.False(t, hasColletion)

		mFactory.setCollectionID(1)
		mFactory.setCollectionName(collectionName1)
		err := ms.loadCollections(ctx)
		assert.NoError(t, err)

		hasColletion = ms.replica.hasCollection(collectionID0)
		assert.True(t, hasColletion)
		hasColletion = ms.replica.hasCollection(collectionID1)
		assert.True(t, hasColletion)
	})

	t.Run("Test Init", func(t *testing.T) {
		ms1 := newMetaService(ctx, replica, mFactory)
		ms1.init()
	})

	t.Run("Test printCollectionStruct", func(t *testing.T) {
		mf := &MetaFactory{}
		collectionMeta := mf.CollectionMetaFactory(collectionID0, collectionName0)
		printCollectionStruct(collectionMeta)
	})
}
