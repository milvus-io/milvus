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

	mFactory := &MasterServiceFactory{}
	mFactory.setCollectionID(collectionID0)
	mFactory.setCollectionName(collectionName0)
	ms := newMetaService(mFactory, collectionID0)

	t.Run("Test getCollectionSchema", func(t *testing.T) {

		sch, err := ms.getCollectionSchema(ctx, collectionID0, 0)
		assert.NoError(t, err)
		assert.NotNil(t, sch)
		assert.Equal(t, sch.Name, collectionName0)
	})

	t.Run("Test printCollectionStruct", func(t *testing.T) {
		mf := &MetaFactory{}
		collectionMeta := mf.CollectionMetaFactory(collectionID0, collectionName0)
		printCollectionStruct(collectionMeta)
	})
}
