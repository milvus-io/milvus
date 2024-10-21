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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestCollectionManager(t *testing.T) {
	manager := NewCollectionManager()
	schema := GenTestCollectionSchema("test", schemapb.DataType_VarChar, false)

	collectionIDs := []int64{1, 2, 3}
	for _, collectionID := range collectionIDs {
		manager.Put(collectionID, schema, nil, nil)
		collection := manager.Get(collectionID)
		assert.NotNil(t, collection)
		assert.Equal(t, collectionID, collection.id)
	}
	assert.ElementsMatch(t, collectionIDs, manager.List())
	for _, collectionID := range collectionIDs {
		manager.Release(collectionID)
		collection := manager.Get(collectionID)
		assert.Nil(t, collection)
	}
	assert.Equal(t, 0, len(manager.List()))
}
