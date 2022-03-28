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

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistorical_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test search", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests(IndexFaissIDMap)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.NoError(t, err)
	})

	t.Run("test no collection - search partitions", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests(IndexFaissIDMap)
		assert.NoError(t, err)

		err = his.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test no collection - search all collection", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests(IndexFaissIDMap)
		assert.NoError(t, err)

		err = his.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test load partition and partition has been released", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests(IndexFaissIDMap)
		assert.NoError(t, err)

		col, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test no partition in collection", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests(IndexFaissIDMap)
		assert.NoError(t, err)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		res, ids, _, err := his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Equal(t, 0, len(res))
		assert.Equal(t, 0, len(ids))
		assert.NoError(t, err)
	})
}
