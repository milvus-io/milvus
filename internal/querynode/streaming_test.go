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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreaming_streaming(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streaming, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)
	defer streaming.close()

	streaming.start()
}

func TestStreaming_search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test search", func(t *testing.T) {
		streaming, err := genSimpleStreaming(ctx)
		assert.NoError(t, err)
		defer streaming.close()

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		res, err := streaming.search(searchReqs,
			defaultCollectionID,
			[]UniqueID{defaultPartitionID},
			defaultVChannel,
			plan,
			Timestamp(0))
		assert.NoError(t, err)
		assert.Len(t, res, 1)
	})

	t.Run("test run empty partition", func(t *testing.T) {
		streaming, err := genSimpleStreaming(ctx)
		assert.NoError(t, err)
		defer streaming.close()

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		res, err := streaming.search(searchReqs,
			defaultCollectionID,
			[]UniqueID{},
			defaultVChannel,
			plan,
			Timestamp(0))
		assert.NoError(t, err)
		assert.Len(t, res, 1)
	})

	t.Run("test run empty partition and loadCollection", func(t *testing.T) {
		streaming, err := genSimpleStreaming(ctx)
		assert.NoError(t, err)
		defer streaming.close()

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		col, err := streaming.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypeCollection)

		err = streaming.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		res, err := streaming.search(searchReqs,
			defaultCollectionID,
			[]UniqueID{defaultPartitionID},
			defaultVChannel,
			plan,
			Timestamp(0))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test run empty partition and loadPartition", func(t *testing.T) {
		streaming, err := genSimpleStreaming(ctx)
		assert.NoError(t, err)
		defer streaming.close()

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		col, err := streaming.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)

		err = streaming.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		_, err = streaming.search(searchReqs,
			defaultCollectionID,
			[]UniqueID{defaultPartitionID},
			defaultVChannel,
			plan,
			Timestamp(0))
		assert.Error(t, err)
	})
}

func TestStreaming_retrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streaming, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)
	defer streaming.close()

	plan, err := genSimpleRetrievePlan()
	assert.NoError(t, err)

	insertMsg, err := genSimpleInsertMsg()
	assert.NoError(t, err)

	segment, err := streaming.replica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	offset, err := segment.segmentPreInsert(len(insertMsg.RowIDs))
	assert.NoError(t, err)

	err = segment.segmentInsert(offset, &insertMsg.RowIDs, &insertMsg.Timestamps, &insertMsg.RowData)
	assert.NoError(t, err)

	t.Run("test retrieve", func(t *testing.T) {
		res, ids, err := streaming.retrieve(defaultCollectionID,
			[]UniqueID{defaultPartitionID},
			plan)
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Len(t, ids, 1)
		//assert.Error(t, err)
		//assert.Len(t, res, 0)
		//assert.Len(t, ids, 0)
	})

	t.Run("test empty partition", func(t *testing.T) {
		res, ids, err := streaming.retrieve(defaultCollectionID,
			[]UniqueID{},
			plan)
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Len(t, ids, 1)
	})
}
