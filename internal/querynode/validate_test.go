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

func TestQueryShardHistorical_validateSegmentIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test normal validate", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{defaultPartitionID}, []UniqueID{defaultSegmentID})
		assert.NoError(t, err)
	})

	t.Run("test normal validate2", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{}, []UniqueID{defaultSegmentID})
		assert.NoError(t, err)
	})

	t.Run("test validate non-existent collection", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID+1, []UniqueID{defaultPartitionID}, []UniqueID{defaultSegmentID})
		assert.Error(t, err)
	})

	t.Run("test validate non-existent partition", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{defaultPartitionID + 1}, []UniqueID{defaultSegmentID})
		assert.Error(t, err)
	})

	t.Run("test validate non-existent segment", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{defaultPartitionID}, []UniqueID{defaultSegmentID + 1})
		assert.Error(t, err)
	})

	t.Run("test validate segment not in given partition", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		err = his.addPartition(defaultCollectionID, defaultPartitionID+1)
		assert.NoError(t, err)
		schema := genTestCollectionSchema()
		seg, err := genSealedSegment(schema,
			defaultCollectionID,
			defaultPartitionID+1,
			defaultSegmentID+1,
			defaultDMLChannel,
			defaultMsgLength)
		assert.NoError(t, err)
		err = his.setSegment(seg)
		assert.NoError(t, err)
		// Scenario: search for a segment (segmentID = defaultSegmentID + 1, partitionID = defaultPartitionID+1)
		// that does not belong to defaultPartition
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{defaultPartitionID}, []UniqueID{defaultSegmentID + 1})
		assert.Error(t, err)
	})

	t.Run("test validate after partition release", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		err = his.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{}, []UniqueID{defaultSegmentID})
		assert.Error(t, err)
	})

	t.Run("test validate after partition release2", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		col, err := his.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)
		err = his.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{}, []UniqueID{defaultSegmentID})
		assert.Error(t, err)
	})

	t.Run("test validate after partition release3", func(t *testing.T) {
		his, err := genSimpleReplicaWithSealSegment(ctx)
		assert.NoError(t, err)
		col, err := his.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypeCollection)
		err = his.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		_, _, err = validateOnHistoricalReplica(context.TODO(), his, defaultCollectionID, []UniqueID{}, []UniqueID{defaultSegmentID})
		assert.NoError(t, err)
	})
}
