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

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func TestDataSyncService_DMLFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	t.Run("test DMLFlowGraphs", func(t *testing.T) {
		_, err = dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 1)

		_, err = dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 1)

		fg, err := dataSyncService.getFlowGraphByDMLChannel(defaultCollectionID, defaultDMLChannel)
		assert.NotNil(t, fg)
		assert.NoError(t, err)

		err = dataSyncService.startFlowGraphByDMLChannel(defaultCollectionID, defaultDMLChannel)
		assert.NoError(t, err)

		dataSyncService.removeFlowGraphsByDMLChannels([]Channel{defaultDMLChannel})
		assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 0)

		fg, err = dataSyncService.getFlowGraphByDMLChannel(defaultCollectionID, defaultDMLChannel)
		assert.Nil(t, fg)
		assert.Error(t, err)

		_, err = dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 1)

		dataSyncService.close()
		assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 0)
	})

	t.Run("test DMLFlowGraphs invalid channel", func(t *testing.T) {
		fg, err := dataSyncService.getFlowGraphByDMLChannel(defaultCollectionID, "invalid-vChannel")
		assert.Nil(t, fg)
		assert.Error(t, err)

		err = dataSyncService.startFlowGraphByDMLChannel(defaultCollectionID, "invalid-vChannel")
		assert.Error(t, err)
	})

	t.Run("test addFlowGraphsForDMLChannels checkReplica Failed", func(t *testing.T) {
		err = dataSyncService.historicalReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		_, err = dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.Error(t, err)
		dataSyncService.historicalReplica.addCollection(defaultCollectionID, genTestCollectionSchema(schemapb.DataType_Int64))
	})
}

func TestDataSyncService_DeltaFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	t.Run("test DeltaFlowGraphs", func(t *testing.T) {
		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

		fg, err := dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.NotNil(t, fg)
		assert.NoError(t, err)

		err = dataSyncService.startFlowGraphForDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.NoError(t, err)

		dataSyncService.removeFlowGraphsByDeltaChannels([]Channel{defaultDeltaChannel})
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 0)

		fg, err = dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.Nil(t, fg)
		assert.Error(t, err)

		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

		dataSyncService.close()
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 0)
	})

	t.Run("test DeltaFlowGraphs invalid channel", func(t *testing.T) {
		fg, err := dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, "invalid-vChannel")
		assert.Nil(t, fg)
		assert.Error(t, err)

		err = dataSyncService.startFlowGraphForDeltaChannel(defaultCollectionID, "invalid-vChannel")
		assert.Error(t, err)
	})

	t.Run("test addFlowGraphsForDeltaChannels checkReplica Failed", func(t *testing.T) {
		err = dataSyncService.historicalReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.Error(t, err)
		dataSyncService.historicalReplica.addCollection(defaultCollectionID, genTestCollectionSchema(schemapb.DataType_Int64))
	})
}

func TestDataSyncService_checkReplica(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)
	defer dataSyncService.close()

	t.Run("test checkReplica", func(t *testing.T) {
		err = dataSyncService.checkReplica(defaultCollectionID)
		assert.NoError(t, err)
	})

	t.Run("test collection doesn't exist", func(t *testing.T) {
		err = dataSyncService.streamingReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		err = dataSyncService.checkReplica(defaultCollectionID)
		assert.Error(t, err)

		err = dataSyncService.historicalReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		err = dataSyncService.checkReplica(defaultCollectionID)
		assert.Error(t, err)

		coll := dataSyncService.historicalReplica.addCollection(defaultCollectionID, genTestCollectionSchema(schemapb.DataType_Int64))
		assert.NotNil(t, coll)
		coll = dataSyncService.streamingReplica.addCollection(defaultCollectionID, genTestCollectionSchema(schemapb.DataType_Int64))
		assert.NotNil(t, coll)
	})

	t.Run("test different loadType", func(t *testing.T) {
		coll, err := dataSyncService.historicalReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		coll.setLoadType(loadTypePartition)

		err = dataSyncService.checkReplica(defaultCollectionID)
		assert.Error(t, err)

		coll, err = dataSyncService.streamingReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		coll.setLoadType(loadTypePartition)
	})

	t.Run("test cannot find tSafe", func(t *testing.T) {
		coll, err := dataSyncService.historicalReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		coll.addVDeltaChannels([]Channel{defaultDeltaChannel})
		coll.addVChannels([]Channel{defaultDMLChannel})

		dataSyncService.tSafeReplica.addTSafe(defaultDeltaChannel)
		dataSyncService.tSafeReplica.addTSafe(defaultDMLChannel)

		dataSyncService.tSafeReplica.removeTSafe(defaultDeltaChannel)
		err = dataSyncService.checkReplica(defaultCollectionID)
		assert.Error(t, err)

		dataSyncService.tSafeReplica.removeTSafe(defaultDMLChannel)
		err = dataSyncService.checkReplica(defaultCollectionID)
		assert.Error(t, err)

		dataSyncService.tSafeReplica.addTSafe(defaultDeltaChannel)
		dataSyncService.tSafeReplica.addTSafe(defaultDMLChannel)
	})
}
