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

func TestDataSyncService_DMLFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 1)

	dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 1)

	fg, err := dataSyncService.getFlowGraphByDMLChannel(defaultCollectionID, defaultDMLChannel)
	assert.NotNil(t, fg)
	assert.NoError(t, err)

	fg, err = dataSyncService.getFlowGraphByDMLChannel(defaultCollectionID, "invalid-vChannel")
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startFlowGraphByDMLChannel(defaultCollectionID, defaultDMLChannel)
	assert.NoError(t, err)

	err = dataSyncService.startFlowGraphByDMLChannel(defaultCollectionID, "invalid-vChannel")
	assert.Error(t, err)

	dataSyncService.removeFlowGraphsByDMLChannels([]Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 0)

	fg, err = dataSyncService.getFlowGraphByDMLChannel(defaultCollectionID, defaultDMLChannel)
	assert.Nil(t, fg)
	assert.Error(t, err)

	dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 1)

	dataSyncService.close()
	assert.Len(t, dataSyncService.dmlChannel2FlowGraph, 0)
}

func TestDataSyncService_DeltaFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel})
	assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

	dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel})
	assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

	fg, err := dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, defaultDeltaChannel)
	assert.NotNil(t, fg)
	assert.NoError(t, err)

	fg, err = dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, "invalid-vChannel")
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startFlowGraphForDeltaChannel(defaultCollectionID, defaultDeltaChannel)
	assert.NoError(t, err)

	err = dataSyncService.startFlowGraphForDeltaChannel(defaultCollectionID, "invalid-vChannel")
	assert.Error(t, err)

	dataSyncService.removeFlowGraphsByDeltaChannels([]Channel{defaultDeltaChannel})
	assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 0)

	fg, err = dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, defaultDeltaChannel)
	assert.Nil(t, fg)
	assert.Error(t, err)

	dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

	dataSyncService.close()
	assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 0)
}
