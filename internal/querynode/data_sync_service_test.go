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

	dataSyncService.addDMLFlowGraphs(defaultCollectionID, defaultPartitionID, loadTypeCollection, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlFlowGraphs, 1)

	dataSyncService.addDMLFlowGraphs(defaultCollectionID, defaultPartitionID, loadTypeCollection, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlFlowGraphs, 1)

	fg, err := dataSyncService.getDMLFlowGraph(defaultCollectionID, defaultDMLChannel)
	assert.NotNil(t, fg)
	assert.NoError(t, err)

	fg, err = dataSyncService.getDMLFlowGraph(defaultCollectionID, "invalid-vChannel")
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startDMLFlowGraph(defaultCollectionID, defaultDMLChannel)
	assert.NoError(t, err)

	err = dataSyncService.startDMLFlowGraph(defaultCollectionID, "invalid-vChannel")
	assert.Error(t, err)

	dataSyncService.removeDMLFlowGraph(defaultDMLChannel)
	assert.Len(t, dataSyncService.dmlFlowGraphs, 0)

	fg, err = dataSyncService.getDMLFlowGraph(defaultCollectionID, defaultDMLChannel)
	assert.Nil(t, fg)
	assert.Error(t, err)

	dataSyncService.addDMLFlowGraphs(defaultCollectionID, defaultPartitionID, loadTypeCollection, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.dmlFlowGraphs, 1)

	dataSyncService.close()
	assert.Len(t, dataSyncService.dmlFlowGraphs, 0)
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

	dataSyncService.addDeltaFlowGraphs(defaultCollectionID, []Channel{defaultDeltaChannel})
	assert.Len(t, dataSyncService.deltaFlowGraphs, 1)

	dataSyncService.addDeltaFlowGraphs(defaultCollectionID, []Channel{defaultDeltaChannel})
	assert.Len(t, dataSyncService.deltaFlowGraphs, 1)

	fg, err := dataSyncService.getDeltaFlowGraph(defaultCollectionID, defaultDeltaChannel)
	assert.NotNil(t, fg)
	assert.NoError(t, err)

	fg, err = dataSyncService.getDeltaFlowGraph(defaultCollectionID, "invalid-vChannel")
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startDeltaFlowGraph(defaultCollectionID, defaultDeltaChannel)
	assert.NoError(t, err)

	err = dataSyncService.startDeltaFlowGraph(defaultCollectionID, "invalid-vChannel")
	assert.Error(t, err)

	dataSyncService.removeDeltaFlowGraph(defaultDeltaChannel)
	assert.Len(t, dataSyncService.deltaFlowGraphs, 0)

	fg, err = dataSyncService.getDeltaFlowGraph(defaultCollectionID, defaultDeltaChannel)
	assert.Nil(t, fg)
	assert.Error(t, err)

	dataSyncService.addDeltaFlowGraphs(defaultCollectionID, []Channel{defaultDMLChannel})
	assert.Len(t, dataSyncService.deltaFlowGraphs, 1)

	dataSyncService.close()
	assert.Len(t, dataSyncService.deltaFlowGraphs, 0)
}
