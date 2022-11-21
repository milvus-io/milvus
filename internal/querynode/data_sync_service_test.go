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
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func init() {
	rateCol, _ = newRateCollector()
}

func TestDataSyncService_DMLFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, replica, tSafe, fac)
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
		err = dataSyncService.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		_, err = dataSyncService.addFlowGraphsForDMLChannels(defaultCollectionID, []Channel{defaultDMLChannel})
		assert.Error(t, err)
		dataSyncService.metaReplica.addCollection(defaultCollectionID, genTestCollectionSchema())
	})
}

func TestDataSyncService_DeltaFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, replica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	t.Run("test DeltaFlowGraphs", func(t *testing.T) {
		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel}, map[string]string{defaultDeltaChannel: defaultDeltaChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel}, map[string]string{defaultDeltaChannel: defaultDeltaChannel})
		assert.NoError(t, err)
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 1)

		fg, err := dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.NotNil(t, fg)
		assert.NoError(t, err)

		err = dataSyncService.startFlowGraphForDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.NoError(t, err)

		dataSyncService.removeFlowGraphsByDeltaChannels([]Channel{defaultDeltaChannel})
		replica.removeCollectionVDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.Len(t, dataSyncService.deltaChannel2FlowGraph, 0)

		fg, err = dataSyncService.getFlowGraphByDeltaChannel(defaultCollectionID, defaultDeltaChannel)
		assert.Nil(t, fg)
		assert.Error(t, err)

		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel}, map[string]string{defaultDeltaChannel: defaultDeltaChannel})
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
		err = dataSyncService.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		_, err = dataSyncService.addFlowGraphsForDeltaChannels(defaultCollectionID, []Channel{defaultDeltaChannel}, map[string]string{defaultDeltaChannel: defaultDeltaChannel})
		assert.Error(t, err)
		dataSyncService.metaReplica.addCollection(defaultCollectionID, genTestCollectionSchema())
	})
}

type DataSyncServiceSuite struct {
	suite.Suite
	factory   dependency.Factory
	dsService *dataSyncService
}

func (s *DataSyncServiceSuite) SetupSuite() {
	s.factory = genFactory()
}

func (s *DataSyncServiceSuite) SetupTest() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica, err := genSimpleReplica()
	s.Require().NoError(err)

	tSafe := newTSafeReplica()
	s.dsService = newDataSyncService(ctx, replica, tSafe, s.factory)
	s.Require().NoError(err)
}

func (s *DataSyncServiceSuite) TearDownTest() {
	s.dsService.close()
	s.dsService = nil
}

func (s *DataSyncServiceSuite) TestRemoveEmptyFlowgraphByChannel() {
	s.Run("non existing channel", func() {
		s.Assert().NotPanics(func() {
			channelName := fmt.Sprintf("%s_%d_1", Params.CommonCfg.RootCoordDml.GetValue(), defaultCollectionID)
			s.dsService.removeEmptyFlowGraphByChannel(defaultCollectionID, channelName)
		})
	})

	s.Run("bad format channel", func() {
		s.Assert().NotPanics(func() {
			s.dsService.removeEmptyFlowGraphByChannel(defaultCollectionID, "")
		})
	})

	s.Run("non-empty flowgraph", func() {
		channelName := fmt.Sprintf("%s_%d_1", Params.CommonCfg.RootCoordDml.GetValue(), defaultCollectionID)
		deltaChannelName, err := funcutil.ConvertChannelName(channelName, Params.CommonCfg.RootCoordDml.GetValue(), Params.CommonCfg.RootCoordDelta.GetValue())
		s.Require().NoError(err)
		err = s.dsService.metaReplica.addSegment(defaultSegmentID, defaultPartitionID, defaultCollectionID, channelName, defaultSegmentVersion, defaultSegmentStartPosition, segmentTypeSealed)
		s.Require().NoError(err)

		_, err = s.dsService.addFlowGraphsForDeltaChannels(defaultCollectionID, []string{deltaChannelName}, map[string]string{deltaChannelName: deltaChannelName})
		s.Require().NoError(err)

		s.Assert().NotPanics(func() {
			s.dsService.removeEmptyFlowGraphByChannel(defaultCollectionID, channelName)
		})

		_, err = s.dsService.getFlowGraphByDeltaChannel(defaultCollectionID, deltaChannelName)
		s.Assert().NoError(err)
	})

	s.Run("empty flowgraph", func() {
		channelName := fmt.Sprintf("%s_%d_2", Params.CommonCfg.RootCoordDml.GetValue(), defaultCollectionID)
		deltaChannelName, err := funcutil.ConvertChannelName(channelName, Params.CommonCfg.RootCoordDml.GetValue(), Params.CommonCfg.RootCoordDelta.GetValue())
		s.Require().NoError(err)

		_, err = s.dsService.addFlowGraphsForDeltaChannels(defaultCollectionID, []string{deltaChannelName}, map[string]string{deltaChannelName: deltaChannelName})
		s.Require().NoError(err)

		s.Assert().NotPanics(func() {
			s.dsService.removeEmptyFlowGraphByChannel(defaultCollectionID, channelName)
		})

		_, err = s.dsService.getFlowGraphByDeltaChannel(defaultCollectionID, deltaChannelName)
		s.Assert().Error(err)
	})
}

func TestDataSyncServiceSuite(t *testing.T) {
	suite.Run(t, new(DataSyncServiceSuite))
}
