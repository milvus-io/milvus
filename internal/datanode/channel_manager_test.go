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

package datanode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestChannelManagerSuite(t *testing.T) {
	suite.Run(t, new(ChannelManagerSuite))
}

func TestOpRunnerSuite(t *testing.T) {
	suite.Run(t, new(OpRunnerSuite))
}

func (s *OpRunnerSuite) SetupTest() {
	ctx := context.Background()
	s.mockAlloc = allocator.NewMockAllocator(s.T())

	s.node = newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	s.node.allocator = s.mockAlloc
}

func (s *OpRunnerSuite) TestWatchWithTimer() {
	var (
		channel string = "ch-1"
		commuCh        = make(chan *opState)
	)
	info := getWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	mockReleaseFunc := func(channel string) {
		log.Info("mock release func")
	}
	runner := NewOpRunner(channel, s.node, mockReleaseFunc, executeWatch, commuCh)
	err := runner.Enqueue(info)
	s.Require().NoError(err)

	opState := runner.watchWithTimer(info)
	s.NotNil(opState.fg)
	s.Equal(channel, opState.channel)

	runner.FinishOp(100)
}

func (s *OpRunnerSuite) TestWatchTimeout() {
	channel := "by-dev-rootcoord-dml-1000"
	paramtable.Get().Save(Params.DataCoordCfg.WatchTimeoutInterval.Key, "0.000001")
	defer paramtable.Get().Reset(Params.DataCoordCfg.WatchTimeoutInterval.Key)
	info := getWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)

	sig := make(chan struct{})
	commuCh := make(chan *opState)

	mockReleaseFunc := func(channel string) { log.Info("mock release func") }
	mockWatchFunc := func(ctx context.Context, dn *DataNode, info *datapb.ChannelWatchInfo, tickler *tickler) (*dataSyncService, error) {
		<-ctx.Done()
		sig <- struct{}{}
		return nil, errors.New("timeout")
	}

	runner := NewOpRunner(channel, s.node, mockReleaseFunc, mockWatchFunc, commuCh)
	runner.Start()
	defer runner.Close()
	err := runner.Enqueue(info)
	s.Require().NoError(err)

	<-sig
	opState := <-commuCh
	s.Require().NotNil(opState)
	s.Equal(info.GetOpID(), opState.opID)
	s.Equal(datapb.ChannelWatchState_WatchFailure, opState.state)
}

type OpRunnerSuite struct {
	suite.Suite
	node      *DataNode
	mockAlloc *allocator.MockAllocator
}

type ChannelManagerSuite struct {
	suite.Suite

	node    *DataNode
	manager *ChannelManagerImpl
}

func (s *ChannelManagerSuite) SetupTest() {
	ctx := context.Background()
	s.node = newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	s.node.allocator = allocator.NewMockAllocator(s.T())
	s.node.flowgraphManager = newFlowgraphManager()

	s.manager = NewChannelManager(s.node)
}

func getWatchInfoByOpID(opID UniqueID, channel string, state datapb.ChannelWatchState) *datapb.ChannelWatchInfo {
	return &datapb.ChannelWatchInfo{
		OpID:  opID,
		State: state,
		Vchan: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  channel,
		},
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
				},
				{
					FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
				},
				{
					FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
				},
				{
					FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "128"},
					},
				},
			},
		},
	}
}

func (s *ChannelManagerSuite) TearDownTest() {
	if s.manager != nil {
		s.manager.Close()
	}
}

func (s *ChannelManagerSuite) TestReleaseStuck() {
	var (
		channel  = "by-dev-rootcoord-dml-2"
		stuckSig = make(chan struct{})
	)
	s.manager.releaseFunc = func(channel string) {
		stuckSig <- struct{}{}
	}

	info := getWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	s.Require().Equal(0, s.manager.opRunners.Len())
	err := s.manager.Submit(info)
	s.Require().NoError(err)

	opState := <-s.manager.communicateCh
	s.Require().NotNil(opState)

	s.manager.handleOpState(opState)

	releaseInfo := getWatchInfoByOpID(101, channel, datapb.ChannelWatchState_ToRelease)
	paramtable.Get().Save(Params.DataCoordCfg.WatchTimeoutInterval.Key, "0.1")
	defer paramtable.Get().Reset(Params.DataCoordCfg.WatchTimeoutInterval.Key)

	err = s.manager.Submit(releaseInfo)
	s.NoError(err)

	opState = <-s.manager.communicateCh
	s.Require().NotNil(opState)
	s.Equal(datapb.ChannelWatchState_ReleaseFailure, opState.state)
	s.manager.handleOpState(opState)

	s.Equal(1, s.manager.abnormals.Len())
	abchannel, ok := s.manager.abnormals.Get(releaseInfo.GetOpID())
	s.True(ok)
	s.Equal(channel, abchannel)

	<-stuckSig

	resp := s.manager.GetProgress(releaseInfo)
	s.Equal(datapb.ChannelWatchState_ReleaseFailure, resp.GetState())
}

func (s *ChannelManagerSuite) TestSubmitIdempotent() {
	channel := "by-dev-rootcoord-dml-1"

	info := getWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	s.Require().Equal(0, s.manager.opRunners.Len())

	for i := 0; i < 10; i++ {
		err := s.manager.Submit(info)
		s.NoError(err)
	}

	s.Equal(1, s.manager.opRunners.Len())
	s.True(s.manager.opRunners.Contain(channel))

	runner, ok := s.manager.opRunners.Get(channel)
	s.True(ok)
	s.Equal(1, runner.UnfinishedOpSize())
}

func (s *ChannelManagerSuite) TestSubmitSkip() {
	channel := "by-dev-rootcoord-dml-1"

	info := getWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	s.Require().Equal(0, s.manager.opRunners.Len())

	err := s.manager.Submit(info)
	s.NoError(err)

	s.Equal(1, s.manager.opRunners.Len())
	s.True(s.manager.opRunners.Contain(channel))
	opState := <-s.manager.communicateCh
	s.NotNil(opState)
	s.Equal(datapb.ChannelWatchState_WatchSuccess, opState.state)
	s.NotNil(opState.fg)
	s.Equal(info.GetOpID(), opState.fg.opID)
	s.manager.handleOpState(opState)

	err = s.manager.Submit(info)
	s.NoError(err)

	runner, ok := s.manager.opRunners.Get(channel)
	s.False(ok)
	s.Nil(runner)
}

func (s *ChannelManagerSuite) TestSubmitWatchAndRelease() {
	channel := "by-dev-rootcoord-dml-0"

	// watch
	info := getWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	err := s.manager.Submit(info)
	s.NoError(err)

	// wait for result
	opState := <-s.manager.communicateCh
	s.NotNil(opState)
	s.Equal(datapb.ChannelWatchState_WatchSuccess, opState.state)
	s.NotNil(opState.fg)
	s.Equal(info.GetOpID(), opState.fg.opID)

	resp := s.manager.GetProgress(info)
	s.Equal(info.GetOpID(), resp.GetOpID())
	s.Equal(datapb.ChannelWatchState_ToWatch, resp.GetState())

	s.manager.handleOpState(opState)
	s.Equal(1, s.manager.fgManager.GetFlowgraphCount())
	s.False(s.manager.opRunners.Contain(info.GetVchan().GetChannelName()))
	s.Equal(0, s.manager.opRunners.Len())

	resp = s.manager.GetProgress(info)
	s.Equal(info.GetOpID(), resp.GetOpID())
	s.Equal(datapb.ChannelWatchState_WatchSuccess, resp.GetState())

	// release
	info = getWatchInfoByOpID(101, channel, datapb.ChannelWatchState_ToRelease)
	err = s.manager.Submit(info)
	s.NoError(err)

	// wait for result
	opState = <-s.manager.communicateCh
	s.NotNil(opState)
	s.Equal(datapb.ChannelWatchState_ReleaseSuccess, opState.state)
	s.manager.handleOpState(opState)

	resp = s.manager.GetProgress(info)
	s.Equal(info.GetOpID(), resp.GetOpID())
	s.Equal(datapb.ChannelWatchState_ReleaseSuccess, resp.GetState())

	s.Equal(0, s.manager.fgManager.GetFlowgraphCount())
	s.False(s.manager.opRunners.Contain(info.GetVchan().GetChannelName()))
	s.Equal(0, s.manager.opRunners.Len())

	err = s.manager.Submit(info)
	s.NoError(err)
	runner, ok := s.manager.opRunners.Get(channel)
	s.False(ok)
	s.Nil(runner)
}
