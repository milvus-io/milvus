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

package channel

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	util2 "github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(t *testing.M) {
	paramtable.Init()
	code := t.Run()
	os.Exit(code)
}

func TestChannelManagerSuite(t *testing.T) {
	suite.Run(t, new(ChannelManagerSuite))
}

func TestOpRunnerSuite(t *testing.T) {
	suite.Run(t, new(OpRunnerSuite))
}

func (s *OpRunnerSuite) SetupTest() {
	mockedBroker := broker.NewMockBroker(s.T())
	mockedBroker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
		Return([]*datapb.SegmentInfo{}, nil).Maybe()

	wbManager := writebuffer.NewMockBufferManager(s.T())
	wbManager.EXPECT().
		Register(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	dispClient := msgdispatcher.NewMockClient(s.T())
	dispClient.EXPECT().Register(mock.Anything, mock.Anything).
		Return(make(chan *msgstream.MsgPack), nil).Maybe()
	dispClient.EXPECT().Deregister(mock.Anything).Maybe()

	s.pipelineParams = &util2.PipelineParams{
		Ctx:                context.TODO(),
		Session:            &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 0}},
		CheckpointUpdater:  util2.NewChannelCheckpointUpdater(mockedBroker),
		WriteBufferManager: wbManager,
		Broker:             mockedBroker,
		DispClient:         dispClient,
		SyncMgr:            syncmgr.NewMockSyncManager(s.T()),
		Allocator:          allocator.NewMockAllocator(s.T()),
	}
}

func (s *OpRunnerSuite) TestWatchWithTimer() {
	var (
		channel string = "ch-1"
		commuCh        = make(chan *opState)
	)
	info := GetWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	mockReleaseFunc := func(channel string) {
		log.Info("mock release func")
	}

	runner := NewOpRunner(context.TODO(), channel, s.pipelineParams, mockReleaseFunc, executeWatch, commuCh)
	err := runner.Enqueue(info)
	s.Require().NoError(err)

	opState := runner.watchWithTimer(info)
	s.NotNil(opState.fg)
	s.Equal(channel, opState.channel)

	runner.FinishOp(100)
}

func (s *OpRunnerSuite) TestWatchTimeout() {
	channel := "by-dev-rootcoord-dml-1000"
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.WatchTimeoutInterval.Key, "0.000001")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.WatchTimeoutInterval.Key)
	info := GetWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)

	sig := make(chan struct{})
	commuCh := make(chan *opState)

	mockReleaseFunc := func(channel string) { log.Info("mock release func") }
	mockWatchFunc := func(ctx context.Context, param *util2.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util2.Tickler) (*pipeline.DataSyncService, error) {
		<-ctx.Done()
		sig <- struct{}{}
		return nil, errors.New("timeout")
	}

	runner := NewOpRunner(context.TODO(), channel, s.pipelineParams, mockReleaseFunc, mockWatchFunc, commuCh)
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
	pipelineParams *util2.PipelineParams
}

type ChannelManagerSuite struct {
	suite.Suite

	pipelineParams *util2.PipelineParams
	manager        *ChannelManagerImpl
}

func (s *ChannelManagerSuite) SetupTest() {
	factory := dependency.NewDefaultFactory(true)

	wbManager := writebuffer.NewMockBufferManager(s.T())
	wbManager.EXPECT().
		Register(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	wbManager.EXPECT().RemoveChannel(mock.Anything).Maybe()

	mockedBroker := &broker.MockBroker{}
	mockedBroker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()

	s.pipelineParams = &util2.PipelineParams{
		Ctx:                context.TODO(),
		Session:            &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 0}},
		WriteBufferManager: wbManager,
		Broker:             mockedBroker,
		MsgStreamFactory:   factory,
		DispClient:         msgdispatcher.NewClient(factory, typeutil.DataNodeRole, paramtable.GetNodeID()),
		SyncMgr:            syncmgr.NewMockSyncManager(s.T()),
		Allocator:          allocator.NewMockAllocator(s.T()),
	}

	s.manager = NewChannelManager(s.pipelineParams, pipeline.NewFlowgraphManager())
}

func (s *ChannelManagerSuite) TearDownTest() {
	if s.manager != nil {
		s.manager.Close()
	}
}

func (s *ChannelManagerSuite) TestReleaseStuck() {
	channel := "by-dev-rootcoord-dml-2"
	s.manager.releaseFunc = func(channel string) {
		time.Sleep(1 * time.Second)
	}

	info := GetWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	s.Require().Equal(0, s.manager.opRunners.Len())
	err := s.manager.Submit(context.TODO(), info)
	s.Require().NoError(err)

	opState := <-s.manager.communicateCh
	s.Require().NotNil(opState)

	s.manager.handleOpState(opState)

	releaseInfo := GetWatchInfoByOpID(101, channel, datapb.ChannelWatchState_ToRelease)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.WatchTimeoutInterval.Key, "0.1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.WatchTimeoutInterval.Key)

	err = s.manager.Submit(context.TODO(), releaseInfo)
	s.NoError(err)

	opState = <-s.manager.communicateCh
	s.Require().NotNil(opState)
	s.Equal(datapb.ChannelWatchState_ReleaseFailure, opState.state)
	s.manager.handleOpState(opState)

	s.Equal(1, s.manager.abnormals.Len())
	abchannel, ok := s.manager.abnormals.Get(releaseInfo.GetOpID())
	s.True(ok)
	s.Equal(channel, abchannel)

	resp := s.manager.GetProgress(releaseInfo)
	s.Equal(datapb.ChannelWatchState_ReleaseFailure, resp.GetState())
}

func (s *ChannelManagerSuite) TestSubmitIdempotent() {
	channel := "by-dev-rootcoord-dml-1"

	info := GetWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	s.Require().Equal(0, s.manager.opRunners.Len())

	for i := 0; i < 10; i++ {
		err := s.manager.Submit(context.TODO(), info)
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

	info := GetWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	s.Require().Equal(0, s.manager.opRunners.Len())

	err := s.manager.Submit(context.TODO(), info)
	s.NoError(err)

	s.Equal(1, s.manager.opRunners.Len())
	s.True(s.manager.opRunners.Contain(channel))
	opState := <-s.manager.communicateCh
	s.NotNil(opState)
	s.Equal(datapb.ChannelWatchState_WatchSuccess, opState.state)
	s.NotNil(opState.fg)
	s.Equal(info.GetOpID(), opState.fg.GetOpID())
	s.manager.handleOpState(opState)

	err = s.manager.Submit(context.TODO(), info)
	s.NoError(err)

	runner, ok := s.manager.opRunners.Get(channel)
	s.False(ok)
	s.Nil(runner)
}

func (s *ChannelManagerSuite) TestSubmitWatchAndRelease() {
	channel := "by-dev-rootcoord-dml-0"

	stream, err := s.pipelineParams.MsgStreamFactory.NewTtMsgStream(context.Background())
	s.NoError(err)
	s.NotNil(stream)
	stream.AsProducer(context.Background(), []string{channel})

	// watch
	info := GetWatchInfoByOpID(100, channel, datapb.ChannelWatchState_ToWatch)
	err = s.manager.Submit(context.TODO(), info)
	s.NoError(err)

	// wait for result
	opState := <-s.manager.communicateCh
	s.NotNil(opState)
	s.Equal(datapb.ChannelWatchState_WatchSuccess, opState.state)
	s.NotNil(opState.fg)
	s.Equal(info.GetOpID(), opState.fg.GetOpID())

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
	info = GetWatchInfoByOpID(101, channel, datapb.ChannelWatchState_ToRelease)
	err = s.manager.Submit(context.TODO(), info)
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

	err = s.manager.Submit(context.TODO(), info)
	s.NoError(err)
	runner, ok := s.manager.opRunners.Get(channel)
	s.False(ok)
	s.Nil(runner)
}

func GetWatchInfoByOpID(opID typeutil.UniqueID, channel string, state datapb.ChannelWatchState) *datapb.ChannelWatchInfo {
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
