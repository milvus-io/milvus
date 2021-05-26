// Copyright (C) 2019-2020 Zilliz. All rights reserved.//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package dataservice

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/logutil"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const role = "dataservice"

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)
type Server struct {
	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel context.CancelFunc
	serverLoopWg     sync.WaitGroup
	state            atomic.Value
	initOnce         sync.Once
	startOnce        sync.Once
	stopOnce         sync.Once

	kvClient          *etcdkv.EtcdKV
	meta              *meta
	segmentInfoStream msgstream.MsgStream
	segAllocator      segmentAllocatorInterface
	statsHandler      *statsHandler
	allocator         allocatorInterface
	cluster           *cluster
	masterClient      types.MasterService
	ddChannelMu       struct {
		sync.Mutex
		name string
	}

	flushMsgStream msgstream.MsgStream
	msFactory      msgstream.Factory

	session  *sessionutil.Session
	activeCh <-chan bool
	watchCh  <-chan *sessionutil.SessionEvent

	dataClientCreator func(addr string) (types.DataNode, error)
}

func CreateServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		ctx:       ctx,
		msFactory: factory,
	}
	s.dataClientCreator = func(addr string) (types.DataNode, error) {
		return grpcdatanodeclient.NewClient(addr)
	}

	s.UpdateStateCode(internalpb.StateCode_Abnormal)
	log.Debug("DataService", zap.Any("State", s.state.Load()))
	return s, nil
}

func (s *Server) getInsertChannels() []string {
	channels := make([]string, 0, Params.InsertChannelNum)
	var i int64 = 0
	for ; i < Params.InsertChannelNum; i++ {
		channels = append(channels, Params.InsertChannelPrefixName+strconv.FormatInt(i, 10))
	}
	return channels
}

func (s *Server) SetMasterClient(masterClient types.MasterService) {
	s.masterClient = masterClient
}

// Register register data service at etcd
func (s *Server) Register() error {
	s.activeCh = s.session.Init(typeutil.DataServiceRole, Params.IP, true)
	Params.NodeID = s.session.ServerID
	return nil
}

func (s *Server) Init() error {
	s.initOnce.Do(func() {
		s.session = sessionutil.NewSession(s.ctx, []string{Params.EtcdAddress})
	})
	return nil
}

var startOnce sync.Once

func (s *Server) Start() error {
	var err error
	s.startOnce.Do(func() {
		m := map[string]interface{}{
			"PulsarAddress":  Params.PulsarAddress,
			"ReceiveBufSize": 1024,
			"PulsarBufSize":  1024}
		err = s.msFactory.SetParams(m)
		if err != nil {
			return
		}

		if err = s.initMeta(); err != nil {
			return
		}

		if err = s.initCluster(); err != nil {
			return
		}

		if err = s.initSegmentInfoChannel(); err != nil {
			return
		}

		s.allocator = newAllocator(s.masterClient)

		s.startSegmentAllocator()
		s.statsHandler = newStatsHandler(s.meta)
		if err = s.initFlushMsgStream(); err != nil {
			return
		}

		if err = s.initServiceDiscovery(); err != nil {
			return
		}

		s.startServerLoop()

		s.UpdateStateCode(internalpb.StateCode_Healthy)
		log.Debug("start success")
	})
	return err
}

func (s *Server) initCluster() error {
	dManager, err := newClusterNodeManager(s.kvClient)
	if err != nil {
		return err
	}
	sManager := newClusterSessionManager(s.dataClientCreator)
	s.cluster = newCluster(s.ctx, dManager, sManager)
	return nil
}

func (s *Server) initServiceDiscovery() error {
	sessions, rev, err := s.session.GetSessions(typeutil.DataNodeRole)
	if err != nil {
		log.Debug("DataService initMeta failed", zap.Error(err))
		return err
	}
	log.Debug("registered sessions", zap.Any("sessions", sessions))

	datanodes := make([]*datapb.DataNodeInfo, 0, len(sessions))
	for _, session := range sessions {
		datanodes = append(datanodes, &datapb.DataNodeInfo{
			Address:  session.Address,
			Version:  session.ServerID,
			Channels: []*datapb.ChannelStatus{},
		})
	}

	if err := s.cluster.startup(datanodes); err != nil {
		log.Debug("DataService loadMetaFromMaster failed", zap.Error(err))
		return err
	}

	s.watchCh = s.session.WatchServices(typeutil.DataNodeRole, rev)

	return nil
}

func (s *Server) startSegmentAllocator() {
	helper := createNewSegmentHelper(s.segmentInfoStream)
	s.segAllocator = newSegmentAllocator(s.meta, s.allocator, withAllocHelper(helper))
}

func (s *Server) initSegmentInfoChannel() error {
	var err error
	s.segmentInfoStream, err = s.msFactory.NewMsgStream(s.ctx)
	if err != nil {
		return err
	}
	s.segmentInfoStream.AsProducer([]string{Params.SegmentInfoChannelName})
	log.Debug("DataService AsProducer: " + Params.SegmentInfoChannelName)
	s.segmentInfoStream.Start()
	return nil
}

func (s *Server) UpdateStateCode(code internalpb.StateCode) {
	s.state.Store(code)
}

func (s *Server) checkStateIsHealthy() bool {
	return s.state.Load().(internalpb.StateCode) == internalpb.StateCode_Healthy
}

func (s *Server) initMeta() error {
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints: []string{Params.EtcdAddress},
		})
		if err != nil {
			return err
		}
		s.kvClient = etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		s.meta, err = newMeta(s.kvClient)
		if err != nil {
			return err
		}
		return nil
	}
	return retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
}

func (s *Server) initFlushMsgStream() error {
	var err error
	// segment flush stream
	s.flushMsgStream, err = s.msFactory.NewMsgStream(s.ctx)
	if err != nil {
		return err
	}
	s.flushMsgStream.AsProducer([]string{Params.SegmentInfoChannelName})
	log.Debug("dataservice AsProducer:" + Params.SegmentInfoChannelName)
	s.flushMsgStream.Start()

	return nil
}

func (s *Server) getDDChannel() error {
	s.ddChannelMu.Lock()
	defer s.ddChannelMu.Unlock()
	if len(s.ddChannelMu.name) == 0 {
		resp, err := s.masterClient.GetDdChannel(s.ctx)
		if err = VerifyResponse(resp, err); err != nil {
			return err
		}
		s.ddChannelMu.name = resp.Value
	}
	return nil
}

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(4)
	go s.startStatsChannel(s.serverLoopCtx)
	go s.startDataNodeTtLoop(s.serverLoopCtx)
	go s.startWatchService(s.serverLoopCtx)
	go s.startActiveCheck(s.serverLoopCtx)
}

func (s *Server) startStatsChannel(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	statsStream, _ := s.msFactory.NewMsgStream(ctx)
	statsStream.AsConsumer([]string{Params.StatisticsChannelName}, Params.DataServiceSubscriptionName)
	log.Debug("DataService AsConsumer: " + Params.StatisticsChannelName + " : " + Params.DataServiceSubscriptionName)
	// try to restore last processed pos
	pos, err := s.loadStreamLastPos(streamTypeStats)
	log.Debug("load last pos of stats channel", zap.Any("pos", pos))
	if err == nil {
		err = statsStream.Seek([]*internalpb.MsgPosition{pos})
		if err != nil {
			log.Error("Failed to seek to last pos for statsStream",
				zap.String("StatisticsChanName", Params.StatisticsChannelName),
				zap.String("DataServiceSubscriptionName", Params.DataServiceSubscriptionName),
				zap.Error(err))
		}
	}
	statsStream.Start()
	defer statsStream.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgPack := statsStream.Consume()
		if msgPack == nil {
			return
		}
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_SegmentStatistics {
				log.Warn("receive unknown msg from segment statistics channel", zap.Stringer("msgType", msg.Type()))
				continue
			}
			ssMsg := msg.(*msgstream.SegmentStatisticsMsg)
			for _, stat := range ssMsg.SegStats {
				if err := s.statsHandler.HandleSegmentStat(stat); err != nil {
					log.Error("handle segment stat error", zap.Int64("segmentID", stat.SegmentID), zap.Error(err))
					continue
				}
			}
			if ssMsg.MsgPosition != nil {
				err := s.storeStreamPos(streamTypeStats, ssMsg.MsgPosition)
				if err != nil {
					log.Error("Fail to store current success pos for Stats stream",
						zap.Stringer("pos", ssMsg.MsgPosition),
						zap.Error(err))
				}
			} else {
				log.Warn("Empty Msg Pos found ", zap.Int64("msgid", msg.ID()))
			}
		}
	}
}

func (s *Server) startDataNodeTtLoop(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	ttMsgStream, err := s.msFactory.NewMsgStream(ctx)
	if err != nil {
		log.Error("new msg stream failed", zap.Error(err))
		return
	}
	ttMsgStream.AsConsumer([]string{Params.TimeTickChannelName},
		Params.DataServiceSubscriptionName)
	log.Debug(fmt.Sprintf("dataservice AsConsumer:%s:%s",
		Params.TimeTickChannelName, Params.DataServiceSubscriptionName))
	ttMsgStream.Start()
	defer ttMsgStream.Close()
	for {
		select {
		case <-ctx.Done():
			log.Debug("data node tt loop done")
			return
		default:
		}
		msgPack := ttMsgStream.Consume()
		if msgPack == nil {
			return
		}
		for _, msg := range msgPack.Msgs {
			if msg.Type() != commonpb.MsgType_DataNodeTt {
				log.Warn("receive unexpected msg type from tt channel",
					zap.Stringer("msgType", msg.Type()))
				continue
			}
			ttMsg := msg.(*msgstream.DataNodeTtMsg)

			ch := ttMsg.ChannelName
			ts := ttMsg.Timestamp
			segments, err := s.segAllocator.GetFlushableSegments(ctx, ch, ts)
			if err != nil {
				log.Warn("get flushable segments failed", zap.Error(err))
				continue
			}

			log.Debug("flushable segments", zap.Any("segments", segments))
			segmentInfos := make([]*datapb.SegmentInfo, 0, len(segments))
			for _, id := range segments {
				sInfo, err := s.meta.GetSegment(id)
				if err != nil {
					log.Error("get segment from meta error", zap.Int64("id", id),
						zap.Error(err))
					continue
				}
				segmentInfos = append(segmentInfos, sInfo)
			}

			s.cluster.flush(segmentInfos)
		}
	}
}

func (s *Server) startWatchService(ctx context.Context) {
	defer s.serverLoopWg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Debug("watch service shutdown")
			return
		case event := <-s.watchCh:
			datanode := &datapb.DataNodeInfo{
				Address:  event.Session.Address,
				Version:  event.Session.ServerID,
				Channels: []*datapb.ChannelStatus{},
			}
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				s.cluster.register(datanode)
			case sessionutil.SessionDelEvent:
				s.cluster.unregister(datanode)
			default:
				log.Warn("receive unknown service event type",
					zap.Any("type", event.EventType))
			}
		}
	}
}

func (s *Server) startActiveCheck(ctx context.Context) {
	defer s.serverLoopWg.Done()

	for {
		select {
		case _, ok := <-s.activeCh:
			if ok {
				continue
			}
			s.Stop()
			log.Debug("disconnect with etcd")
			return
		case <-ctx.Done():
			log.Debug("connection check shutdown")
			return
		}
	}
}

var stopOnce sync.Once

func (s *Server) Stop() error {
	s.stopOnce.Do(func() {
		s.cluster.releaseSessions()
		s.segmentInfoStream.Close()
		s.flushMsgStream.Close()
		s.stopServerLoop()
	})
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	log.Debug("clean meta", zap.Any("kv", s.kvClient))
	return s.kvClient.RemoveWithPrefix("")
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

//func (s *Server) validateAllocRequest(collID UniqueID, partID UniqueID, channelName string) error {
//	if !s.meta.HasCollection(collID) {
//		return fmt.Errorf("can not find collection %d", collID)
//	}
//	if !s.meta.HasPartition(collID, partID) {
//		return fmt.Errorf("can not find partition %d", partID)
//	}
//	for _, name := range s.insertChannels {
//		if name == channelName {
//			return nil
//		}
//	}
//	return fmt.Errorf("can not find channel %s", channelName)
//}

func (s *Server) loadCollectionFromMaster(ctx context.Context, collectionID int64) error {
	resp, err := s.masterClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_DescribeCollection,
			SourceID: Params.NodeID,
		},
		DbName:       "",
		CollectionID: collectionID,
	})
	if err = VerifyResponse(resp, err); err != nil {
		return err
	}
	presp, err := s.masterClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     -1, // todo
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		DbName:         "",
		CollectionName: resp.Schema.Name,
		CollectionID:   resp.CollectionID,
	})
	if err = VerifyResponse(presp, err); err != nil {
		log.Error("show partitions error", zap.String("collectionName", resp.Schema.Name), zap.Int64("collectionID", resp.CollectionID), zap.Error(err))
		return err
	}
	collInfo := &datapb.CollectionInfo{
		ID:         resp.CollectionID,
		Schema:     resp.Schema,
		Partitions: presp.PartitionIDs,
	}
	return s.meta.AddCollection(collInfo)
}

func (s *Server) prepareBinlogAndPos(req *datapb.SaveBinlogPathsRequest) (map[string]string, error) {
	meta := make(map[string]string)
	segInfo, err := s.meta.GetSegment(req.GetSegmentID())
	if err != nil {
		log.Error("Failed to get segment info", zap.Int64("segmentID", req.GetSegmentID()), zap.Error(err))
		return nil, err
	}
	log.Debug("segment", zap.Int64("segment", segInfo.CollectionID))

	for _, fieldBlp := range req.Field2BinlogPaths {
		fieldMeta, err := s.prepareField2PathMeta(req.SegmentID, fieldBlp)
		if err != nil {
			return nil, err
		}
		for k, v := range fieldMeta {
			meta[k] = v
		}
	}

	ddlMeta, err := s.prepareDDLBinlogMeta(req.CollectionID, req.GetDdlBinlogPaths())
	if err != nil {
		return nil, err
	}
	for k, v := range ddlMeta {
		meta[k] = v
	}
	segmentPos, err := s.prepareSegmentPos(segInfo, req.GetDmlPosition(), req.GetDdlPosition())
	if err != nil {
		return nil, err
	}
	for k, v := range segmentPos {
		meta[k] = v
	}

	return meta, nil
}

func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

func composeSegmentFlushMsgPack(segmentID UniqueID) msgstream.MsgPack {
	msgPack := msgstream.MsgPack{
		Msgs: make([]msgstream.TsMsg, 0, 1),
	}
	completeFlushMsg := internalpb.SegmentFlushCompletedMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentFlushDone,
			MsgID:     0, // TODO
			Timestamp: 0, // TODO
			SourceID:  Params.NodeID,
		},
		SegmentID: segmentID,
	}
	var msg msgstream.TsMsg = &msgstream.FlushCompletedMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SegmentFlushCompletedMsg: completeFlushMsg,
	}

	msgPack.Msgs = append(msgPack.Msgs, msg)
	return msgPack
}
