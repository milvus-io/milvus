// Copyright (C) 2019-2020 Zilliz. All rights reserved.//// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
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
	"sync"
	"sync/atomic"
	"time"

	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	masterclient "github.com/milvus-io/milvus/internal/distributed/masterservice/client"
	"github.com/milvus-io/milvus/internal/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const role = "dataservice"

const masterClientTimout = 20 * time.Second

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)
type Server struct {
	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel context.CancelFunc
	serverLoopWg     sync.WaitGroup
	isServing        int64

	kvClient          *etcdkv.EtcdKV
	meta              *meta
	segmentInfoStream msgstream.MsgStream
	segmentManager    Manager
	allocator         allocator
	cluster           *cluster
	masterClient      types.MasterService
	ddChannelName     string

	flushCh        chan UniqueID
	flushMsgStream msgstream.MsgStream
	msFactory      msgstream.Factory

	session  *sessionutil.Session
	activeCh <-chan bool
	eventCh  <-chan *sessionutil.SessionEvent

	dataClientCreator   func(addr string) (types.DataNode, error)
	masterClientCreator func(addr string) (types.MasterService, error)
}

func CreateServer(ctx context.Context, factory msgstream.Factory) (*Server, error) {
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		ctx:       ctx,
		msFactory: factory,
		flushCh:   make(chan UniqueID, 1024),
	}
	s.dataClientCreator = func(addr string) (types.DataNode, error) {
		return datanodeclient.NewClient(addr, 3*time.Second)
	}
	s.masterClientCreator = func(addr string) (types.MasterService, error) {
		return masterclient.NewClient(ctx, Params.MetaRootPath, Params.EtcdEndpoints, masterClientTimout)
	}

	return s, nil
}

// Register register data service at etcd
func (s *Server) Register() error {
	s.session = sessionutil.NewSession(s.ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	s.activeCh = s.session.Init(typeutil.DataServiceRole, Params.IP, true)
	Params.NodeID = s.session.ServerID
	return nil
}

func (s *Server) Init() error {
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) Start() error {
	var err error
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = s.msFactory.SetParams(m)
	if err != nil {
		return err
	}
	if err = s.initMasterClient(); err != nil {
		return err
	}

	if err = s.initMeta(); err != nil {
		return err
	}

	if err = s.initCluster(); err != nil {
		return err
	}

	if err = s.initSegmentInfoChannel(); err != nil {
		return err
	}

	s.allocator = newAllocator(s.masterClient)

	s.startSegmentManager()
	if err = s.initFlushMsgStream(); err != nil {
		return err
	}

	if err = s.initServiceDiscovery(); err != nil {
		return err
	}

	s.startServerLoop()

	atomic.StoreInt64(&s.isServing, 2)
	log.Debug("start success")
	return nil
}

func (s *Server) initCluster() error {
	dManager, err := newClusterNodeManager(s.kvClient)
	if err != nil {
		return err
	}
	sManager := newClusterSessionManager(s.dataClientCreator)
	s.cluster = newCluster(s.ctx, dManager, sManager, s)
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

	s.eventCh = s.session.WatchServices(typeutil.DataNodeRole, rev)
	return nil
}

func (s *Server) startSegmentManager() {
	helper := createNewSegmentHelper(s.segmentInfoStream)
	s.segmentManager = newSegmentManager(s.meta, s.allocator, withAllocHelper(helper))
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

func (s *Server) initMeta() error {
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
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

func (s *Server) startServerLoop() {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	s.serverLoopWg.Add(5)
	go s.startStatsChannel(s.serverLoopCtx)
	go s.startDataNodeTtLoop(s.serverLoopCtx)
	go s.startWatchService(s.serverLoopCtx)
	go s.startActiveCheck(s.serverLoopCtx)
	go s.startFlushLoop(s.serverLoopCtx)
}

func (s *Server) startStatsChannel(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	statsStream, _ := s.msFactory.NewMsgStream(ctx)
	statsStream.AsConsumer([]string{Params.StatisticsChannelName}, Params.DataServiceSubscriptionName)
	log.Debug("dataservce stats stream",
		zap.String("channelName", Params.StatisticsChannelName),
		zap.String("descriptionName", Params.DataServiceSubscriptionName))
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
				log.Warn("receive unknown msg from segment statistics channel",
					zap.Stringer("msgType", msg.Type()))
				continue
			}
			log.Debug("Receive DataNode segment statistics update")
			ssMsg := msg.(*msgstream.SegmentStatisticsMsg)
			for _, stat := range ssMsg.SegStats {
				s.segmentManager.UpdateSegmentStats(stat)
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
				log.Warn("Receive unexpected msg type from tt channel",
					zap.Stringer("msgType", msg.Type()))
				continue
			}
			ttMsg := msg.(*msgstream.DataNodeTtMsg)

			ch := ttMsg.ChannelName
			ts := ttMsg.Timestamp
			log.Debug("Receive datanode timetick msg", zap.String("channel", ch),
				zap.Any("ts", ts))
			segments, err := s.segmentManager.GetFlushableSegments(ctx, ch, ts)
			if err != nil {
				log.Warn("get flushable segments failed", zap.Error(err))
				continue
			}

			if len(segments) == 0 {
				continue
			}
			log.Debug("Flush segments", zap.Int64s("segmentIDs", segments))
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
			if len(segmentInfos) > 0 {
				s.cluster.flush(segmentInfos)
			}
		}
	}
}

func (s *Server) startWatchService(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Debug("watch service shutdown")
			return
		case event := <-s.eventCh:
			datanode := &datapb.DataNodeInfo{
				Address:  event.Session.Address,
				Version:  event.Session.ServerID,
				Channels: []*datapb.ChannelStatus{},
			}
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				log.Info("Received datanode register",
					zap.String("address", datanode.Address),
					zap.Int64("serverID", datanode.Version))
				s.cluster.register(datanode)
			case sessionutil.SessionDelEvent:
				log.Info("Received datanode unregister",
					zap.String("address", datanode.Address),
					zap.Int64("serverID", datanode.Version))
				s.cluster.unregister(datanode)
			default:
				log.Warn("receive unknown service event type",
					zap.Any("type", event.EventType))
			}
		}
	}
}

func (s *Server) startActiveCheck(ctx context.Context) {
	defer logutil.LogPanic()
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

func (s *Server) startFlushLoop(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	// send `Flushing` segments
	go s.handleFlushingSegments(ctx2)
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Debug("flush loop shutdown")
			return
		case segmentID := <-s.flushCh:
			// write flush msg into segmentInfo/flush stream
			msgPack := composeSegmentFlushMsgPack(segmentID)
			err = s.flushMsgStream.Produce(&msgPack)
			if err != nil {
				log.Error("produce flush msg failed",
					zap.Int64("segmentID", segmentID),
					zap.Error(err))
				continue
			}
			log.Debug("send segment flush msg", zap.Int64("id", segmentID))

			// set segment to SegmentState_Flushed
			if err = s.meta.FlushSegment(segmentID); err != nil {
				log.Error("flush segment complete failed", zap.Error(err))
				continue
			}
			log.Debug("flush segment complete", zap.Int64("id", segmentID))
		}
	}
}

func (s *Server) handleFlushingSegments(ctx context.Context) {
	segments := s.meta.GetFlushingSegments()
	for _, segment := range segments {
		select {
		case <-ctx.Done():
			return
		case s.flushCh <- segment.ID:
		}
	}
}

func (s *Server) initMasterClient() error {
	var err error
	s.masterClient, err = s.masterClientCreator("")
	if err != nil {
		return err
	}
	if err = s.masterClient.Init(); err != nil {
		return err
	}
	return s.masterClient.Start()
}

func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt64(&s.isServing, 2, 0) {
		return nil
	}
	log.Debug("dataservice server shutdown")
	atomic.StoreInt64(&s.isServing, 0)
	s.cluster.releaseSessions()
	s.segmentInfoStream.Close()
	s.flushMsgStream.Close()
	s.stopServerLoop()
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
		log.Error("show partitions error", zap.String("collectionName", resp.Schema.Name),
			zap.Int64("collectionID", resp.CollectionID), zap.Error(err))
		return err
	}
	collInfo := &datapb.CollectionInfo{
		ID:         resp.CollectionID,
		Schema:     resp.Schema,
		Partitions: presp.PartitionIDs,
	}
	return s.meta.AddCollection(collInfo)
}

func (s *Server) prepareBinlog(req *datapb.SaveBinlogPathsRequest) (map[string]string, error) {
	meta := make(map[string]string)

	for _, fieldBlp := range req.Field2BinlogPaths {
		fieldMeta, err := s.prepareField2PathMeta(req.SegmentID, fieldBlp)
		if err != nil {
			return nil, err
		}
		for k, v := range fieldMeta {
			meta[k] = v
		}
	}

	return meta, nil
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
